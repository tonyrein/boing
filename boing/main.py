"""
    Process HonSSH data -- get it from the files generated by HonSSH
    into the ElasticSearch database. There are three steps:
    
    1. "Scraping." In this step, the files are read and their contents
    are stored in a database that's local to the HonSSH process. Currently,
    sqlite is used; other database systems would be feasible if needed. After
    a file's content is scraped, the file is marked as done.
    
    2. Inserting into ElasticSearch. In this step, the database records are
    read and used as the input to ElasticSearch insertion methods. Records that are
    successfully inserted into ElasticSearch are marked as such by having their
    ElasticSearch-assigned id strings stored in the local database.
    
    3. Cleanup. In this step, files and database records marked as done in steps
    1 and 2 are deleted.
"""
import sqlite3
import logging
import sys
import os

from dao.record_dao_es import AttemptRecordDaoES, LogRecordDaoES
from dao.record_dao_es import SessionLogDaoES, SessionRecordingDaoES, SessionDownloadDaoES
from dao.record_dao_local import AttemptRecordDaoLocal, LogRecordDaoLocal
from dao.record_dao_local import SessionLogDaoLocal, SessionRecordingDaoLocal, SessionDownloadDaoLocal
from dao.local_db_access import LocalDBAccessor
from dto.record import AttemptRecord, LogRecord
from dto.record import SessionLogRecord, SessionRecordingRecord, SessionDownloadFileRecord
from file.file_lister import AttemptFileLister, LogFileLister
from file.file_lister import SessionLogFileLister, SessionRecordingFileLister, SessionDownloadFileLister
from service.service_local import ServiceLocal
from util.config import StretchConfig
from util.util import logging_level_from_string, configure_logging
from util.util import generate_archive_name, archive_file_list
import os.path


class Boing(object ):
    def __init__(self):
        self._cfg = StretchConfig()
        self._logger = configure_logging(self._cfg.get_logging_info)
        self._dba = LocalDBAccessor(self._cfg.get_db_info())
        self._dba.initialize_database()
        # Create directory to store archived data files, if it doesn't already exist:
        self._arc_dir = self._cfg.get_locations()['arc_dir']
        if not os.path.isdir(self._arc_dir):
            os.makedirs(self._arc_dir)
        
    def configure_logging(self):
        fn=self._cfg.get_logging_info()['filename']
        levelstr=self._cfg.get_logging_info()['level']
        if levelstr == '': levelstr = 'NOTSET'
        level = logging_level_from_string(levelstr)
        if fn != 'CONSOLE':
            logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', filename=fn, level=level)
        else:
            logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=level)
        self._logger = logging.getLogger()
        
    def scrape_honssh_files(self, loc_type, lister_class, dao_local_class):
        source_dir = self._cfg.get_locations()[loc_type]
        lister = lister_class(source_dir)
        lister.load_file_name_lists()
        lister.load_pending_file_objects()
        self._logger.info("File lister loaded with %s files", len(lister))
        print "File lister loaded with " + str(len(lister)) + " files"
        dao_obj = dao_local_class(self._dba)
        aservice = ServiceLocal(dao_obj)
        total_num_saved = 0
        done_files = []
        for f in lister:
            if f.load():
                self._logger.info("loaded %s, containing %s records", f.name(), len(f) )
                print "loaded " + f.name() + " containing " + str(len(f)) + " records"
                if len(f) > 0:
                    num_saved = aservice.write_new_records(f._entry_list)
                    self._logger.info("Saved %s records", num_saved)
                    print "Number saved: " + str(num_saved)
                    total_num_saved += num_saved
                    if num_saved is None or num_saved != len(f):
                        raise Exception("Only " + num_saved + "records written from file " + f._name + ". File contains " + len(f) + " + records.")
                    else:
                        lister.mark_as_done(f)
                        done_files.append(f._name)
        return done_files
    
    
    def scrape_session_log_records(self):
        return self.scrape_honssh_files('session_dir', SessionLogFileLister, SessionLogDaoLocal)
    
    def scrape_session_download_files(self,):
        return self.scrape_honssh_files('session_dir', SessionDownloadFileLister, SessionDownloadDaoLocal)
    
    def scrape_session_recordings(self,):
        return self.scrape_honssh_files('session_dir', SessionRecordingFileLister, SessionRecordingDaoLocal)
    
    def scrape_attempt_records(self,):
        return self.scrape_honssh_files('attempt_dir', AttemptFileLister, AttemptRecordDaoLocal)
    
    def scrape_log_records(self,):
        return self.scrape_honssh_files('log_dir', LogFileLister, LogRecordDaoLocal)
    
    def put_records_into_es(self, localdaoclass, esclass, recordclass):
            db_local = localdaoclass(self._dba)
            aservice = ServiceLocal(db_local)
            es_link = esclass(self._cfg.get_es_info())
            rows = aservice.get_non_processed()
            total_to_add = len(rows)
            self._logger.info("Found %s records not yet put into ES", total_to_add)
            print "Found " + str(total_to_add) + " records not yet put into ES"
            num_into_es = 0
            for row in rows:
                r = recordclass()
                i = 0
                for fld in db_local.get_insert_fields():
                    setattr(r, fld, row[i+2])
                    i += 1
                es_id = es_link.insert_single(r)
                if es_id:
                    try:
                        db_id = row[0]
                        aservice.update_with_es_id(db_id, es_id)
                    except sqlite3.Error as e:
                        self._logger.error("Could not update record in local db", exc_info = True)
                        print "Could not update record with database id " + str(row[0]) + " in local database!"
                        raise e
                    num_into_es += 1
                    # emit progress indication...
                    if (self._logger.isEnabledFor(logging.INFO)):
                        if num_into_es % 50 == 0:
                            self._logger.info("Added to ES: %s of %s...", num_into_es, total_to_add)
                else:
                    raise Exception("Could not add record with database id " + str(row[0]) + " to ElasticSearch!")
            return num_into_es
        
    def put_session_log_records_into_es(self):
        return self.put_records_into_es(SessionLogDaoLocal, SessionLogDaoES, SessionLogRecord)
        
    def put_session_download_records_into_es(self):
        return self.put_records_into_es(SessionDownloadDaoLocal, SessionDownloadDaoES, SessionDownloadFileRecord)
        
    def put_session_recordings_into_es(self):
        return self.put_records_into_es(SessionRecordingDaoLocal, SessionRecordingDaoES, SessionRecordingRecord)
    
    def put_attempt_records_into_es(self):
        return self.put_records_into_es(AttemptRecordDaoLocal, AttemptRecordDaoES, AttemptRecord)
    
    def put_log_records_into_es(self):
        return self.put_records_into_es(LogRecordDaoLocal, LogRecordDaoES, LogRecord)
    
    
    def prune_honssh_records(self, loc_type, lister_class, dao_local_class):
        source_dir = self._cfg.get_locations()[loc_type]
        lister = lister_class(source_dir)
        lister.load_file_name_lists()
        self._logger.info("Found %s files to prune", len(lister._done_file_names) )
        print "Found {0} files to prune".format(len(lister._done_file_names))
        count_files_removed = lister.delete_done_files()
        self._logger.info("Removed %s files", count_files_removed)
        print "Removed {0} files.".format(count_files_removed)
        self._logger.info('Will now attempt to remove processed records from local database...')
        print "Will now attempt to remove processed records from local database..."
        db_local = dao_local_class(self._dba)
        aservice = ServiceLocal(db_local)
        count_db_rows_deleted = aservice.delete_finished_records()
        self._logger.info("Removed %s records from local database", count_db_rows_deleted)
        return (count_files_removed, count_db_rows_deleted)
    
    
        
    def prune_session_log_records(self):
        return self.prune_honssh_records('session_dir', SessionLogFileLister, SessionLogDaoLocal)
    
    def prune_session_download_records(self):
        return self.prune_honssh_records('session_dir', SessionDownloadFileLister, SessionDownloadDaoLocal)
    
    def prune_session_recordings(self):
        return self.prune_honssh_records('session_dir', SessionRecordingFileLister, SessionRecordingDaoLocal)
    
    def prune_attempt_records(self):
        return self.prune_honssh_records('attempt_dir', AttemptFileLister, AttemptRecordDaoLocal)
    
    def prune_log_records(self):
        return self.prune_honssh_records('log_dir', LogFileLister, LogRecordDaoLocal)
    
    def main(self):
        td = self._arc_dir
        files_scraped = self.scrape_attempt_records()
        if len(files_scraped) > 0:
            arc_name = generate_archive_name(td + os.sep + 'HonSSH_Attempts-')
            archive_file_list(arc_name, files_scraped)
#         self.scrape_log_records()
        files_scraped = self.scrape_session_download_files()
        if len(files_scraped) > 0:
            arc_name = generate_archive_name(td + os.sep + 'HonSSH_Session_Downloads-')
            archive_file_list(arc_name, files_scraped)

        files_scraped = self.scrape_session_log_records()
        if len(files_scraped) > 0:
            arc_name = generate_archive_name(td + os.sep + 'HonSSH_Session_Logs-')
            archive_file_list(arc_name, files_scraped)

        files_scraped = self.scrape_session_recordings()
        if len(files_scraped) > 0:
            arc_name = generate_archive_name(td + os.sep + 'HonSSH_Session_Recordings-')
            archive_file_list(arc_name, files_scraped)
        
        self.put_attempt_records_into_es()
        self.put_log_records_into_es()
        self.put_session_download_records_into_es()
        self.put_session_log_records_into_es()
        self.put_session_recordings_into_es()
        
        self.prune_attempt_records()
        self.prune_log_records()
        self.prune_session_download_records()
        self.prune_session_log_records()
        self.prune_session_recordings()
        
if __name__ == '__main__':
    b = Boing()
    b.main()