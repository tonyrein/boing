import abc
import sqlite3

import os
import os.path
import sys
from pogo.dao.local_db_access import LocalDBAccessor
from pogo.util.config import StretchConfig

class RecordDaoLocal(object):
    __metaclass__ = abc.ABCMeta
    def __init__(self, localdbaccessor):
        if not localdbaccessor:
            raise ValueError("RecordDaoLocal object needs a LocalDBAccessor.")
        else:
            self._dba = localdbaccessor
            
        """
        assemble a SQL insert string appropriate for
        this object's class.
        The result will be something like
        "INSERT INTO attempts ( timestamp, bifrozt_host, source_ip ) VALUES ( ?,?,? )"
    """    
    def build_insert_query(self):
        table_name = self.get_table_name()
        insert_fields = self.get_insert_fields()
        sql = "INSERT INTO " + table_name + " ( "
        sql +=  ','.join(insert_fields) + " ) VALUES ( "
        sql += len(insert_fields) * '?,'
        if sql.endswith(','):
            sql = sql.rstrip(',')
        sql += " )"
        return sql
    
    """
        Make a list of values for a record, with the fields
        in the same order as in this class's INSERT_FIELDS tuple.
    """
    def build_values_list(self, record):
        d = record.as_dict()
        ret_list = []
        for f in self.get_insert_fields():
            ret_list.append(d[f])
        return ret_list

             
    """
        Insert a single record.
    """
    def insert_single(self, record ):
        sql = self.build_insert_query()
        values_list = self.build_values_list(record)
        try:
#             self.db_open()
            cursor = self._dba.db.cursor()
            cursor.execute('BEGIN TRANSACTION')
            cursor.execute(sql, values_list )
            cursor.execute('COMMIT')
        except sqlite3.Error as e:  # @UndefinedVariable
            cursor.execute('ROLLBACK')
            raise e
        
    """
        records should be a tuple of dto records, all of
        the type appropriate for this record_dao_local object.
        That is, if this is an AttemptRecordDaoLocal, all
        the records should be instances of AttemptRecord
    """
    def insert_bulk(self, records):
        sql = self.build_insert_query()
        count_of_written = 0
        try:
            cursor = self._dba.db.cursor()
            cursor.execute('BEGIN TRANSACTION')
            for r in records:
                values_list = self.build_values_list(r)
                cursor.execute(sql, values_list)
                count_of_written+=1
            cursor.execute('COMMIT')
            return count_of_written
        except sqlite3.Error as e:  # @UndefinedVariable
            cursor.execute('ROLLBACK')
            raise e

    
    def list_all(self):
        return self.list_where(None)
        
    def list_where(self, where_clause=None):
        sql = "SELECT " + self.get_all_fields() + " FROM " + self.get_table_name()
        if where_clause:
            sql += " WHERE " + where_clause
        cursor = self._dba.db.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    
    """
        Meant to be used as follows:
        new_values = ('a1', 'a2')
        where_clause = "db_id = 2444"
        fields = ('columnA', 'columnII')
        dao_object.update_where(fields, new_values, where_clause)
        Of course, fields and corresponding values must be in
        the same order.
    """
    def update_where(self, fields, new_values, where_clause=None):
        table_name = self.get_table_name()
        sql = "UPDATE " + table_name + " SET "
        if fields and len(fields) > 0:
            for f in fields:
                sql += "'" + f + "' =  ?,"
            # remove last comma, if present
            if sql.endswith(','):
                sql = sql.rstrip(',')
        if where_clause is not None:
            sql += " WHERE " + where_clause
        try:
            cursor = self._dba.db.cursor()
            cursor.execute('BEGIN TRANSACTION')
            cursor.execute(sql, new_values)
            cursor.execute('COMMIT')
        except sqlite3.Error as e:  # @UndefinedVariable
            cursor.execute('ROLLBACK')
            raise e
        
    def delete_where(self, where_clause):
        if not where_clause:
            raise ValueError("RecordDaoLocal.delete_where() called without where clause.")
        table_name = self.get_table_name()
        sql = "DELETE FROM " + table_name + " WHERE " + where_clause
        count_deleted = 0
        try:
            cursor = self._dba.db.cursor()
            cursor.execute('BEGIN TRANSACTION')
            cursor.execute(sql)
            count_deleted = cursor.rowcount
            cursor.execute('COMMIT')
            return count_deleted # should be the number of rows changed
        except sqlite3.Error as e:  # @UndefinedVariable
            cursor.execute('ROLLBACK')
            raise e


             
    @abc.abstractmethod
    def get_table_name(self):
        return ''
    
    @abc.abstractmethod
    def get_all_fields(self):
        return ''
    
    @abc.abstractmethod
    def get_insert_fields(self):
        return ''

class AttemptRecordDaoLocal(RecordDaoLocal):
    TABLE_NAME = 'attempts'
    ALL_FIELDS = "db_id, es_id, timestamp, bifrozt_host, source_ip, user, password, success, country_code, country_name"
    INSERT_FIELDS = ( 'timestamp',  'bifrozt_host',
                       'source_ip', 'user', 'password', 'success',
                       'country_code', 'country_name' )
    def __init__(self, localdbaccessor):
        super(AttemptRecordDaoLocal,self).__init__(localdbaccessor)
        
    def get_table_name(self):
        return AttemptRecordDaoLocal.TABLE_NAME
    
    def get_all_fields(self):
        return AttemptRecordDaoLocal.ALL_FIELDS
    
    def get_insert_fields(self):
        return AttemptRecordDaoLocal.INSERT_FIELDS
    
        

class LogRecordDaoLocal(RecordDaoLocal):
    TABLE_NAME = 'log_msg'
    ALL_FIELDS = "db_id, es_id, timestamp, bifrozt_host, server_info, message"
    INSERT_FIELDS = ( 'timestamp', 'bifrozt_host', 'server_info', 'message' )
    def __init__(self, localdbaccessor):
        super(LogRecordDaoLocal,self).__init__(localdbaccessor)
        
    def get_table_name(self):
        return LogRecordDaoLocal.TABLE_NAME
    
    def get_all_fields(self):
        return LogRecordDaoLocal.ALL_FIELDS
    
    def get_insert_fields(self):
        return LogRecordDaoLocal.INSERT_FIELDS
    
class SessionLogDaoLocal(RecordDaoLocal):
    TABLE_NAME = 'session_log_records'
    ALL_FIELDS = ALL_FIELDS = "db_id, es_id, timestamp, bifrozt_host, source_ip, country_code, country_name, channel, message"
    INSERT_FIELDS = ( 'timestamp',  'bifrozt_host',
                       'source_ip', 'country_code', 'country_name', 'channel', 'message' )
        
    def __init__(self, localdbaccessor):
        super(SessionLogDaoLocal,self).__init__(localdbaccessor)
        
    def get_table_name(self):
        return SessionLogDaoLocal.TABLE_NAME
    
    def get_all_fields(self):
        return SessionLogDaoLocal.ALL_FIELDS
    
    def get_insert_fields(self):
        return SessionLogDaoLocal.INSERT_FIELDS


class SessionRecordingDaoLocal(RecordDaoLocal):
    TABLE_NAME = 'session_recordings'
    ALL_FIELDS = ALL_FIELDS = "db_id, es_id, timestamp, bifrozt_host, source_ip, country_code, country_name, filename, contents"
    INSERT_FIELDS = ( 'timestamp',  'bifrozt_host',
                       'source_ip', 'country_code', 'country_name', 'filename', 'contents' )
        
    def __init__(self, localdbaccessor):
        super(SessionRecordingDaoLocal,self).__init__(localdbaccessor)
        
    def get_table_name(self):
        return SessionRecordingDaoLocal.TABLE_NAME
    
    def get_all_fields(self):
        return SessionRecordingDaoLocal.ALL_FIELDS
    
    def get_insert_fields(self):
        return SessionRecordingDaoLocal.INSERT_FIELDS


class SessionDownloadDaoLocal(RecordDaoLocal):
    TABLE_NAME = 'session_downloads'
    ALL_FIELDS = ALL_FIELDS = "db_id, es_id, timestamp, bifrozt_host, source_ip, country_code, country_name, filename, contents"
    INSERT_FIELDS = ( 'timestamp',  'bifrozt_host',
                       'source_ip', 'country_code', 'country_name', 'filename', 'contents' )
        
    def __init__(self, localdbaccessor):
        super(SessionDownloadDaoLocal,self).__init__(localdbaccessor)
        
    def get_table_name(self):
        return SessionDownloadDaoLocal.TABLE_NAME
    
    def get_all_fields(self):
        return SessionDownloadDaoLocal.ALL_FIELDS
    
    def get_insert_fields(self):
        return SessionDownloadDaoLocal.INSERT_FIELDS



