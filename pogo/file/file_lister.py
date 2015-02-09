
import abc
import os
import os.path
import re
import datetime
import time

from pogo.file.stretch_file import AttemptFile, LogFile
from pogo.file.stretch_file import  SessionLogFile, SessionDownloadFile, SessionRecordingFile

"""
    Base class for objects that keep track of lists of files of one specific
    HonSSH-generated type, for example attempt files or session recordings.
    Child classes differentiate themselves by implementing the methods decorated
    with @abc.abstractmethod, and optionally overriding other methods, such
    as my_directories().
"""
class FileLister(object):
    __metaclass__ = abc.ABCMeta

    DONE_EXTENSION = '.DONE'

    # Some static methods
    @staticmethod
    def done_name(stretch_file_name):
        return stretch_file_name + FileLister.DONE_EXTENSION

    @staticmethod
    def is_done(stretch_file_name):
        # return True if there's a file named filespec + '.DONE'
        return os.path.isfile(FileLister.done_name(stretch_file_name)) and not FileLister.is_a_done_marker(stretch_file_name)

    """
        Does this file mark another file as "done," or does it
        simply happen to end with DONE_EXTENSION? (That would
        be legal for a session download file.)
    """
    @staticmethod
    def is_a_done_marker(name):
        # Is there another file with the same name as this one, minus DONE_EXTENSION?
        return os.path.isfile(name[0:len(name) - len(FileLister.DONE_EXTENSION)]) and name.endswith(FileLister.DONE_EXTENSION)

    @staticmethod
    def mark_as_done(stretch_file_object):
        # open().close() is equivalent of 'touch' - creates
        # file if it doesn't already exist.
        fname = FileLister.done_name(stretch_file_object.name())
        open(fname, 'a').close()



    def __init__(self, source_dir):
        if not os.path.isdir(source_dir):
            raise ValueError(source_dir + ' is not a directory.')
        else:
            self.source_dir = source_dir
            self._pending_file_names = []
            self._done_file_names = []
            self._pending_file_objects = []
            self.set_latest_timestamp_to_process()

    """
        Generate a list of directories to check for our files
    """
    def my_directories(self):
        dirlist = [self.source_dir]
        for (root, dirs, files) in os.walk(self.source_dir):
            dirlist += [ os.path.join(root,dir) for dir in dirs ]
        return dirlist

    """
        Given a file name, is this a file that this file lister should process?
    """
    def one_of_my_files(self, name):
        return self.get_filespec_pattern().match(name)

    """
        Generate two lists of files: 1) files that still need
        to be processed, and 2) files that are done.
    """
    def load_file_name_lists(self):
        self._pending_file_names = []
        self._done_file_names = []
        for dn in self.my_directories():
            for name in os.listdir(dn):
                if not self.one_of_my_files(name): continue
                whole_path = os.path.join(dn, name)
                if not os.path.isfile(whole_path): continue #isfile() returns True for symlinks to files.
                if os.path.islink(whole_path): # If it is a symlink, use the actual file instead.
                    path_to_add = os.path.realpath(whole_path)
                else:
                    path_to_add = whole_path
                # Don't process the files that are there merely to mark another file as "done."
                if FileLister.is_a_done_marker(path_to_add): continue
                # Don't process files that have already been done - add these to the "done" list instead.
                if self.is_done(path_to_add):
                    self._done_file_names.append(path_to_add)
                else:
                    self._pending_file_names.append(path_to_add)
        # Eliminate duplicates -- possible with symlinks:
        self._pending_file_names = list(set(self._pending_file_names))
        self._pending_file_names.sort() # simple ascending sort of names
        self._done_file_names = list(set(self._done_file_names))
        self._done_file_names.sort()


    @abc.abstractmethod
    def get_file_class(self):
        return ''

    @abc.abstractmethod
    def get_filespec_pattern(self):
        return ''


    def __iter__(self):
        return iter(self._pending_file_objects)

    def __len__(self):
        return len(self._pending_file_objects)

    """
        Get the timestamp of 23:59:59: last night. We
        don't want to process any files unless they're
        earlier than that.
    """
    def set_latest_timestamp_to_process(self):
        date_yesterday = datetime.date.today() - datetime.timedelta(days=1)
        mn_last_night= datetime.datetime.combine(date_yesterday, datetime.time(23,59,59) )
        self._latest_timestamp_to_process = time.mktime(mn_last_night.timetuple())

    def load_pending_file_objects(self):
        # Don't process files from today...
        file_class = self.get_file_class()
        self._pending_file_objects = []
        for name in self._pending_file_names:
            file_mtime = os.stat(name).st_mtime
            if file_mtime < self._latest_timestamp_to_process:
                self._pending_file_objects.append(file_class(name))

    def delete_done_files(self):
        count_removed = 0
        for name in self._done_file_names:
            try:
                os.remove(name)
                os.remove(self.done_name(name))
                count_removed += 1
            except OSError, e:  ## if failed, report it back to the user ##
                print "Error: {0} - {1}.".format(e.filename,e.strerror)
        return count_removed


class AttemptFileLister(FileLister):

    FILESPEC_PATTERN=re.compile('^\d{8}$')

    def __init__(self, source_dir):
        super( AttemptFileLister, self ).__init__(source_dir)

    def get_file_class(self):
        return AttemptFile

    def get_filespec_pattern(self):
        return AttemptFileLister.FILESPEC_PATTERN


class LogFileLister(FileLister):
    FILESPEC_PATTERN = re.compile('^honssh\.log.*')

    def __init__(self, source_dir):
        super( LogFileLister, self ).__init__(source_dir)

    def get_file_class(self):
        return LogFile

    def get_filespec_pattern(self):
        return LogFileLister.FILESPEC_PATTERN



class SessionLogFileLister(FileLister):
    FILESPEC_PATTERN = re.compile('.*\.log') # This regex pattern will stop working in 3000 AD!
    def __init__(self, source_dir):
        super( SessionLogFileLister, self ).__init__(source_dir)

    """
        Override my_directories because we don't search recursively for this kind of file
    """
    def my_directories(self):
        dirs = [ os.path.join(self.source_dir, d) for d in os.listdir(self.source_dir) if os.path.isdir(os.path.join(self.source_dir, d)) ]
        return dirs

    def get_file_class(self):
        return SessionLogFile

    def get_filespec_pattern(self):
        return SessionLogFileLister.FILESPEC_PATTERN


class SessionRecordingFileLister(FileLister):
    FILESPEC_PATTERN = re.compile('.*\.tty')
    def __init__(self, source_dir):
        super( SessionRecordingFileLister, self ).__init__(source_dir)

    def get_file_class(self):
        return SessionRecordingFile

    def get_filespec_pattern(self):
        return SessionRecordingFileLister.FILESPEC_PATTERN

    """
        Override my_directories because we don't search recursively for this kind of file
    """
    def my_directories(self):
        dirs = [ os.path.join(self.source_dir, d) for d in os.listdir(self.source_dir) if os.path.isdir(os.path.join(self.source_dir, d)) ]
        return dirs

class SessionDownloadFileLister(FileLister):
    FILESPEC_PATTERN = None # This pattern is not relevant for htis class
    def __init__(self, source_dir):
        super( SessionDownloadFileLister, self ).__init__(source_dir)

    def get_file_class(self):
        return SessionDownloadFile

    def get_filespec_pattern(self):
        return SessionDownloadFileLister.FILESPEC_PATTERN

    """
        Override my_directories because we don't search recursively for this kind of file
    """
    def my_directories(self):
        dirs = [ os.path.join(self.source_dir, d, 'downloads') for d in os.listdir(self.source_dir) if os.path.isdir(os.path.join(self.source_dir, d, 'downloads')) ]
        return dirs

    """
        Override this for this class -- these file names can be any file name
        legal in the system.
    """
    def one_of_my_files(self, name):
        return True

