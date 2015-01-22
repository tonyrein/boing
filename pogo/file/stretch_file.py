"""
    A StretchFile reads disk files into RAM and creates Record objects from their contents.
"""
import abc
import os
import os.path
import re

from pogo.dto.record import LogRecord, AttemptRecord, SessionLogRecord, SessionDownloadFileRecord
from pogo.dto.record import SessionRecordingRecord
from pogo.util.util import get_geo_info

class StretchFile(object):
    __metaclass__ = abc.ABCMeta
    def __init__(self, file_name):
        self._name = file_name
        self._entry_list = []
        self._loaded = False
    
    def name(self):
        return self._name
    
    def __iter__(self):
        return iter(self._entry_list)
    
    # override __len__ so that len(bungeedatafile)
    # returns the number of files in entry_list
    def __len__(self):
        return len(self._entry_list)
    
    @abc.abstractmethod
    def load(self):
        pass


class AttemptFile(StretchFile):
    def __init__(self, file_name):
        super(AttemptFile, self).__init__(file_name)

    def load(self):
        if os.path.isfile(self.name()):
            try:
                f = open(self.name(), "rt")
                try:
                    for line in f:
                        r = AttemptRecord(line.rstrip())
                        self._entry_list.append(r)
                    f.close()
                    self._loaded = True
                    return True
                except IOError:
                    if f is not None:
                        f.close()
                    logging.error("Failed to load file ", exc_info = True)
                    print "During loading of " + self.name() + " encountered i/o error reading line " + line
                    self._loaded = False
                    return False
            except IOError:
                logging.error("Could not open file", exc_info = True)
                self._loaded = False
                return False
        else: 
            raise ValueError(self.name() + " is not a file.")
            self._loaded = False
            return False
    

class LogFile(StretchFile):
    EXTRA_LINE_PATTERN = re.compile('^\t')

    def __init__(self, file_name):
        super(LogFile, self).__init__(file_name)
        
    def load(self):
        if os.path.isfile(self.name()):
            try:
                f = open(self.name(), "rt")
                try:
                    for line in f:
                        if LogFile.EXTRA_LINE_PATTERN.match(line):
                            # Don't use this to construct a
                            # LogRecord. Instead, tack it on to
                            # the end of the last entry's message
                            # field.
                            if len(self._entry_list) > 0:
                                r = self._entry_list.pop()
                                r.message += ' -- ' + line.strip()
                        else:
                            r = LogRecord(line.rstrip()) 
                        self._entry_list.append(r)
                        
                    f.close()
                    self._loaded = True
                    return True
                except IOError:
                    if f is not None:
                        f.close()
                    logging.error("Could not read a line in file", exc_info = True)    
                    print "During loading of " + self.name() + " encountered i/o error reading line " + line
                    self._loaded = False
                    return False
            except IOError:
                logging.error("Failed to load file ", exc_info = True)
                print "Error during loading of file " + self.name()
                self._loaded = False
                return False
        else: 
            raise ValueError(self.name() + " is not a file.")
            self._loaded = False
            return False
# end of LogFile.load())


class SessionLogFile(StretchFile):
    def __init__(self, file_name):
        super(SessionLogFile, self).__init__(file_name)
        # Get second-to-last element of filespec -- this is the IP address
        source_ip = file_name.split(os.sep)[-2]
        self.set_source_ip(source_ip)
        
    def set_source_ip(self, source_ip):
        if source_ip:
            self.source_ip = source_ip
            gpi = get_geo_info(self.source_ip)
            self.country_code = gpi.country_code
            self.country_name = gpi.country_name
        else:
            self.source_ip = ''
            self.country_code = ''
            self.country_name = ''

        
    def load(self):
        if os.path.isfile(self.name()):
            try:
                f = open(self.name(), "rt")
                try:
                    for line in f:
                        r = SessionLogRecord(line)
                        r.set_source_ip(self.source_ip)
                        r.set_country_info(self.country_code, self.country_name)
                        self._entry_list.append(r)
                    f.close()
                    self._loaded = True
                    return True
                except IOError:
                    if f is not None:
                        f.close()
                        logging.error("Failed to load file ", exc_info = True)
                        print "During loading of " + self.name() + " encountered i/o error reading line " + line
                    self._loaded = False
                    return False
            except IOError:
                logging.error("Failed to load file ", exc_info = True)
                print "Error during loading of file " + self.name()
                self._loaded = False
                return False
        else: 
            raise ValueError(self.name() + " is not a file.")
            self._loaded = False
            return False
# end of SessionLogFile.load())


class SessionDownloadFile(StretchFile):
    def __init__(self, file_name):
        super(SessionDownloadFile, self).__init__(file_name)
        # IP is third-to-last element of filespec
        source_ip = file_name.split(os.sep)[-3]
        self.set_source_ip(source_ip)
        
    def set_source_ip(self, source_ip):
        if source_ip:
            self.source_ip = source_ip
            gpi = get_geo_info(self.source_ip)
            self.country_code = gpi.country_code
            self.country_name = gpi.country_name
        else:
            self.source_ip = ''
            self.country_code = ''
            self.country_name = ''

        
    def load(self):
        if os.path.isfile(self.name()):
            try:
                with open(self.name(), "rb") as f:
                    data = f.read()
                    if data:
                        data64 = data.encode("base64")
                    else:
                        data64 = ''
                    r = SessionDownloadFileRecord(self.name(), data64)
                    # Part of file name is a datetime stamp. Extract it
                    namepart = self.name().split(os.sep)[-1] # get last element of filespec
                    # datetime string in format expected by set_timestamp():
                    normalized_namepart = ( namepart[0:4] + '-'
                                             + namepart[4:6] + '-'
                                             + namepart[6:8] + ' '
                                             + namepart[9:11] + ':'
                                             + namepart[11:13] + ':'
                                             + namepart[13:15] )
                    r.set_timestamp(normalized_namepart)
                    r.set_source_ip(self.source_ip)
                    r.set_country_info(self.country_code, self.country_name)
                    self._entry_list.append(r)
                    self._loaded = True
                    return True
            except IOError:
                logging.error("Failed to load file ", exc_info = True)
                print "During loading of " + self.name() + " encountered i/o error"
                self._loaded = False
                return False
        else: 
            raise ValueError(self.name() + " is not a file.")
            self._loaded = False
            return False
# end of SessionDownloadFile.load())


class SessionRecordingFile(StretchFile):
    def __init__(self, file_name):
        super(SessionRecordingFile, self).__init__(file_name)
        # IP is second-to-last element of filespec
        source_ip = file_name.split(os.sep)[-2]
        self.set_source_ip(source_ip)
        
    def set_source_ip(self, source_ip):
        if source_ip:
            self.source_ip = source_ip
            gpi = get_geo_info(self.source_ip)
            self.country_code = gpi.country_code
            self.country_name = gpi.country_name
        else:
            self.source_ip = ''
            self.country_code = ''
            self.country_name = ''

        
    def load(self):
        if os.path.isfile(self.name()):
            try:
                with open(self.name(), "rb") as f:
                    data = f.read()
                    if data:
                        data64 = data.encode("base64")
                    else:
                        data64 = ''
                    r = SessionRecordingRecord(self.name(), data64)
                    # Part of file name is a datetime stamp. Extract it
                    namepart = self.name().split(os.sep)[-1] # get last element of filespec
                    # datetime string in format expected by set_timestamp():
                    normalized_namepart = ( namepart[0:4] + '-'
                                             + namepart[4:6] + '-'
                                             + namepart[6:8] + ' '
                                             + namepart[9:11] + ':'
                                             + namepart[11:13] + ':'
                                             + namepart[13:15] )
                    r.set_timestamp(normalized_namepart)
                    r.set_source_ip(self.source_ip)
                    r.set_country_info(self.country_code, self.country_name)
                    self._entry_list.append(r)
                    self._loaded = True
                    return True
            except IOError:
                logging.error("Failed to load file ", exc_info = True)
                print "During loading of " + self.name() + " encountered i/o error"
                self._loaded = False
                return False
        else: 
            raise ValueError(self.name() + " is not a file.")
            self._loaded = False
            return False
# end of SessionRecordingFile.load())



