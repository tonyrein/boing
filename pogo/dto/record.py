"""
    dto.Record is an abstract base class. Its subclasses hold
    information about individual data records, which are
    represented by individual lines in the HonSSH-generated files,
    individual rows in the local db storage tables, or individual
    "documents" in the ElasticSearch database.
"""
import abc
from pogo.util.util import local_timestamp_to_gmt, local_no_tz_to_utc, get_geo_info

from socket import gethostname
class Record(object):
    def __init__(self):
        __metaclass__ = abc.ABCMeta
        self.bifrozt_host = gethostname()
        self.db_id = ''
        self.es_id = ''
    @abc.abstractmethod    
    def as_dict(self):
        raise Exception('Abstract methods should not be called.')
    
    # Comparison operator method
    def __eq__(self, other):
        return self.__dict__ == other.__dict__
    
    def __ne__(self, other):
        return not (self == other)
    # end of base class
    
class AttemptRecord(Record):
    def __init__(self, logLine=None):
        super( AttemptRecord, self ).__init__()
        
        # If we were passed a string, use that to
        # initialize ourself:
        if logLine is not None:
            splitList = logLine.split(',')
            # if < 5 elements in splitList, append '' to bring length to 5
            while (len(splitList) < 5):
                splitList.append('')
            timestring = splitList[0]
            # We know that timestring is in Eastern time, but
            # don't know if daylight savings time was in effect or
            # not. Use local_no_tz_to_utc() to convert to UTC.
            self.timestamp = local_no_tz_to_utc(timestring)
            self.source_ip = splitList[1]
            # Try to protect against invalid chars
            # in user-entered values:
            self.user = unicode(splitList[2], errors='replace')
            self.password = unicode(splitList[3], errors='replace')
            self.success = splitList[4]
            if not self.success: self.success = '0'
            gpi = get_geo_info(self.source_ip)
            self.country_code = gpi.country_code
            self.country_name = gpi.country_name
        else:
            # initialize fields to empty strings or null values:
            self.timestamp = ''
            self.source_ip = ''
            self.user = ''
            self.password = ''
            self.success = '0' # assume failure
            self.country_code = ''
            self.country_name = ''
    
    @abc.abstractmethod
    def as_dict(self):
        # make a structure to hold our stuff
        adict = {}
        if self.timestamp:
            # if timestamp exists and isn't blank...
            adict['bifrozt_host'] = self.bifrozt_host
            adict['source_ip'] = self.source_ip
            adict['user'] = self.user
            adict['password'] = self.password
            adict['success'] = self.success
            adict['timestamp'] = self.timestamp
            adict['country_name'] = self.country_name
            adict['country_code'] = self.country_code
        return adict   
    
    # end of AttemptRecord class  
    
class LogRecord(Record):
    def __init__(self, log_line = None):
        super( LogRecord, self ).__init__()
        if log_line is not None:
            splitList = log_line.split(' ')
            local_time_string = splitList[0] + ' ' + splitList[1]
            self.timestamp = local_timestamp_to_gmt(local_time_string)
            self.server_info = splitList[2]
            self.message = splitList[3]
        else:
            # initialize to empty fields
            self.timestamp = ''
            self.server_info = ''
            self.message = ''
        
    @abc.abstractmethod    
    def as_dict(self):
        # make a structure to hold our stuff
        adict = {}
        if self.timestamp:
            # if timestamp exists and isn't blank...
            adict['bifrozt_host'] = self.bifrozt_host
            adict['timestamp'] = self.timestamp
            adict['server_info'] = self.server_info
            adict['message'] = self.message
        return adict





class SessionRecord(Record):
    def __init__(self):
        super( SessionRecord, self ).__init__()

    def as_dict(self):
        # make a structure to hold our stuff
        adict = {}
        if self.bifrozt_host:
            adict['bifrozt_host'] = self.bifrozt_host
            adict['timestamp'] = self.timestamp
            adict['source_ip'] = self.source_ip
            adict['country_name'] = self.country_name
            adict['country_code'] = self.country_code
        return adict   

    def set_source_ip(self, source_ip):
        if source_ip:
            self.source_ip = source_ip
        else:
            self.source_ip = ''
    
    def set_country_info(self, code, name):
        if code:
            self.country_code = code
        else:
            self.country_code = ''
        if name:
            self.country_name = name
        else:
            self.country_name = ''
            
    def set_timestamp(self, timestring):
        if timestring:
            self.timestamp = local_no_tz_to_utc(timestring)
        else:
            self.timestamp = ''
        

class SessionLogRecord(SessionRecord):
    def __init__(self, line = None):
        super( SessionLogRecord, self ).__init__()
        if line :
            line = line.strip()
            if len(line) > 30:
                self.set_timestamp(line[0:19])
                lb_ind = line.index('[')
                rb_ind = line.index(']')
                chan = line[lb_ind+1:rb_ind] # channel
                self.channel = chan.strip()
                # Now take everything after the last ']':
                self.message = line[rb_ind+1:] # rest of the line
            else:
                self.set_timestamp('')
                self.set_source_ip('')
                self.set_country_info('', '')
                self.channel = ''
                self.message = ''
        else:
            self.set_timestamp('')
            self.set_source_ip('')
            self.set_country_info('', '')
            self.channel = ''
            self.message = ''
    
    def as_dict(self):
        # make a structure to hold our stuff
        adict = super(SessionLogRecord,self).as_dict()
        adict['channel'] = self.channel
        adict['message'] = self.message
        return adict   


class SessionRecordingRecord(SessionRecord):
    def __init__(self, filename=None, contents=None):
        super( SessionRecordingRecord, self ).__init__()
        self.set_contents(contents)
        self.set_filename(filename)
            
    def set_filename(self, name):
        if name is not None:
            self.filename = name
        else:
            self.filename = ''
    
    def set_contents(self, contents):
        if contents is not None:
            self.contents = contents
        else:
            self.contents = ''
    
    def as_dict(self):
        # make a structure to hold our stuff
        adict = super(SessionRecordingRecord,self).as_dict()
        adict['filename'] = self.filename
        adict['contents'] = self.contents
        return adict   

class SessionDownloadFileRecord(SessionRecord):
    def __init__(self, fname=None, contents=None):
        super( SessionDownloadFileRecord, self ).__init__()
        self.set_contents(contents)
        self.set_filename(fname)
            
    def set_filename(self, name):
        if name is not None:
            self.filename = name
        else:
            self.filename = ''
    
    def set_contents(self, contents):
        if contents is not None:
            self.contents = contents
        else:
            self.contents = ''
    
    def as_dict(self):
        # make a structure to hold our stuff
        adict = super(SessionDownloadFileRecord,self).as_dict()
        adict['filename'] = self.filename
        adict['contents'] = self.contents
        return adict   




"""
    While the Log and Attempt record types are collections of strings, a Session is
    not. HonSSH generates the following kinds of session information:
        1. session logs. These are simple text files, with each line listing:
            A. a timestamp
            B. a "channel," (ie, whether this event involved communication via an SSH connection,
            typing at a terminal command line, or something else.
            C. a description, or the text of the command executed
            
        2. recording files. These are binary files in a format suitable for playback with
        the playback.py utility included with HonSSH.
        
        3. timestamped copies of any files downloaded by users during SSH sessions.
        
    The session information can be associated with an originating IP because HonSSH stores
    all of this in a directory with that IP as its name.
    
    However, it's difficult to associate a given file, for example, with a particular session log or
    session recording -- their dates and times may or may not be the same, depending on
    the exact sequence and timing of events. Therefore, this program will treat the types of
    information as separate entities rather than as part of a single "session object."
    
    Thus, the session-related objects handled by this program are:
        class SessionLog
        class SessionRecording
        class SessionDownloadFile
        
    SessionLog
        This is a collection of SessionLogEntry objects. In other words, the session log file is parsed
        into records, very similarly to the way the regular log files are parsed, but the records from
        a given session log are kept together.
        
    SessionRecording
        This is a plain-text file name, and "black box" contents. When the SessionRecording object is initialized
        from a recording file, the contents are base64-encoded and stored, without any provision for allowing
        ElasticSearch queries of the contents.
        
    SessionDownloadFile
        This is handled the same way as the session recording -- the contents of each file are base64-encoded, but
        their names are stored as plain text. Each file is handled separately -- to see which ones were downloaded
        as part of which session, it's necessary to look at the session logs or play back the recordings from
        the relevant dates and times.
        
   
""" 
