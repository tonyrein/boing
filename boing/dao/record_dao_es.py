"""
    Classes in this file are responsible for writing to and reading from
    the ElasticSearch database on the log server.
"""
import abc
from elasticutils import get_es

class RecordDaoES(object):
    __metaclass__ = abc.ABCMeta
    def __init__(self, es_cfg):
        self._make_es_connection(es_cfg)
        self._assure_index()
        self._assure_mapping()

    def _make_es_connection(self, es_cfg):
        if es_cfg is None:
            raise ValueError("RecordDaoEs needs ElasticSearch Configuration Information")
        else:
            self._es_index = es_cfg['es_index']
            self._es_host = es_cfg['es_host']
            self._es_port = es_cfg['es_port']
            
            # Open the connection to the ES server and make sure our index exists
            es_url = 'http://' + self._es_host
            if self._es_port:
                es_url += ':' + self._es_port
            self._es_connection = get_es(urls=[es_url])
            if not self._es_connection:
                raise Exception("Could not initialize ElasticSearch connection.")

    def _assure_index(self):
        # Make sure our index exists:
        if not self._es_connection.indices.exists(index=self._es_index):
            self._es_connection.indices.create(index=self._es_index)


    def _assure_mapping(self):
        # Verify our mapping exists.
        es_res = self._es_connection.indices.get_mapping(index=self._es_index)
        # If es_res does not have key INDEX_NAME then the mappings don't exist
        # either.
        if (not es_res.has_key(self._es_index) or
            not es_res[self._es_index].has_key('mappings') or
            not es_res[self._es_index]['mappings'].has_key(self.get_document_type())
            ):
            self._es_connection.indices.put_mapping(self.get_document_type(),
                 {'properties': self.get_mapping() }, self._es_index)

        
    def insert_single(self, record):
        d = record.as_dict()
        t = self.get_document_type()
        idx = self._es_index
        r = self._es_connection.index(index=idx, doc_type=t, body=d)
        return r['_id']
    
    abc.abstractmethod
    def get_document_type(self):
        return ''
    
    abc.abstractmethod
    def get_mapping(self):
        return ''
    

class AttemptRecordDaoES(RecordDaoES):
    DOCUMENT_TYPE = 'HonSSH_Attempt'
    MAPPING = {
        "bifrozt_host": {"type": "string", "index": "not_analyzed" },
        "timestamp": {"type": "date", "format": "YYYY-MM-dd HH:mm:SS"},
        "source_ip": {"type": "string", "index": "not_analyzed" },
        "country_name": {"type": "string", "index" : "not_analyzed" },
        "country_code": {"type": "string", "index" : "not_analyzed" },
        "user": {"type" : "string", "index" : "not_analyzed"},
        "password": {"type": "string", "index": "not_analyzed"},
        "success": {"type": "boolean"}
    }
    
    def __init__(self, es_cfg):
        super(AttemptRecordDaoES, self).__init__(es_cfg)
        
    def get_document_type(self):
        return AttemptRecordDaoES.DOCUMENT_TYPE
    
    def get_mapping(self):
        return AttemptRecordDaoES.MAPPING
        


class LogRecordDaoES(RecordDaoES):
    DOCUMENT_TYPE = 'HonSSH_LogEntry'
    MAPPING = {
           "bifrozt_host": {  "type": "string", "index": "not_analyzed" },
          "timestamp": {"type": "date", "format": "YYYY-MM-dd HH:mm:SS"},
           "server_info": {"type": "string", "index": "not_analyzed"},
           "message": {"type": "string"}
           }
    
    def __init__(self, es_cfg):
        super(LogRecordDaoES, self).__init__(es_cfg)


    def get_document_type(self):
        return LogRecordDaoES.DOCUMENT_TYPE
    
    def get_mapping(self):
        return LogRecordDaoES.MAPPING
    
class SessionLogDaoES(RecordDaoES):
    DOCUMENT_TYPE = 'HonSSH_SessionLogEntry'
    MAPPING = {
           "bifrozt_host": {  "type": "string", "index": "not_analyzed" },
           "timestamp": {"type": "date", "format": "YYYY-MM-dd HH:mm:SS"},
           "source_ip": { "type": "string", "index": "not_analyzed" },
           "country_name": { "type": "string", "index" : "not_analyzed" },
           "country_code": { "type": "string", "index" : "not_analyzed" },
           "channel": {"type": "string", "index": "not_analyzed"},
           "message": {"type": "string"}
           }

    def __init__(self, es_cfg):
        super(SessionLogDaoES, self).__init__(es_cfg)

    def get_document_type(self):
        return SessionLogDaoES.DOCUMENT_TYPE
    
    def get_mapping(self):
        return SessionLogDaoES.MAPPING


class SessionRecordingDaoES(RecordDaoES):
    DOCUMENT_TYPE = 'HonSSH_SessionRecording'
    MAPPING = {
       "bifrozt_host": {  "type": "string", "index": "not_analyzed" },
       "timestamp": {"type": "date", "format": "YYYY-MM-dd HH:mm:SS"},
       "source_ip": { "type": "string", "index": "not_analyzed" },
       "country_name": { "type": "string", "index" : "not_analyzed" },
       "country_code": { "type": "string", "index" : "not_analyzed" },
       "filename": {"type": "string", "index": "not_analyzed"},
       "contents": {"type": "string", "index": "no"}
       }
    
    def __init__(self, es_cfg):
        super(SessionRecordingDaoES, self).__init__(es_cfg)

    def get_document_type(self):
        return SessionRecordingDaoES.DOCUMENT_TYPE
    
    def get_mapping(self):
        return SessionRecordingDaoES.MAPPING


class SessionDownloadDaoES(RecordDaoES):
    DOCUMENT_TYPE = 'HonSSH_SessionDownload'
    MAPPING = {
       "bifrozt_host": {  "type": "string", "index": "not_analyzed" },
       "timestamp": {"type": "date", "format": "YYYY-MM-dd HH:mm:SS"},
       "source_ip": { "type": "string", "index": "not_analyzed" },
       "country_name": { "type": "string", "index" : "not_analyzed" },
       "country_code": { "type": "string", "index" : "not_analyzed" },
       "filename": {"type": "string", "index": "not_analyzed"},
       "contents": {"type": "string", "index": "no"}
       }
    
    def __init__(self, es_cfg):
        super(SessionDownloadDaoES, self).__init__(es_cfg)

    def get_document_type(self):
        return SessionDownloadDaoES.DOCUMENT_TYPE
    
    def get_mapping(self):
        return SessionDownloadDaoES.MAPPING



    
