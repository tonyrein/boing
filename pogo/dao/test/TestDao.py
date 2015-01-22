'''
Created on Jan 13, 2015

@author: tony
'''
import unittest
import sqlite3

from pogo.dao.record_dao_local import AttemptRecordDaoLocal, LogRecordDaoLocal
from pogo.dao.record_dao_local import SessionDownloadDaoLocal, SessionLogDaoLocal, SessionRecordingDaoLocal

from pogo.util.config import StretchConfig
from pogo.util.util import get_geo_info
from pogo.dto.record import AttemptRecord


class TestDaoLocal(unittest.TestCase):


    def drop_tables(self):
        c = self._db.cursor()
        c.execute('BEGIN TRANSACTION')
        for t in self.tables:
            c.execute('DROP TABLE IF EXISTS ' + t)
        c.execute('COMMIT')
        

    def create_tables(self):
        c = self._db.cursor()
        c.execute('BEGIN TRANSACTION')
        for sql in self.table_creation_commands:
            c.execute(sql)
        c.execute('COMMIT')

        
    def setUp(self):
        self.tables = ['attempts', 'log_msg', 'session_downloads', 'session_log_records', 'session_recordings']
        self.table_creation_commands = [
            "CREATE TABLE attempts ( db_id INTEGER PRIMARY KEY AUTOINCREMENT, es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER,    bifrozt_host TEXT,    source_ip TEXT,    user TEXT, password TEXT, success INTEGER, country_code TEXT, country_name TEXT);",
            "CREATE TABLE log_msg ( db_id INTEGER PRIMARY KEY AUTOINCREMENT,    es_id TEXT NOT NULL DEFAULT '',    timestamp INTEGER,    bifrozt_host TEXT, server_info TEXT, message TEXT );",
            "CREATE TABLE session_downloads (db_id  INTEGER PRIMARY KEY AUTOINCREMENT, es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER, bifrozt_host TEXT, source_ip TEXT, country_code TEXT, country_name TEXT, filename TEXT NOT NULL, contents TEXT );",
            "CREATE TABLE session_log_records (db_id  INTEGER PRIMARY KEY AUTOINCREMENT, es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER, bifrozt_host TEXT, source_ip TEXT, country_code TEXT, country_name TEXT, channel TEXT, message TEXT );",
            "CREATE TABLE session_recordings (db_id  INTEGER PRIMARY KEY AUTOINCREMENT, es_id TEXT NOT NULL DEFAULT '', timestamp INTEGER, bifrozt_host TEXT, source_ip TEXT, country_code TEXT, country_name TEXT, filename TEXT NOT NULL, contents TEXT );"
            ]

        self.dbconfig = {
                  'type': 'sqlite',
                  'host': '',
                  'port': '',
                  'user': '',
                  'password': '',
                  'name': 'boing.db'
                  }
        self._db = sqlite3.connect(self.dbconfig['name'])  # @UndefinedVariable
        self._db.isolation_level = None
        self.assertIsNotNone(self._db, "Test suite could not initialize db")
        self.drop_tables()
        self.create_tables()
        
    

    def tearDown(self):
        pass


    def testAttemptRecordDaoLocalInit(self):
        daolocal = AttemptRecordDaoLocal(self.dbconfig)
        self.assertIsNotNone(daolocal._db, "Did not initialize db")
        
    def testAttemptRecordDaoLocalInsertSingle(self):
        daolocal = AttemptRecordDaoLocal(self.dbconfig)
        r = AttemptRecord()
        r.source_ip = '192.168.0.1'
        r.timestamp = '2015-01-13 20:22:00'
        r.bifrozt_host = 'phoebe'
        r.user = 'donaldduck'
        r.password = 'secret'
        r.success = '0'
        pgi = get_geo_info(r.source_ip)
        r.country_code = pgi.country_code
        r.country_name = pgi.country_name
        daolocal.insert_single(r)
        list_where = daolocal.list_where("es_id = ''")
        self.assertEqual(len(list_where), 1, "select did not return correct number of items")
        row = list_where[0]
        r2 = AttemptRecord()
        [ r2.timestamp, r2.bifrozt_host, r2.source_ip, r2.user, r2.password, r2.success, r2.country_code, r2.country_name] = row[2:]
        if not r2.success:
            r2.success = '0'
        else:
            r2.success = str(r2.success)
        self.assertEquals(r, r2,  "Two records don't match")



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()