"""
    This class holds methods used by:
        1. main.py in order to set up database at
        program start
        2. record_dao_local.py classes to read from and write to local database
"""


import sqlite3
import os
import os.path
import sys

class LocalDBAccessor(object):
    def __init__(self, dbconfig):
        if not dbconfig:
            raise ValueError('Must supply db configuration')
        self._dbconfig = dbconfig
        self._db = None
        self.initialize_database()
    
    """
        Convenience method to return db, calling db_open() if needed
    """    
    @property
    def db(self):
        return self._db or self.db_open()
    
    """
        If database file doesn't exist:
            create it
        If tables don't exist:
            create them
    """
    def initialize_database(self):
        name = self._dbconfig['name']
        # Get name of directory holding main.py:
        appdir =  os.path.abspath( os.path.dirname(sys.argv[0]) )
        data_dir = os.path.join(appdir, 'data')
        # If name is a relative path, assume that it's relative to
        # appdir and convert it to an absolute path:
        if not os.path.isabs(name):
            name = os.path.join(appdir, name)
            self._dbconfig['name'] = name
        # Ensure that the db directory exists:
        dbdir = os.path.dirname(name)
        if not os.path.isdir(dbdir):
            os.makedirs(dbdir)
        self.execute_sql_file(os.path.join(data_dir,'boing_schema.sql') )
    
    def execute_sql_file(self, filename):
        if not os.path.isfile(filename):
            raise ValueError('Invalid filename')
        # open the file and read its contents:
        data = ''
        with open(filename, 'rt') as f:
            data = f.read()
        data.replace(os.linesep, '') # strip newlines
        data = data.strip() # and leading/trailing whitespace
        commands = data.split(';') # split on SQL end-of-command marker
        cursor = self.db.cursor()
        cursor.execute('BEGIN TRANSACTION')
        for c in commands:
            cursor.execute(c)
        cursor.execute('COMMIT')

    def db_open(self):
        if self._db is None:
            if (self._dbconfig['type'] == 'sqlite'):
                self._db = sqlite3.connect(self._dbconfig['name'])  # @UndefinedVariable
                self._db.isolation_level = None # Do this to turn off automatic transactions.
            else:
                raise ValueError('Unsupported database type')
        return self._db
    
    def db_close(self):
        if self._db is not None:
            self._db.close()
            self._db = None
    