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
from pkg_resources import resource_string

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
        # Where does the user want the database to live?
        name = self._dbconfig['name']
        # turn into an absolute path:
        name = os.path.abspath(name)
        # save absolute path back into config so
        # that other parts of the program can use it.
        self._dbconfig['name'] = name
        # If directory for db doesn't already exist,
        # create it. Use os.makedirs since this does
        # a recursive, multi-level mkdir if needed.
        dbdir = os.path.dirname(name)
        print 'will create db file in {}'.format(dbdir)
        if not os.path.isdir(dbdir):
            os.makedirs(dbdir)
        # Set up db file and tables:
        self.execute_sql_resource('data' + os.sep + 'pogo_schema.sql')
    
    
    """
        Open the named resource, which should be a list
        of sql commands, separated by semicolons. Execute
        each statement.
    """
    def execute_sql_resource(self, resource_name):
        data = resource_string('pogo', resource_name)
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
    