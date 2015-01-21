import ConfigParser
import os
import os.path
# from os.path import dirname
import sys

class StretchConfig(object):
    
    def __init__(self):
        config_file_search_path = [ '/etc/default/boing.cfg', '/etc/boing/boing.cfg', '/etc/boing.cfg',
                            '~/.config/boing/boing.cfg', './boing.cfg']
                # Default settings:
        def_top_dir = '/opt/honssh'
        def_db_dir = os.path.dirname( os.path.abspath(sys.argv[0]) ) + os.sep + 'db'
        self._settings = {
                        'top_dir': def_top_dir,
                        'debug': 0,
                        'locations': {
                                      'log_dir': '/opt/honssh/logs',
                                      'session_dir': '/opt/honssh/sessions',
                                      'attempt_dir': '/opt/honssh/logs',
                                      'arc_dir': '/opt/honssh/archives'
                                      },
                        'elasticsearch': {
                                          'es_host': 'localhost',
                                          'es_port': '9200',
                                          'hon_index': 'hon_ssh'                  
                                          },
                        'db_connection': {
                                          'type': 'sqlite',
                                          'host': '',
                                          'port': '',
                                          'user': '',
                                          'password': '',
                                          'name': def_db_dir + '/boing.db'
                                          },
                          'logging': {
                                      'filename': 'CONSOLE',
                                      'level': 'WARNING'
                                      }
                        }
        
        # See if we can read a config file:
        cfg = ConfigParser.SafeConfigParser()
        self.config_files_read = cfg.read(config_file_search_path)

        
        # If we found a config file, override
        # defaults with values from config file:
        # special case for debug, since it's a boolean (rest are strings)
        if cfg.has_section('main'):
            self._settings['debug'] = cfg.getboolean('main', 'debug')
  
        for section in ('locations', 'db_connection', 'elasticsearch', 'logging'):
            if cfg.has_section(section):
                for item in cfg.items(section):
                    self._settings[section][item[0]] = item[1]
#         
            

    def __str__(self, *args, **kwargs):
        retStr = 'StretchConfig: \n\tDebug: ' + str(self._settings['debug']) + '\n'
        for section in ('locations', 'db_connection', 'elasticsearch', 'logging'):
            retStr += '\t' + section + ' section:\n'
            for key in self._settings[section]:
                retStr += '\t\t' + key + ': ' + self._settings[section][key] + '\n'
        return retStr
    
    def get_db_info(self):
        return self._settings['db_connection']
    
    def get_locations(self):
        return self._settings['locations']
    
    def get_es_info(self):
        return self._settings['elasticsearch']
    
    def get_logging_info(self):
        return self._settings['logging']
            

if __name__ == '__main__':
    bc = StretchConfig()
    if bc.config_files_read is not None and len(bc.config_files_read) > 0:
        print "Read one or more config files: "
        for f in bc.config_files_read: print f
    else:
        print "No config files read; using default values"
    
    print bc