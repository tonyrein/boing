import iso8601
import time
from datetime import datetime
from tzlocal import get_localzone
from geoip import geolite2
import logging
import os.path
import sqlite3
import sys
import tarfile
        

# Convert timestamp in local time to GMT:
# Requires modules iso8601 and time
def local_timestamp_to_gmt(localtimestamp):
    if not localtimestamp:
        return None
    d = iso8601.parse_date(localtimestamp)
    utc_seconds = time.mktime(d.timetuple())
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(utc_seconds))


"""
    We're given a string representing a date and time.
    We know it's in US Eastern Time, but don't know whether
    daylight savings time was in effect at that time. In
    other words, we know the offset from UTC is either -5 hours
    or - 4 hours, but we don't know which. We'd like to
    convert that date/time to UTC.
"""
def local_no_tz_to_utc(datetime_string):
    if not datetime_string:
        return None
    lnaive = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
    tz=get_localzone()
    l_with_tz = tz.localize(lnaive)
    #l_with_tz now has timezone info
    utc_seconds = time.mktime(l_with_tz.timetuple())
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(utc_seconds))

"""
    Given a logging level string such as 'DEBUG' or 'INFO',
    get the corresponding numeric logging level.
"""
def logging_level_from_string(levelstr):
    numeric_level = getattr(logging, levelstr.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % levelstr)
    return numeric_level

"""
    Use the information in the app's configuration
    to set logging options.
"""
def configure_logging(loginfo):
    fn=loginfo()['filename']
    levelstr=loginfo()['level']
    if levelstr == '': levelstr = 'NOTSET'
    level = logging_level_from_string(levelstr)
    if fn != 'CONSOLE':
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', filename=fn, level=level)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=level)
    return logging.getLogger()

"""
    Generate an archive name
    based on the passed-in prefix and the current date and time.
"""
def generate_archive_name(prefix = None):
    utcnow = datetime.utcnow()
    if prefix is None:
        prefix = ''
    return prefix + utcnow.isoformat() + '.bz2'

"""
    Given a list of files and an archive file name,
    create the archive file and add the files from the list.
    Try to figure out what kind of compression to use
    based on the archive file extension.
"""
def archive_file_list(filename, files):
    comp_mode = ''
    if filename.endswith('bz2'): comp_mode = ':bz2'
    if filename.endswith('gz'): comp_mode = ':gz'
    open_mode_string = 'w' + comp_mode
    with tarfile.open(filename, open_mode_string) as tf:
        for fn in files:
            tf.add(fn)


class PogoGeoInfo(object):
    def __init__(self, ipi=None): # ipi is a geoip.IPInfo
        if ipi is None:
            self.country_name = ''
            self.country_code = ''
        else:
            self._info_dict = ipi.get_info_dict()
            # several pieces of data in _info_dict are used almost
            # every time this class is used, so save them in an
            # easily-accessible place:
            # Other items are still accessible by dereferencing _info_dict.
            if not self._info_dict['country']:
                self.country_name = ''
                self.country_code = ''
            else:
                if self._info_dict['country']['names'] and self._info_dict['country']['names']['en']:
                    self.country_name = self._info_dict['country']['names']['en']
                else:
                    self.country_name = ''
                if self._info_dict['country']['iso_code']:
                    self.country_code = self._info_dict['country']['iso_code']
                else:
                    self.country_code = ''

"""
    Return an object holding information about
    location of given ip address. If no ip address
    is given, instantiate a PgooGeoInfo object with None,
    which will set country_code and country_name to ''.
"""
def get_geo_info(ipaddress):
    if not ipaddress:
        return PogoGeoInfo()
    else:
        return PogoGeoInfo(geolite2.lookup(ipaddress))
    
     