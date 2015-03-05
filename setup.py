'''
boing: Boing is a utility for putting data generated by a HonSSH honeypot into an Elasticsearch database.

Note that "python setup.py test" invokes pytest on the package. With appropriately
configured setup.cfg, this will check both xxx_test modules and docstrings.

Copyright 2015, Tony Rein.
Licensed under MIT.
'''
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
from setuptools.command.install import install as _install
from pkg_resources import resource_stream
import shutil



# This is a plug-in for setuptools that will invoke py.test
# when you run python setup.py test
class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest  # import here, because outside the required eggs aren't loaded yet
        sys.exit(pytest.main(self.test_args))


version = "0.9.5.0"

class install(_install):
    def install_config_file(self):
        instream = resource_stream('pogo', '/data/pogo.cfg')
        outfilename = '/etc/pogo.cfg'
        with open(outfilename, 'wt') as f:
            shutil.copyfileobj(instream, f)
    def install_logrotate_conf(self):
        instream = resource_stream('pogo', '/data/logrotate.cfg')
        outfilename = '/etc/logrotate.d/pogo'
        with open(outfilename, 'wt') as f:
            shutil.copyfileobj(instream, f)

    def run(self):
        _install.run(self)
        print 'Post-install stage...'
        print 'Copying config file...'
        self.install_config_file()
        self.install_logrotate_conf()

setup(name="pogo",
      version=version,
      description="Pogo is a utility for putting data generated by a HonSSH honeypot into an Elasticsearch database.",
      long_description=open("README.rst").read(),
      classifiers=[ # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Topic :: Security',
        'Topic :: Utilities',
        'Intended Audience :: System Administrators'
      ],
      keywords="HonSSH Elasticsearch Python Honeypot cybersecurity", # Separate with spaces
      author="Tony Rein",
      author_email="boing.to.elasticsearch@gmail.com",
      url="https://github.com/tonyrein/pogo.git",
      license="MIT",
      packages=find_packages(exclude=['examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      tests_require=['pytest'],
      cmdclass={'test': PyTest, 'install': install},
      # package_data files are:
      #		* pogo_schema.sql to initialize the app's "staging" database
      #		* pogo.cfg, a default configuration file
      #		* logrotate.cfg, a default control file for logrotate
      #
      package_data = {'pogo': ['data/pogo_schema.sql', 'data/pogo.cfg', 'data/logrotate.cfg']},
      install_requires=['iso8601', 'tzlocal', 'python-geoip-geolite2', 'elasticsearch'],

      # The entry_points entry results in an executable script called 'pogo'
      # in the PATH, which invokes the main() method in the 'main' module.
      entry_points={
        'console_scripts':
            ['pogo=pogo.main:main']
      }
)
