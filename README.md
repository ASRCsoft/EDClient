EDClient is a Python client application for querying, and optionally
downloading, data from the NASA Earth Observing System (EOS) Clearning
House (ECHO).  ECHO is a spatial and temporal metadata registry with
entries from multiple data providers.  EDClient access ECHO using REST
request specification, and receives its responses in ECHO.10 XML
format.  To utilize EDClient to query ECHO, you must have a valid
account in the NASA Reverb|ECHO online search system.  Your account
information should then be entered in the "ECHOlogin.xml" file used
by EDClient.

To create a Reverb|ECHO account, visit http://reverb.echo.nasa.gov.

EDClient requires the following Python modules:
    os, argparse, datetime, requests, math,
    lxml.etree, pycurl, MySQLdb, re

EDClient connects to a remote database server to store metadata for
successfully downloaded data files.  You must create a file called
'.my.cnf' in your HOME directory with the following format/content:

[client]
user = <your username>
password = "<your database password, including the quotes>"

EDClient keeps track of download failures and upon restart, attempts to
redownload files that failed to download previously.  The client also
keeps track of failed database transactions (inserts) and upon startup,
if any transactions are pending, attempts to reprocess them.  Any
outstanding database transactions (failures) will cause EDClient to
abort.  This was designed to maintain integrity of the metadata database.

Usage:
python EDClient.py [-h] [-o OPMODE] [-r RESULTSIZE] [-s DOWNLOADLIMIT] xmlfile

positional arguments:
  xmlfile               Your ECHO Download Request File (XML format)

optional arguments:
  -h, --help            show this help message and exit
  -o OPMODE, --opmode OPMODE
     Operation mode ('Q' for query only (default), 'D' for download)
  -r RESULTSIZE, --resultsize RESULTSIZE
     Allowable # of data files to download (Max=2000, Default=1000)
  -s DOWNLOADLIMIT, --downloadlimit DOWNLOADLIMIT
     Maximum download size in MegaBytes (Max=5120, Default=3072)

