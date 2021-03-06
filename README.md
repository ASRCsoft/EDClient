### **EDClient
### **_(NASA ECHO Data Client)_

### Version 1.0.0
### July 2015
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

####With Database Option
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

### Version 1.1.0
### November 2015
Added temporal search functionality

### Version 1.2.0
### February 29, 2016

####Without Database Option
EDClient can be run to download data files through ECHO, without tracking
the downloaded files in a database.  In this case, the data is downloaded
to a separate data directory to maintain integrity of the database and
files tracked through that database.

Added 'useDB' attribute to 'echoDownload' element.  Set to either
'True' or 'False' to enable/disable DB file tracking.  Required field

Changed 'dirRoot' attribute of 'echoDownload' element to 'dbRoot'.
'dbRoot' is used if the 'useDB' element is set true.  Required and
cannot be the same as 'dataRoot'.

Added 'dataRoot' attribute to 'echoDownload' element.  The 'dataRoot'
attribute is used as a directory root for file downloads if DB 
tracking of files is disabled ('useDB' is set false). Required and
cannot be the same as 'dbRoot'.

####Usage:
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
