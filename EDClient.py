"""
   Python Module: 'EDClient' (ECHO Data Client)
   Atmospheric Sciences Research Center
   Python Env   : Anaconda Python (2.7.10)

   Revision History:

        Version Modification Details
        v1.0.0, July 2015           mcb, ASRC
                Initial release
        v1.1.0, November 2015       mcb, ASRC
                Added temporal search capability
        v1.2.0, February 2016       mcb, ASRC
                Added optional DB tracking capability

"""
import os
import argparse
import datetime as dt
import requests
import math
import lxml.etree as ET
import pycurl
import MySQLdb
from lxml.etree import XMLSyntaxError
from re import sub as resub

__version__ = "1.2.0"

# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
try:
    import signal
    from signal import SIGPIPE, SIG_IGN

    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    EDClog.write("EDClient::Problem importing and resetting signals\n")
    raise SystemExit


class runManager(object):
    def __init__(self):

        self.dtStamp = dt.datetime.now()
        self.dtStamp = self.dtStamp.replace(microsecond=0)

        self.dtString = self.dtStamp.isoformat('T')
        self.dtString = self.dtString.replace("-", "_")
        self.dtString = self.dtString.replace(":", "_")

        self.logfilename = "./EDClient_" + self.dtString + ".log"

        self.setLogFH(self.logfilename)
        self.setCmdLineArgs()

    def setLogFH(self, fname):
        try:
            self.logfh = open(fname, 'w')
        except IOError:
            EDClog.write("runManager::setLogFH\n")
            EDClog.write("\t***ERROR: Could not open ECHO Data Client Log File ({})\n".format(fname))
            raise SystemExit

    def getLogFH(self):
        return (self.logfh)

    def setCmdLineArgs(self):

        parser = argparse.ArgumentParser()
        parser.add_argument("xmlfile", help="Your ECHO Download Request File (XML format)", type=str)
        parser.add_argument("-o", "--opmode", help="Operation mode ('Q' for query only (default), 'D' for download)",
                            type=str, default='Q')
        parser.add_argument("-r", "--resultsize", help="Allowable # of data files to download (Max=2000,Default=1000)",
                            type=int, default=1000)
        parser.add_argument("-s", "--downloadlimit", help="Maximum download size in MegaBytes (Max=5120,Default=3072)",
                            type=int, default=3072)
        args = parser.parse_args()

        self.XMLcfgFile = args.xmlfile
        self.opMode = args.opmode
        self.MAXdataFiles = args.resultsize
        self.dwnloadSize = args.downloadlimit

    def getopMode(self):
        return self.opMode

    def getXMLfile(self):
        return self.XMLcfgFile

    def getMaxFiles(self):
        return self.MAXdataFiles

    def getDwnLoadLimit(self):
        return self.dwnloadSize


class ECHOrequest(object):
    """
    This class will validate the ECHO Request information, and keep
    track of all collections (and granules) that are requested in
    the XML configuration file
    """

    # Create an instance of a parser object, and give it the ability
    # to remove comments from XML data
    parser = ET.XMLParser(remove_comments=True)

    def __init__(self, runMgr):
        """EDClient_2015_11_24T08_47_12.log
        :param cla: Command line arguments
        """
        self.xmlConfigFile = runMgr.getXMLfile()
        self.maxDataFiles = runMgr.getMaxFiles()
        self.dwnloadLimit = runMgr.getDwnLoadLimit()
        self.directoryRoot = ""
        self.availDiskSpaceMB = 0.0
        self.dataSetQueries = []
        self.numDatasetQueries = 0
        self.numCollections = 0
        self.havePendDwnld = False  # assume no pending downloads
        self.pdlfile = "pendingDwnld.xml"

        # Check to see if there are any pending downloads that
        # will need to be processed
        if os.access(self.pdlfile, os.F_OK):
            try:
                self.pdlFH = open(self.pdlfile, 'r')
            except IOError:
                EDClog.write("ECHOrequest::__init__\n")
                EDClog.write("\t***WARNING: Pending download file exists but\n")
                EDClog.write("\t***couldn't open file for reading {}\n".format(self.pdlfile))
            else:
                self.havePendDwnld = True

        # Container to hold dataset (collection) objects
        self.collContainer = []

        if not self.validateRequest():
            raise SystemExit

        if not self.loadDataSetQueries():
            raise SystemExit

        if not self.setDiskSpaceAvail():
            raise SystemExit

    def validateRequest(self):
        """
        Using the download request object, check to make sure the XML file can be opened,
        return False if not. Then attempt to load the XML content through the lxml etree
        parse method.  If an error occurs, return False.  If all tests pass, the 'EDR' object
        will contain the XML root contained in the 'edrRoot' variable, and will then return True.
        """
        EDClog.write("ECHOrequest::validateRequest\n")
        try:
            self.xmlFileObj = open(self.xmlConfigFile, 'r')
        except IOError:
            EDClog.write("\tCould not open ECHO Download Query File: " + self.xmlConfigFile)
            return False

        try:
            self.edrTree = ET.parse(self.xmlFileObj, self.parser)  # Use XML 'parser' define as class variable
        except ET.ParseError:
            EDClog.write(
                "\tCould not parse download request file (" + self.xmlConfigFile + ") please check XML syntax\n")
            return False

        # Now assign the XML root to 'edrRoot' and check that the root element
        # has children
        self.edrRoot = self.edrTree.getroot()
        if (len(self.edrRoot) == 0):
            EDClog.write("\tDownload XML request file has no dataset specifications\n")
            return False

        # Get DB tracking flag from XML request file, exit if missing
        self.dbFlag = self.edrRoot.get('useDB', default="")
        if (self.dbFlag != 'True' and self.dbFlag != 'False'):
            EDClog.write("\tuseDB flag not set to True or False, check\n")
            return False

        if self.dbFlag == 'True':
            self.directoryRoot = self.edrRoot.get('dbRoot', default="")
            if len(self.directoryRoot) == 0:
                EDClog.write("\tMissing DB directory root in XML request file\n")
                return False
        else:
            self.directoryRoot = self.edrRoot.get('dataRoot', default="")
            if len(self.directoryRoot) == 0:
                EDClog.write("\tMissing DATA directory root in XML request file\n")
                return False

        # Check to make sure that the directory root exists, and is
        # writeable by the user running the script
        if (not os.access(self.directoryRoot, os.F_OK)):
            EDClog.write("\tDirectory root path in XML request file does not exist\n")
            return False
        if (not os.access(self.directoryRoot, os.W_OK)):
            EDClog.write("\tYou do not have permission to write to " + self.directoryRoot + "\n")
            return False

        # Check specified number of data files to download.
        if (self.maxDataFiles < 1 or self.maxDataFiles > 2000):
            EDClog.write("\tInvalid result set size (" + str(self.maxDataFiles) + "), should be >= 1 and <= 2000\n")
            return False

        # Check download size (MegaBytes).  Current maximum is 5GB (5120MB)
        if (self.dwnloadLimit <= 0 or self.dwnloadLimit > 5120):
            EDClog.write("\tInvalid download size (" + str(self.dwnloadLimit) + "), should be > 0 and <= 5120\n")
            return False

        # Set available disk space using directory root.
        if not self.setDiskSpaceAvail():
            EDClog.write("\tCouldn't determine available disk space\n")
            return False

        EDClog.write("\tRequest valid, continuing\n")
        return True

    def loadDataSetQueries(self):
        """
        Method: loadDataSetQueries
        Intent: Build a list of dataset download request queries.  This method
                validates the XML download request input on the fly and returns
                False if problems are found with the input.  If no problems are
                found, the "EDR" object will contain a new list of the dataset
                query objects, and will return True.
        """
        bb = {}  # spatial bounding box dictionary
        vinfo = ""
        sdatetime = ""
        edatetime = ""
        temporal_start_day = ""
        temporal_end_day = ""

        EDClog.write("ECHOrequest::loadDataSetQueries\n")
        for dataset in self.edrRoot:
            vFlag = False  # dataset criteria flags for version,
            bbFlag = False  # boundingbox,
            tFlag = False
            shortName = dataset.get("shortname")  # get shortname attribute of dataset
            for criteria in dataset:
                critname = criteria.tag
                if (critname == "boundingbox"):
                    if ((criteria.get("w") == None) or (criteria.get("s") == None) or
                            (criteria.get("e") == None) or (criteria.get("n") == None)):
                        EDClog.write("\tInvalid bounding box attribute in XML input, (valid are 'w','s','e','n')")
                        return False
                    else:
                        bb['w'] = criteria.get('w')
                        bb['s'] = criteria.get('s')
                        bb['e'] = criteria.get('e')
                        bb['n'] = criteria.get('n')
                        bbFlag = True

                elif (critname == "version"):
                    vinfo = criteria.get("v")
                    vFlag = True

                elif (critname == "temporal"):
                    tFlag = True
                    temporalSearchType = criteria.get('type', default="")
                    if len(temporalSearchType) == 0:
                        EDClog.write(
                            "\tMissing temporal search type in XML request file, valid are ('static', 'recurring')")
                        return False

                    if (temporalSearchType == "static"):
                        stFlag = False
                        etFlag = False
                        for tcrit in criteria:
                            if (tcrit.tag == "startdatetime"):
                                sdatetime = tcrit.get("dtstr", default="")
                                stFlag = True
                            if (tcrit.tag == "enddatetime"):
                                edatetime = tcrit.get("dtstr", default="")
                                etFlag = True

                        if (not (stFlag and etFlag)):
                            EDClog.write("\tMissing elements in static temporal search criteria")
                            return False

                    elif (temporalSearchType == "recurring"):
                        yearFlag = False
                        startFlag = False
                        endFlag = False
                        for tcrit in criteria:
                            if (tcrit.tag == "year"):
                                yr_start = tcrit.get("yr_start", default="")
                                yr_end = tcrit.get("yr_end", default="")

                                if (len(yr_start) != 0 and len(yr_end) != 0):
                                    if (int(yr_start) > int(yr_end)):
                                        EDClog.write("\tStart year past end year in temporal recurring search criteria")
                                        return False
                                    yearFlag = True

                            if (tcrit.tag == "start"):
                                mon_start = tcrit.get("mon_start", default="")
                                day_start = tcrit.get("day_start", default="")
                                tim_start = tcrit.get("tim_start", default="")

                                if (len(mon_start) != 0 and len(day_start) != 0 and len(tim_start) != 0):
                                    if (int(mon_start) < 1 or int(mon_start) > 12):
                                        EDClog.write("\tInvalid start month in temporal recurring search criteria");
                                        return False
                                    if (int(day_start) < 1 or int(day_start) > 31):
                                        EDClog.write("\tInvalid start day in temporal recurring search criteria")
                                        return False
                                    startFlag = True

                            if (tcrit.tag == "end"):
                                mon_end = tcrit.get("mon_end", default="")
                                day_end = tcrit.get("day_end", default="")
                                tim_end = tcrit.get("tim_end", default="")

                                if (len(mon_end) != 0 and len(day_end) != 0 and len(tim_end) != 0):
                                    if (int(mon_end) < 1 or int(mon_end) > 12):
                                        EDClog.write("\tInvalid end month in temporal recurring search criteria")
                                        return False
                                    if (int(day_end) < 1 or int(day_end) > 31):
                                        EDClog.write("\tInvalid end day in temporal recurring search criteria")
                                        return False
                                    endFlag = True

                        if (int(mon_start) > int(mon_end)):
                            EDClog.write("\tMonth start past month end in temporal recurring search criteria")
                            return False

                        if (not (yearFlag and startFlag and endFlag)):
                            EDClog.write("\tInvalid or missing recurring temporal search criteria")
                            return False

                        # Now, since this is a request for a temporal recurring search we need to
                        # figure out the starting and ending year days based on the use request.
                        # Bit of a trick here.  The ECHO REST specification for a temporal search
                        # (https://api.echo.nasa.gov/catalog-rest/catalog-docs/index.html) is as
                        # follows:
                        #
                        # At least one of a temporal_start or temporal_end datetime string which
                        # is then followed by a temporal_start_day and/or temporal_end_day (day
                        # of year).  But because a year might be a leap year we need to make
                        # sure the temporal_start_day and temporal_end_day encapsulate the recurring
                        # date range the user requested.  To do this, we calculate the day of year
                        # for the starting month/day for all years and take the minimum and use it
                        # as the temporal_start_day, and then calculate the day of year for the
                        # ending month/day for all years and take the maximum and use it as the
                        # temporal_end_day.

                        temporal_start_list = []
                        temporal_end_list = []
                        intMonStart = int(mon_start)
                        intDayStart = int(day_start)
                        intMonEnd = int(mon_end)
                        intDayEnd = int(day_end)
                        for yr in range(int(yr_start), int(yr_end) + 1):
                            sDate = dt.date(yr, intMonStart, intDayStart)
                            sdoy = sDate.toordinal() - dt.date(yr, 1, 1).toordinal() + 1
                            temporal_start_list.append(sdoy)
                            eDate = dt.date(yr, intMonEnd, intDayEnd)
                            edoy = eDate.toordinal() - dt.date(yr, 1, 1).toordinal() + 1
                            temporal_end_list.append(edoy)

                        temporal_start_day = min(temporal_start_list)
                        temporal_end_day = max(temporal_end_list)

                        # Now build the 'sdatetime' and 'edatetime' strings for a recurring
                        # temporal search.  Note that the month/day components are used to
                        # create bounds to encapsulate the user desired starting and ending
                        # day of year values.
                        sdatetime = yr_start + '-01-01T' + tim_start
                        edatetime = yr_end + '-12-31T' + tim_end

                    else:
                        EDClog.write("\tInvalid temporal search type, (valid are 'static', 'recurring')")
                        return False

                else:
                    EDClog.write(
                        "\tInvalid dataset search criteria (" + critname + ") specified in XML download request file")
                    return False

            if (not (bbFlag and vFlag and tFlag)):
                EDClog.write("\tMissing bounding box, version, or temporal criteria in XML input")
                return False

            self.numDatasetQueries += 1
            dsQuery = ECHOdsQuery(shortName, vinfo, bb, temporalSearchType,
                                  sdatetime, edatetime, temporal_start_day, temporal_end_day)
            self.dataSetQueries.append(dsQuery)

        EDClog.write("\tSuccessful.\n")
        return True

    def setDiskSpaceAvail(self):
        """
        Determine how much disk space is available using directory root
        provided by the user.  Note that when the ECHO download request
        object was created and validated, the directory root was checked
        for existence and writeable.
        """

        try:
            st = os.statvfs(self.directoryRoot)
        except OSError:
            EDClog.write("ECHOrequest::setDiskSpaceAvail\n")
            EDClog.write("\t***ERROR: Couldn't run statvfs on directory root\n")
            return False
        else:
            self.availDiskSpaceMB = st.f_bavail * st.f_frsize / math.pow(1024, 2)
            return True

    def getDiskSpaceAvail(self):
        return self.availDiskSpaceMB

    def getDirRoot(self):
        return self.directoryRoot

    def getReqData(self, eClient):
        """
        Retrieve all requested collection and granule information from the ECHO
        web service using the 'eClient' object interface
        """
        # Keep track of a 'collection counter' (numCollections) since we can't
        # rely on every collection/dataset request to be successful (return
        # 1, and only 1, collection)
        # self.numCollections set to 0 (zero) in __init__

        for i in range(self.numDatasetQueries):
            queryStr = self.dataSetQueries[i].getDSqueryStr()
            collElemRoot = eClient.makeDatasetQuery(queryStr, "echo10")
            if ((len(collElemRoot) == 0) or (len(collElemRoot) > 1)):
                EDClog.write("ECHOrequest::getReqData\n\t***IGNORING REQUEST\n")
                EDClog.write("\tYour dataset query: " + queryStr +
                             " returned " + str(len(collElemRoot)) +
                             " results, should be 1, check your query criteria\n")
                EDClog.write(ET.tostring(collElemRoot, pretty_print=True))
            else:
                # If we reach this block we are confident there is 1, and only 1 result
                # from the collection query.  Create a new collection object with a
                # collection element root reference.

                result = collElemRoot.find('result')
                collID = result.get("echo_dataset_id")
                try:
                    shortName = result.find('Collection').find('ShortName').text
                except AttributeError:
                    shortName = "NoShortName"

                try:
                    archCenter = result.find('Collection').find('ArchiveCenter').text
                except AttributeError:
                    archCenter = "NoArchiveCenter"

                try:
                    collDesc = result.find('Collection').find('Description').text
                except AttributeError:
                    collDesc = "NoDescription"

                try:
                    begDateTime = result.find('Collection').find('Temporal').find('RangeDateTime').find(
                        'BeginningDateTime').text
                except AttributeError:
                    begDateTime = "null"
                else:
                    # Remove trailing 'Z' from datetime value (for DB insert)
                    begDateTime = resub('[Z]', '', begDateTime)

                try:
                    endDateTime = result.find('Collection').find('Temporal').find('RangeDateTime').find(
                        'EndingDateTime').text
                except AttributeError:
                    endDateTime = "null"
                else:
                    # Remove trailing 'Z' from datetime value
                    endDateTime = resub('[Z]', '', endDateTime)

                # Locate the additional attributes and extract Digital Object Identifier,
                # if one exists.
                doiname = "NoDOI"
                doiauth = "NoDOIauth"
                for attr in collElemRoot.iter('AdditionalAttribute'):
                    attrname = attr.find('Name').text
                    if (attrname == 'identifier_product_doi'):
                        try:
                            doiname = attr.find('Value').text
                        except:
                            doiname = "NoDOI"
                    if (attrname == 'identifier_product_doi_authority'):
                        try:
                            doiauth = attr.find('Value').text
                        except:
                            doiauth = "NoDOIauth"

                doi = doiauth + '/' + doiname

                self.collContainer.append(ECHOcollection(collID, shortName, archCenter,
                                                         collDesc, begDateTime, endDateTime,
                                                         doi))
                # EDClog.write(ET.tostring(collElemRoot, pretty_print=True))

                granElemRoot = eClient.makeGranuleQuery(
                    self.collContainer[self.numCollections].collID,
                    self.dataSetQueries[i].getSpatialstr(),
                    self.dataSetQueries[i].getTemporalStr(),
                    self.maxDataFiles, "echo10")
                # EDClog.write(ET.tostring(granElemRoot, pretty_print=True))

                # remember, 'self' is the ECHOrequest object, 'collContainer' stores
                # the collections objects, which have a method 'getGranules'
                self.collContainer[self.numCollections].getGranules(granElemRoot)

                self.numCollections += 1

    def getHavePendDwnld(self):
        return self.havePendDwnld

    def getDBflag(self):
        return self.dbFlag

    def loadPendDwnld(self):

        try:
            pendTree = ET.parse(self.pdlFH, self.parser)  # Use XML 'parser' define as class variable
        except ET.ParseError:
            EDClog.write("ECHOrequest::loadPendDwnld\n")
            EDClog.write("\t***INTERNAL ERROR***\n")
            EDClog.write("\tCould not parse pending download file (" + self.pdlfile + ")\n")
            raise SystemExit

        self.pdlFH.close()
        pendRoot = pendTree.getroot()
        for c in pendRoot.findall('collection'):
            collID = c.find('collID').text
            collIndex = self.inCollections(collID)
            if collIndex == -1:
                # This pending download collection is NOT in the current
                # collection container, so add it as a new collection object
                shortName = c.find('shortName').text
                archCtr = c.find('archCenter').text
                collDesc = c.find('collDesc').text
                CbegDateTime = c.find('begDateTime').text
                CendDateTime = c.find('endDateTime').text
                doi = c.find('doi').text

                self.collContainer.append(ECHOcollection(collID, shortName, archCtr, collDesc,
                                                         CbegDateTime, CendDateTime, doi))
                self.numCollections += 1

                collIndex = self.numCollections - 1  # 0 based index
            else:
                EDClog.write("ECHOrequest::loadPendDwnld\n")
                EDClog.write("\tPending collection {} already in collection container\n".format(collID))

            granRoot = c.find('granules')
            for g in granRoot:
                polyPoints = []
                accessURLs = []

                granID = g.find('granID').text
                numDwnldTrys = int(g.find('dwnldtrys').text) + 1
                granuleUR = g.find('granuleUR').text
                sizeMB = float(g.find('sizeMB').text)
                GbegDateTime = g.find('begDateTime').text
                GendDateTime = g.find('endDateTime').text

                spatial = g.find('spatial')
                ppts = spatial.find('polyPoints')
                if ppts is None:
                    hasPolyPoints = 0
                    w_bound = float(spatial.find('w_bound').text)
                    s_bound = float(spatial.find('s_bound').text)
                    e_bound = float(spatial.find('e_bound').text)
                    n_bound = float(spatial.find('n_bound').text)
                else:
                    hasPolyPoints = 1
                    w_bound = -180.0
                    s_bound = -90.0
                    e_bound = 180.0
                    n_bound = 90.0
                    for pp in ppts:
                        lat = float(pp.find('latitude').text)
                        lon = float(pp.find('longitude').text)
                        polyPoints.append((lat, lon))

                accessURLs.append((g.find('accessURL').text, "NoMimeType"))
                localFileName = g.find('localFileName').text

                granIndex = self.inGranules(collID, granID)
                if granIndex == -1:
                    # Granule not already in the granule container, add it
                    self.collContainer[collIndex].granContainer.append(
                        ECHOgranule(granID, granuleUR, sizeMB,
                                    GbegDateTime, GendDateTime, hasPolyPoints, polyPoints,
                                    w_bound, s_bound, e_bound, n_bound,
                                    accessURLs, localFileName, numDwnldTrys + 1))
                else:
                    EDClog.write("ECHOrequest::loadPendDwnld\n")
                    EDClog.write("\tPending granule {} already in granule container\n".format(granID))
                    EDClog.write("\tIncrementing # of download trys\n")
                    self.collContainer[collIndex].granContainer[granIndex].setnumtrys(numDwnldTrys + 1)

    def inCollections(self, cid):
        """
        :param cid: A Collection ID
        """
        index = 0
        for c in self.collContainer:
            if c.collID == cid:
                return index
            index += 1
        return -1

    def inGranules(self, cid, gid):
        """
        :param cid: The Collection Object ID
        :param gid: Granule ID to search for in the collection
        """
        index = 0
        for coll in self.collContainer:
            if coll.collID == cid:
                for g in coll.granContainer:
                    if g.egid == gid:
                        return index
                    index += 1
        return -1

    def zapPending(self):
        """
        Close and remove the pending download file
        """
        self.pdlFH.close()
        try:
            os.remove(self.pdlfile)
        except OSError:
            EDClog.write("ECHOrequest::zapPending\n")
            EDClog.write("\t****SEVERE: Couldn't remove old pending download file\n".format(self.pdlfile))
            raise SystemExit

    def savePending(self):
        """
        If any downloads failed (status codes -1 (file transfer failed) or -2
        (directory make fail), save them as "pending" downloads
        """
        haveNewPending = False
        for c in self.collContainer:
            if c.getFailedStatus():
                haveNewPending = True

        if haveNewPending:
            xmlroot = ET.Element("data")
            for c in self.collContainer:
                if c.getFailedStatus():
                    # At least one granule in this collection encountered a download
                    # failure (-1) or granule directory make failure (-2), or the
                    # whole collection failed (-2) if the collection directory make failed
                    coll = ET.SubElement(xmlroot, "collection")
                    coll_id = ET.SubElement(coll, 'collID')
                    coll_id.text = c.getid()
                    coll_sn = ET.SubElement(coll, "shortName")
                    coll_sn.text = c.getshortname()
                    coll_ac = ET.SubElement(coll, "archCenter")
                    coll_ac.text = c.getarchcenter()
                    coll_desc = ET.SubElement(coll, "collDesc")
                    coll_desc.text = c.getdesc()
                    coll_bdt = ET.SubElement(coll, 'begDateTime')
                    coll_bdt.text = c.getbegdate()
                    coll_edt = ET.SubElement(coll, 'endDateTime')
                    coll_edt.text = c.getenddate()
                    coll_doi = ET.SubElement(coll, 'doi')
                    coll_doi.text = c.getdoi()

                    grans = ET.SubElement(coll, "granules")
                    for g in c.granContainer:
                        if g.getDownloadStatus() < 0:
                            gran = ET.SubElement(grans, "granule")
                            gran_id = ET.SubElement(gran, "granID")
                            gran_id.text = g.getgranuleid()
                            gran_trys = ET.SubElement(gran, "dwnldtrys")
                            gran_trys.text = str(g.getnumtrys())
                            gran_stat = ET.SubElement(gran, "dwnldstat")
                            gran_stat.text = str(g.getDownloadStatus())
                            gran_ur = ET.SubElement(gran, "granuleUR")
                            gran_ur.text = g.getgranuleur()
                            gran_size = ET.SubElement(gran, "sizeMB")
                            gran_size.text = str(g.getGranuleSizeMB())
                            gran_bdt = ET.SubElement(gran, 'begDateTime')
                            gran_bdt.text = g.getgranulebd()
                            gran_edt = ET.SubElement(gran, 'endDateTime')
                            gran_edt.text = g.getgranuleed()
                            gran_spatial = ET.SubElement(gran, 'spatial')
                            if g.getPolyPointStatus():
                                gran_ppts = ET.SubElement(gran_spatial, 'polyPoints')
                                ppts = g.getPolyPoints()
                                for lat, lon in ppts:
                                    pp = ET.SubElement(gran_ppts, 'polyPoint')
                                    pp_lat = ET.SubElement(pp, 'latitude')
                                    pp_lat.text = str(lat)
                                    pp_lon = ET.SubElement(pp, 'longitude')
                                    pp_lon.text = str(lon)
                            else:
                                wb = ET.SubElement(gran_spatial, 'w_bound')
                                wb.text = str(g.getgranulewb())
                                sb = ET.SubElement(gran_spatial, 's_bound')
                                sb.text = str(g.getgranulesb())
                                eb = ET.SubElement(gran_spatial, 'e_bound')
                                eb.text = str(g.getgranuleeb())
                                nb = ET.SubElement(gran_spatial, 'n_bound')
                                nb.text = str(g.getgranulenb())
                            gran_accurl = ET.SubElement(gran, 'accessURL')
                            granuleURL, mimeType = g.accessURLs[0]
                            gran_accurl.text = granuleURL
                            gran_lf = ET.SubElement(gran, 'localFileName')
                            gran_lf.text = g.getLocalFileName()

            # Things get a little weird here.  If there was a pending download
            # file, it was read, and processed.  Now we have "new" pending
            # downloads and must save them, in XML format.  We must first try
            # to remove and reopen in write mode the old file.  If we fail to
            # remove and reopen the file, we'll write the pending data to the
            # log file and, in earnest, let the user know that manual
            # intervention is required after the run

            EDClog.write("ECHOrequest::savePending\n")
            EDClog.write("\tAttempting to save pending download information...\n")
            pendingFileOk = True

            try:
                self.pdlFH = open(self.pdlfile, 'w')
            except IOError:
                EDClog.write(
                    "\t****SEVERE: Couldn't reopen pending download file for writing {}\n".format(self.pdlfile))
                pendingFileOk = False
            else:
                try:
                    self.pdlFH.write(ET.tostring(xmlroot, pretty_print=True))
                except IOError:
                    EDClog.write("\t****SEVERE: Failed to write to pending download file {}\n".format(self.pdlfile))
                    pendingFileOk = False

            if not pendingFileOk:
                EDClog.write("\t****SEVERE: Writing pending download data to log file instead\n")
                EDClog.write("\t****SEVERE: You must manually delete old pending download file\n")
                EDClog.write("\t****SEVERE: and populate new file with XML data that follows.\n")
                EDClog.write(ET.tostring(xmlroot, pretty_print=True))
            else:
                EDClog.write("\tSuccessful.\n")

class ECHOdsQuery(object):
    def __init__(self,
                 sname,  # ECHO dataset short name
                 ver,  # ECHO dataset version
                 bbox,  # ECHO dataset bounding box dictionary
                 tst,  # temporal search type (static or recurring)
                 sdt,  # ECHO dataset start date/time
                 edt,  # ECHO dataset end date/time
                 tsd, ted):  # temporal start and end day values

        self.snStr = "?shortName=" + sname
        self.vStr = "&version=" + ver
        self.bbStr = "&bounding_box=" + bbox['w'] + ',' + bbox['s'] + ',' + bbox['e'] + ',' + bbox['n']

        if (tst == "static"):
            self.tStr = "&temporal=" + sdt + ',' + edt
        else:
            # Recurring temporal search
            self.tStr = "&temporal=" + sdt + ',' + edt + ',' + str(tsd) + ',' + str(ted)

        self.w_bound = bbox['w']
        self.s_bound = bbox['s']  # spatial elements to be transferred
        self.e_bound = bbox['e']  # to corresponding collection object
        self.n_bound = bbox['n']

    def getDSqueryStr(self):
        return (self.snStr + self.vStr + self.bbStr + self.tStr)

    def getSpatialstr(self):
        return self.bbStr

    def getTemporalStr(self):
        return self.tStr


class ECHOclient(object):
    """
    Create an ECHO web service client class.  When the client
    object is created a login attempt is made to the web service.
    This class will be used to handle all communication with
    the ECHO web service
    """

    echoURL = 'https://api.echo.nasa.gov'
    echoRestURL = echoURL + "/echo-rest"
    echoLoginURL = echoRestURL + "/tokens"
    echoProvURL = echoRestURL + "/providers"

    echoCatalogURL = echoURL + "/catalog-rest/echo_catalog"
    echoCollectionURL = echoCatalogURL + "/datasets"
    echoGranuleURL = echoCatalogURL + "/granules"

    def __init__(self, maxfiles):
        self.maxFiles = maxfiles
        self.login()

    def login(self):
        reqHeaders = {'Content-type': 'application/xml'}
        Xmltree = ET.parse('ECHOlogin.xml')
        reqDataXml = ET.tostring(Xmltree)

        EDClog.write("ECHOclient::login\n")
        try:
            wsResponse = requests.post(self.echoLoginURL, data=reqDataXml, headers=reqHeaders)
        except requests.exceptions.ConnectionError:
            EDClog.write("\t***FATAL ERROR: Couldn't make connection to ECHO web service\n")
            self.ECHO_TOKEN = "Failed"
        else:
            # ECHO returns its response in XML format.  Use the ElementTree
            # method 'fromstring' to encode the XML at 'xmlTreeRoot' (Note:
            # <response>.content is the content in bytes NOT unicode which lxml
            # ElementTree expects. Create an instance of an XML tree from
            # 'ElementTree' class (not currently used). Inside the XML
            # document, the ECHO token id is exposed as the 'id' element

            try:
                tokenRespRoot = ET.fromstring(wsResponse.content)
            except XMLSyntaxError:
                EDClog.write("\t***FATAL ERROR: XML Syntax Error on login response\n")
                self.ECHO_TOKEN = "Failed"
            else:
                self.ECHO_TOKEN = tokenRespRoot.find('id').text

        if self.ECHO_TOKEN == "Failed":
            raise SystemExit
        else:
            EDClog.write("\tSuccessful.\n")

    def getProviders(self):
        """
        Get a list of data providers from ECHO and store provider ID (key)
        and organization name (value) in a dictionary
        """
        self.echoProviders = {}
        reqHeaders = {'Content-type': 'application/xml',
                      'Echo-Token': self.ECHO_TOKEN}
        provRes = requests.get(self.echoProvURL, headers=reqHeaders)
        try:
            provRoot = ET.fromstring(provRes.content)
        except XMLSyntaxError:
            EDClog.write(">>>>Error: ECHOclient.getProviders : XML Syntax Error on provider response")
        else:
            # Dump XML tree for debugging....
            # print(ET.tostring(provRoot))
            for prov in provRoot.findall('provider'):
                p_id = prov.find('provider_id').text
                p_org = prov.find('organization_name').text
                self.echoProviders[p_id] = p_org

    def listProviders(self):
        pkeys = sorted(self.echoProviders.keys())
        n = 1
        for kw in pkeys:
            EDClog.write("{} : {} : {}".format(n, kw, self.echoProviders[kw]))
            n += 1

    def makeDatasetQuery(self, dsQueryStr, responseFormat):
        queryURL = self.echoCollectionURL + '.' + responseFormat + dsQueryStr
        reqHeaders = {'Content-type': 'application/xml',
                      'Echo-Token': self.ECHO_TOKEN}

        queryResponse = requests.get(queryURL, headers=reqHeaders)
        try:
            respRoot = ET.fromstring(queryResponse.content)
        except XMLSyntaxError:
            EDClog.write(">>>>Error: ECHOclient.makeDatasetQuery : XML Syntax Error on dataset query response")
            # In the event of a failure getting the full XML response, create an empty response element root
            # to pass back to the caller
            respRoot = ET.Element("results")

        return respRoot

    def makeGranuleQuery(self, echoDSid, boundingBoxStr, temporalStr, maxFiles, responseFormat):
        queryURL = self.echoGranuleURL + '.' + responseFormat
        queryURL += "?echo_collection_id[]=" + echoDSid + boundingBoxStr + temporalStr
        queryURL += "&page_size=" + str(self.maxFiles)
        reqHeaders = {'Content-type': 'application/xml',
                      'Echo-Token': self.ECHO_TOKEN}

        # GET request.  Note the values stored in the 'headers' dictionary
        # are stored as strings!  This got me the first time when trying to
        # use 'hitsReceived'
        queryResponse = requests.get(queryURL, headers=reqHeaders)
        hitsReceived = int(queryResponse.headers['echo-hits'])

        if (hitsReceived > maxFiles):
            EDClog.write("ECHOclient::makeGranuleQuery\n")
            EDClog.write("\tYour query (" + queryURL + ")")
            EDClog.write("\tgot " + str(
                hitsReceived) + " hits, increase 'resultsize' via command line, or refine download request\n")
            EDClog.write("\t<Note: forcing 0 collection results because of the volume of hits>\n")
            respRoot = ET.Element("results")
        else:
            try:
                respRoot = ET.fromstring(queryResponse.content)
            except XMLSyntaxError:
                EDClog.write("ECHOclient::makeGranuleQuery\n")
                EDClog.write("\t****Error: XML Syntax Error on granule query response")
                # In the event of a failure getting the full XML response, create an empty response element root
                # to pass back to the caller
                respRoot = ET.Element("results")

        return respRoot

    def logout(self):

        tokenURL = self.echoLoginURL + '/' + self.ECHO_TOKEN
        logoutResp = requests.delete(tokenURL)
        EDClog.write("ECHOclient::logout\n")
        EDClog.write("\t***ECHO logout (status: " + str(logoutResp.status_code) + ")\n")


class ECHOcollection(object):
    """
    Collection and Dataset are interchangeable names to the same type of object
    """

    def __init__(self,
                 cid, csn, cac, ccd, bdt, edt, doi):

        self.collID = cid
        self.shortName = csn
        self.archCenter = cac
        self.collDesc = ccd
        self.begDateTime = bdt
        self.endDateTime = edt
        self.doi = doi
        self.granContainer = []
        self.numGranules = 0
        self.haveFailedDwnlds = False
        self.dbInsertFailed = False

    def showCollectionInfo(self):
        EDClog.write("\n#######################\n")
        EDClog.write("##Collection id       : {}\n".format(self.collID))
        EDClog.write("##Collection shortname: {}\n".format(self.shortName))
        EDClog.write("##Collection Arch Ctr : {}\n".format(self.archCenter))
        EDClog.write("##Collection Desc     : {}\n".format(self.collDesc))
        EDClog.write("##Begin Date/Time     : {}\n".format(self.begDateTime))
        EDClog.write("##End Date/Time       : {}\n".format(self.endDateTime))
        EDClog.write("##Data Obj. Identifier: {}\n".format(self.doi))

    def getGranules(self,
                    geRoot):
        self.granRoot = geRoot
        self.numGranules = len(self.granRoot)
        if (self.numGranules > 0):
            for granule in self.granRoot.findall('result'):
                polyPoints = []  # list of polypoint tuples
                accessURLs = []
                w_bound = -180.0
                e_bound = 180.0
                s_bound = -90.0
                n_bound = 90.0

                egid = granule.get("echo_granule_id")

                spatialGeometry = granule.find('Granule').find('Spatial').find(
                    'HorizontalSpatialDomain').find('Geometry')

                if spatialGeometry is None:
                    # No spatial geometry information, which means it's orbit
                    # information.  For now, just store global extent in the
                    # boundary fields (default initialization above)
                    HasPolyPoints = 0
                else:
                    boundingRect = spatialGeometry.find('BoundingRectangle')
                    if boundingRect is None:
                        boundaryPoints = spatialGeometry.find('GPolygon').find('Boundary')
                        if boundaryPoints is None:
                            EDClog.write("ECHOcollection::getGranules\n")
                            EDClog.write(
                                "\t***FATAL ERROR: Missing bounding rectangle or Polygon points for granule {}\n".format(
                                    egid))
                            raise SystemExit
                        else:
                            HasPolyPoints = 1
                            for point in boundaryPoints.findall('Point'):
                                pt_longitude = float(point.find('PointLongitude').text)
                                pt_latitude = float(point.find('PointLatitude').text)
                                polyPoints.append((pt_latitude, pt_longitude))
                    else:
                        HasPolyPoints = 0
                        w_bound = float(boundingRect.find('WestBoundingCoordinate').text)
                        s_bound = float(boundingRect.find('SouthBoundingCoordinate').text)
                        e_bound = float(boundingRect.find('EastBoundingCoordinate').text)
                        n_bound = float(boundingRect.find('NorthBoundingCoordinate').text)

                try:
                    granuleUR = granule.find('Granule').find('GranuleUR').text
                except AttributeError:
                    granuleUR = "NoGranuleUR"

                try:
                    granuleSizeMB = float(granule.find('Granule').find('DataGranule').find('SizeMBDataGranule').text)
                except AttributeError:
                    granuleSizeMB = -0.0

                try:
                    begDateTime = granule.find('Granule').find('Temporal').find('RangeDateTime').find(
                        'BeginningDateTime').text
                except AttributeError:
                    begDateTime = "null"
                    EDClog.write("ECHOcollection:getGranules\n")
                    EDClog.write("\tWARNING: No Beginning DateTime for Granule {}".format(egid))
                else:
                    # Remove trailing 'Z' from datetime object
                    begDateTime = resub('[Z]', '', begDateTime)

                try:
                    endDateTime = granule.find('Granule').find('Temporal').find('RangeDateTime').find(
                        'EndingDateTime').text
                except AttributeError:
                    endDateTime = "null"
                    EDClog.write("ECHOcollection:getGranules\n")
                    EDClog.write("\tWARNING: No Ending DateTime for Granule {}".format(egid))
                else:
                    # Remove trailing 'Z' from datetime object
                    endDateTime = resub('[Z]', '', endDateTime)

                for child in granule.find('Granule').find('OnlineAccessURLs').iterchildren():
                    try:
                        # Find the URL part of the child, if it exists
                        thisurl = child.find('URL').text
                    except AttributeError:
                        thisurl = "Unknown URL"

                    try:
                        # Find the Mimetype part of the child, if it exists
                        thismt = child.find('MimeType').text
                    except:
                        thismt = "Unknown MimeType"

                    t = (thisurl, thismt)
                    accessURLs.append(t)

                # Create a new granule object and store in 'granContainer'.  The
                # last parameter is the number of download trys.  This was added
                # to keep track of pending downloads that keep failing
                self.granContainer.append(
                    ECHOgranule(egid, granuleUR, granuleSizeMB,
                                begDateTime, endDateTime, HasPolyPoints, polyPoints,
                                w_bound, s_bound, e_bound, n_bound, accessURLs, "", 1))

    def getNumGranules(self):
        return len(self.granContainer)

    def showGranuleInfo(self):
        for g in self.granContainer:
            g.printGranuleInfo()

    def getCollSizeMB(self):
        cSizeMB = 0.0
        for g in self.granContainer:
            cSizeMB += g.getGranuleSizeMB()
        return cSizeMB

    def getid(self):
        return self.collID

    def getshortname(self):
        return self.shortName

    def getarchcenter(self):
        return self.archCenter

    def getdesc(self):
        return self.collDesc

    def getbegdate(self):
        return self.begDateTime

    def getenddate(self):
        return self.endDateTime

    def getdoi(self):
        return self.doi

    def getFailedStatus(self):
        return self.haveFailedDwnlds

    def setFailedStatus(self, statFlag):
        self.haveFailedDwnlds = statFlag

    def setInsertFailed(self, statFlag):
        self.dbInsertFailed = statFlag

    def getInsertFailed(self):
        return self.dbInsertFailed


class ECHOgranule(object):
    def __init__(self,
                 gid, ur, sizeMB, bdt, edt, ppflag, ppts,
                 wbnd, sbnd, ebnd, nbnd, aurls, lfn, dltrys):

        # 'accessURLs' is a  list of tuples of the form:
        # (URL, MimeType)

        self.egid = gid
        self.granuleUR = ur
        self.granuleSizeMB = sizeMB
        self.begDateTime = bdt
        self.endDateTime = edt
        self.HasPolyPoints = ppflag
        self.polyPoints = []  # list of polypoint objects, if any 'ppts' tuples
        self.w_bound = wbnd
        self.s_bound = sbnd
        self.e_bound = ebnd
        self.n_bound = nbnd
        self.localFileName = lfn
        self.accessURLs = aurls
        self.numDloadTrys = dltrys
        self.dbInsertFailed = False

        for lat, lon in ppts:
            self.polyPoints.append(ECHOpolypoint(lat, lon))

        # Little explanation of 'downloadStatus' needed here.  This
        # is the process...if a granule is returned from the ECHO system,
        # a granule object is created for it and it is stored in the
        # granule container for its parent collection.  Then if download
        # operation mode (-o D) was specified, the granule is added to the
        # download queue for an attempted download.  Before download, the
        # database is checked to see if it is already on disk (previously
        # downloaded successfully), if it is, the download is skipped. If
        # it is not in the database, the collection and granule holding
        # directories are built and the download is attempted. So here
        # are the possible codes/values for 'downloadStatus':
        #
        #  0==not downloaded (because the granule was already in the DB)
        #  1==downloaded ok (a download was successful)
        # -1==download failed (a download for this granule failed)
        # -2==collection or granule directory make failed

        self.downloadStatus = 0

    def printGranuleInfo(self):
        EDClog.write("\n#######################\n")
        EDClog.write("##Granule id          : %s\n" % (self.egid))
        EDClog.write("##Granule UR          : %s\n" % (self.granuleUR))
        EDClog.write("##Granule size (MB)   : %f\n" % (self.granuleSizeMB))
        EDClog.write("##Begin Date/Time     : %s\n" % (self.begDateTime))
        EDClog.write("##End Date/Time       : %s\n" % (self.endDateTime))
        if self.HasPolyPoints:
            EDClog.write("##Granule has {0:d} bounding polygon points:\n".format(len(self.polyPoints)))
            for pp in self.polyPoints:
                lat = pp.getLatitude()
                lon = pp.getLongitude()
                EDClog.write("##  ({0:11.6f},{1:11.6f})\n".format(lat, lon))
        else:
            EDClog.write("##West Boundary       : %s\n" % str(self.w_bound))
            EDClog.write("##East Boundary       : %s\n" % str(self.e_bound))
            EDClog.write("##South Boundary      : %s\n" % str(self.s_bound))
            EDClog.write("##North Boundary      : %s\n" % str(self.n_bound))

        if (len(self.accessURLs) > 1):
            EDClog.write("##No. Access URLs     : %d ****\n" % (len(self.accessURLs)))
        else:
            EDClog.write("##No. Access URLs     : %d\n" % (len(self.accessURLs)))

        for u, m in self.accessURLs:
            EDClog.write("##Access URL          : {}\n".format(u))

        EDClog.write("##Local File Name     : {}\n".format(self.localFileName))

    def getGranuleSizeMB(self):
        return (self.granuleSizeMB)

    def setLocalFileName(self, fn):
        self.localFileName = fn

    def getLocalFileName(self):
        return self.localFileName

    def setDownloadStatus(self, dstat):
        self.downloadStatus = dstat

    def getDownloadStatus(self):
        return self.downloadStatus

    def getPolyPointStatus(self):
        return self.HasPolyPoints

    def getgranuleid(self):
        return self.egid

    def getgranulebd(self):
        return self.begDateTime

    def getgranuleed(self):
        return self.endDateTime

    def getgranulewb(self):
        return self.w_bound

    def getgranulesb(self):
        return self.s_bound

    def getgranuleeb(self):
        return self.e_bound

    def getgranulenb(self):
        return self.n_bound

    def getgranuleur(self):
        return self.granuleUR

    def setnumtrys(self, nt):
        self.numDloadTrys = nt

    def getnumtrys(self):
        return self.numDloadTrys

    def getPolyPoints(self):
        return self.polyPoints

    def setInsertFailed(self, statFlag):
        self.dbInsertFailed = statFlag

    def getInsertFailed(self):
        return self.dbInsertFailed


class ECHOpolypoint(object):
    def __init__(self, lat, lon):
        self.latitude = lat
        self.longitude = lon
        self.dbInsertFailed = False

    def getLatitude(self):
        return self.latitude

    def getLongitude(self):
        return self.longitude

    def setInsertFailed(self, statFlag):
        self.dbInsertFailed = statFlag

    def getInsertFailed(self):
        return self.dbInsertFailed


class ECHOdownloader(object):
    def __init__(self, ero, dbh):
        """
        :param ero: ECHO Request Object containing collections and granules
        :param dbh: Local 'echo' database handle object (for checking if granule
                    already exists (has been downloaded previously)
        :return: Process exists if download conditions (disk space etc) are not
                adequate.
        """
        self.rootDir = ero.getDirRoot()
        self.granuleQueue = []  # list of (egid, url, filename) tuples
        self.granuleStatus = {}  # egid, true/false(0/1) flag dictionary
        self.dbHandle = dbh

        if not self.downloadOk(ero):
            raise SystemExit

    def downloadOk(self, ero):
        """
        Check available disk space and download limit (both in MegaBytes) against
        the actual download size of the request.  Return True if ok, False otherwise
        """
        totalDataSizeMB = 0.0
        totalNumGranules = 0
        for i in range(ero.numCollections):
            totalNumGranules += ero.collContainer[i].numGranules
            totalDataSizeMB += ero.collContainer[i].getCollSizeMB()

        EDClog.write("ECHOdownloader::downloadOk\n")
        EDClog.write("\tRequesting %d granules, at %f MB\n" %
                     (totalNumGranules, totalDataSizeMB))

        if totalDataSizeMB <= 0.0:
            EDClog.write("ECHOdownloader::downloadOk\n")
            EDClog.write("\t****WARNING: Total data Size (MB) <= 0.0 (Granule XML issue)\n")
            EDClog.write("\tNOT ABORTING, BUT CHECK AVAILABLE DISK SPACE!\n")
            return True

        if totalDataSizeMB >= ero.getDiskSpaceAvail():
            EDClog.write("ECHOdownloader::downloadOk\n")
            EDClog.write("\t****ERROR: Total data size (%fMB) larger than available disk space (%fMB)\n" %
                         (totalDataSizeMB, ero.getDiskSpaceAvail()))
            return False

        if totalDataSizeMB > ero.dwnloadLimit:
            EDClog.write("ECHOdownloader::downloadOk\n")
            EDClog.write("\t****ERROR: Total data size (%fMB) larger than download limit (%fMB)\n" %
                         (totalDataSizeMB, ero.dwnloadLimit))
            EDClog.write("\tIncrease download limit on command line\n")
            return False

        return True

    def makeCollPath(self, archCtr, shortName):

        self.collPath = self.rootDir + '/' + archCtr + '/' + shortName

        if os.access(self.collPath, os.F_OK) and os.access(self.collPath, os.W_OK):
            # exists and is writeable
            return True

        if not os.access(self.collPath, os.F_OK):
            # doesn't exist, try to create it. note that we are using the
            # recursive OS functon 'makedirs' which makes all of the requisite
            # intermediate subdirectories that contain the final leaf directory
            try:
                os.makedirs(self.collPath, 0o755)
            except OSError:
                EDClog.write("ECHOdownloader::makeCollPath\n")
                EDClog.write("\tERROR: Couldn't create directory %s\n" % self.collPath)
                return False

        if not os.access(self.collPath, os.W_OK):
            # exists but is not writeable, try to change permission
            try:
                os.chmod(self.collPath, 0o755)
            except OSError:
                EDClog.write("ECHOdownloader::makeCollPath\n")
                EDClog.write("\tERROR: Couldn't make directory %s writeable\n" % self.collPath)
                return False

        return True

    def makeGranPath(self, gp):
        """
        :param gp: Desired granule pathname
        :return: true if path can be created and is writeable, false otherwise
        """

        if os.access(gp, os.F_OK) and os.access(gp, os.W_OK):
            # exists and is writeable
            return True

        if not os.access(gp, os.F_OK):
            # doesn't exist, try to create it. note that we are using the
            # recursive OS functon 'makedirs' which makes all of the requisite
            # intermediate subdirectories that contain the final leaf directory
            try:
                os.makedirs(gp, 0o755)
            except OSError:
                EDClog.write("ECHOdownloader::makeGranPath\n")
                EDClog.write("\tERROR: Couldn't create directory %s\n" % gp)
                return False

        if not os.access(gp, os.W_OK):
            # exists but is not writeable, try to change permission
            try:
                os.chmod(gp, 0o755)
            except OSError:
                EDClog.write("ECHOdownloader::makeGanPath\n")
                EDClog.write("\tERROR: Couldn't make directory %s writeable\n" % gp)
                return False

        return True

    def getCollPath(self):
        return (self.collPath)

    def downloadGranules(self, ero):

        for cc in ero.collContainer:
            # If there were no granules retrieved from ECHO, for this collection,
            # we can completely ignore it
            if len(cc.granContainer) > 0:
                if self.makeCollPath(cc.archCenter, cc.shortName):
                    # Collection filesystem ready to accept granules. Create
                    # queue of (egid, url, filename) tuples for all granules in collection

                    for g in cc.granContainer:

                        if (len(g.accessURLs) > 1):
                            EDClog.write("ECHOdownloader::downloadGranules\n")
                            EDClog.write("\tWARNING: Using URL 1 for granule %s with > 1 access URLs\n".format(g.egid))

                        granuleURL, mimeType = g.accessURLs[0]
                        filename = os.path.basename(granuleURL)
                        yyyy = int(g.begDateTime[0:4])
                        mm = int(g.begDateTime[5:7])
                        dd = int(g.begDateTime[8:10])

                        granDate = dt.date(yyyy, mm, dd)
                        yday = granDate.toordinal() - dt.date(yyyy, 1, 1).toordinal() + 1
                        ydayStr = '{0:03d}'.format(yday)

                        granPath = self.getCollPath() + '/' + str(yyyy) + '/' + ydayStr
                        if self.makeGranPath(granPath):
                            # Filesystem ready to receive this granule, add it to
                            # the download queue
                            granuleFilename = granPath + '/' + filename
                            # Save this granule's local filename in the granule
                            # object for subsequent loading of database
                            g.setLocalFileName(granuleFilename)

                            # If this granule has NOT already been inserted into the
                            # local 'echo' database, add it to the download queue
                            # v1.2.0 Only use check the DB if this is a useDB=True request
                            if ero.getDBflag() == "True":
                                qStr = "select granuleUR from granules where granID = '{}'".format(g.egid)
                                qResults = self.dbHandle.makeDBquery(qStr)
                                if not qResults:
                                    # This granule is NOT already in the DB, add it to the
                                    # download queue
                                    self.granuleQueue.append((g.egid, granuleURL, granuleFilename))
                                else:
                                    # Granule already in the DB, don't download
                                    self.granuleStatus[g.egid] = 0
                            else:
                                self.granuleQueue.append((g.egid, granuleURL, granuleFilename))
                        else:
                            self.granuleStatus[g.egid] = -2  # granule directory make failed
                else:
                    # Failed to make the collection directory, so ALL
                    # granules in this collection will NOT be downloaded
                    for g in cc.granContainer:
                        self.granuleStatus[g.egid] = -2  # collection directory make failed

        # All granules, for all collections, that have not already been
        # downloaded before (already in the local 'echo' database), are
        # in the download 'granuleQueue'.  Run file downloader.
        # You have two options here, you can call 'singledownload' or
        # 'multidownload'.  The 'multidownload' uses PyCurl's concurrent
        # download feature and is currently hardcoded for 10 simultaneous
        # downloads (determined after stress testing to be optimal for our
        # network conditions).  'singledownload' is included for
        # benchmarking purposes, and really should never be used as it is
        # about 50% slower (stress testing with 30 granules (~1.5GB) to
        # download).

        EDClog.write("ECHOdownloader::downloadGranules\n")
        EDClog.write("\t{0:d} total granules will be downloaded\n".format(len(self.granuleQueue)))
        if len(self.granuleQueue) > 0:
            self.multidownload()
        # self.singledownload()
        EDClog.write("\tFinished multi-download process\n")

        # Update the 'downloadStatus' attribute of all granule objects using the
        # 'granuleStatus' dictionary {egid,0|1|-1|-2}
        #
        #  0: No download attempted, granule already in local 'echo' DB
        #  1: download was attempted and was successful
        # -1: download was attempted and failed
        # -2: failed to make either the collection or granule holding directory
        #
        for cc in ero.collContainer:
            for g in cc.granContainer:
                g.setDownloadStatus(self.granuleStatus[g.egid])
                if g.getDownloadStatus() < 0:
                    # see above status codes
                    cc.setFailedStatus(True)

    def singledownload(self):

        for egid, url, filename in self.granuleQueue:
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            c.fp = open(filename, "wb")
            c.setopt(c.WRITEDATA, c.fp)
            try:
                c.perform()
            except pycurl.error:
                EDClog.write("\tsingledownload failed: %s\n" % egid)
                self.granuleStatus[egid] = 0
            else:
                EDClog.write("\tsinglelownload success: %s\n" % egid)
                self.granuleStatus[egid] = 1
            c.close()

    def multidownload(self):
        """
        Using PyCurl's multi-file concurrent download mechanism
        download the URL's contained in 'gQueue'.  On success, set
        status flag to 1 in 'gStatus' (gStatus is already initialized
        to 0's).

        This code is based on the Python program 'retriever-multi.py' that
        is provided with the PyCurl documentation.
        """
        concurrent_conns = 10
        queue = self.granuleQueue[:]
        num_urls = len(queue)

        # Pre-allocate a list of curl objects
        m = pycurl.CurlMulti()
        m.handles = []
        for i in range(concurrent_conns):
            c = pycurl.Curl()
            c.fp = None
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.CONNECTTIMEOUT, 30)
            c.setopt(pycurl.TIMEOUT, 300)
            c.setopt(pycurl.NOSIGNAL, 1)
            m.handles.append(c)

        freelist = m.handles[:]
        num_processed = 0
        while num_processed < num_urls:
            # If there is an url to process and a free curl object, add to multi stack
            while queue and freelist:
                egid, url, filename = queue.pop(0)
                c = freelist.pop()  # from the bottom
                c.fp = open(filename, "wb")
                c.setopt(pycurl.URL, url)
                c.setopt(pycurl.WRITEDATA, c.fp)
                m.add_handle(c)
                # store some info
                c.filename = filename
                c.url = url
                c.egid = egid
            # Run the internal curl state machine for the multi stack
            while 1:
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            # Check for curl objects which have terminated, and add them to the freelist
            while 1:
                num_q, ok_list, err_list = m.info_read()
                for c in ok_list:
                    c.fp.close()
                    c.fp = None
                    m.remove_handle(c)
                    self.granuleStatus[c.egid] = 1
                    EDClog.write("\tmultidownload success: %s\n" % c.egid)
                    freelist.append(c)
                for c, errno, errmsg in err_list:
                    c.fp.close()
                    c.fp = None
                    m.remove_handle(c)
                    self.granuleStatus[c.egid] = -1
                    #
                    # Perhaps this is where we should remove the empty
                    # local file?
                    #
                    EDClog.write("\tmultidownload failed: %s\n" % c.egid)
                    freelist.append(c)
                num_processed = num_processed + len(ok_list) + len(err_list)
                if num_q == 0:
                    break
            # Currently no more I/O is pending, could do something in the meantime
            # (display a progress bar, etc.).
            # We just call select() to sleep until some more data is available.
            m.select(1.0)

        # Cleanup
        for c in m.handles:
            if c.fp is not None:
                c.fp.close()
                c.fp = None
            c.close()

        m.close()

    def cleanup(self, ero):
        """
        :param: 'ero' - ECHO Request Object containing collections and granules
        Wade through all of the granules, in all collections.  If a
        download attempt was made, but failed (granule 'downloadStatus'
        set to -1), look for the local file and if it exists, delete it
        """
        EDClog.write("ECHOdownloader::cleanup\n")
        for c in ero.collContainer:
            for g in c.granContainer:
                if g.getDownloadStatus() == -1 and os.access(g.getLocalFileName(), os.F_OK):
                    try:
                        os.remove(g.getLocalFileName())
                    except OSError:
                        EDClog.write("\t****WARNING: Download of granule {} failed\n".format(g.egid))
                        EDClog.write("\tBut couldn't remove local file {}\n".format(g.getLocalFileName()))
                    else:
                        EDClog.write("\t***INFO: Download of granule {} failed\n".format(g.egid))
                        EDClog.write("\tRemoved local file {}\n".format(g.getLocalFileName()))


class ECHOdbHandler(object):
    def __init__(self, user, dbname, host):
        self.username = user
        self.database = dbname
        self.dbhost = host

        if not self.makeDBconnect():
            raise SystemExit

    def makeDBconnect(self):
        try:
            self.dbHook = MySQLdb.connect(user=self.username, host=self.dbhost,
                                          db=self.database, read_default_file="~/.my.cnf")
        except MySQLdb.OperationalError:
            EDClog.write("ECHOdbHandler::makeDBconnect\n")
            EDClog.write("\tCouldn't make connection to local 'echo' database.\n")
            return False
        else:
            # not really sure what a db cursor is, but will research this
            self.dbCursor = self.dbHook.cursor()
            return True

    def makeDBquery(self, queryStr):
        try:
            self.dbCursor.execute(queryStr)
        except MySQLdb.Error as error:
            EDClog.write("ECHOdbHandler::makeDBquery\n")
            EDClog.write("\t***ERROR: DB Query Error: {}\n".format(error))
            return None
        else:
            # Return all query results, None if empty set
            return self.dbCursor.fetchall()

    def makeDBinsert(self, sqlStr):
        """
        :param sqlStr: The SQL insert string
        :return: True on success, False on failure
        """
        try:
            self.dbCursor.execute(sqlStr)
        except MySQLdb.Error as error:
            EDClog.write("ECHOdbHandler::makdeDBinsert\n")
            EDClog.write("\t***ERROR: DB Insert Error: {}\n".format(error))
            self.dbHook.rollback()
            return False
        else:
            self.dbHook.commit()
            return True

    def collectionInsert(self, c):
        """
        :param c: The collection object
        :return: True on success, False on failure
        """
        cid = c.getid()
        csn = c.getshortname()
        cac = c.getarchcenter()
        cde = c.getdesc()
        cbd = c.getbegdate()
        ced = c.getenddate()
        doi = c.getdoi()

        EDClog.write("ECHOdbHandler::collectionInsert\n")
        qStr = "insert into collections (collID,shortName,archCenter,collDesc,begDateTime,endDateTime,doi)"
        if cbd == "null":
            cbdstr = "convert(null, datetime)"
        else:
            cbdstr = "convert('" + cbd + "',datetime)"
        if ced == "null":
            cedstr = "convert(null, datetime)"
        else:
            cedstr = "convert('" + ced + "',datetime)"

        qStr += " values('" + cid + "','" + csn + "','" + cac + \
                "','" + cde + "'," + cbdstr + ", " + cedstr + ", '" + doi + "');"

        if not self.makeDBinsert(qStr):
            EDClog.write("\tDB Insertion failure for collection {}\n".format(cid))
            return False

        EDClog.write("\tDB Insertion success for collection {}\n".format(cid))
        return True

    def granuleInsert(self, g, cid):
        """
        :param g: The granule object
        :param cid: The collection object id that owns the granule to insert
        :return: True on success, False on failure
        """
        if g.getPolyPointStatus():
            ppf = 1
        else:
            ppf = 0

        gid = g.getgranuleid()
        gur = g.getgranuleur()
        gbd = g.getgranulebd()
        ged = g.getgranuleed()
        gwb = g.getgranulewb()
        gsb = g.getgranulesb()
        geb = g.getgranuleeb()
        gnb = g.getgranulenb()
        glf = g.getLocalFileName()

        EDClog.write("ECHOdbHandler::granuleInsert\n")
        qStr = "insert into granules (granID,collID,granuleUR,begDateTime,endDateTime,"
        qStr += "hasPolyPoints,w_bound,s_bound,e_bound,n_bound,localFileName)"

        if gbd == "null":
            gbdstr = "convert(null, datetime)"
        else:
            gbdstr = "convert('" + gbd + "',datetime)"
        if ged == "null":
            gedstr = "convert(null, datetime)"
        else:
            gedstr = "convert('" + ged + "',datetime)"

        qStr = qStr + " values('" + gid + "','" + cid + "','" + gur + \
               "'," + gbdstr + ", " + gedstr + ", '" + \
               str(ppf) + "'," + \
               "'" + str(gwb) + \
               "','" + str(gsb) + "','" + str(geb) + "','" + str(gnb) + \
               "','" + glf + "');"
        if not self.makeDBinsert(qStr):
            EDClog.write("\tDB Insertion failure for granule {}\n".format(gid))
            return False

        EDClog.write("\tDB Insertion success for granule {}\n".format(gid))
        return True

    def polypointInsert(self, gid, lat, lon):
        """
        :param gid: The granule id
        :param lat: Latitude
        :param lon: Longitude
        :return: True on success, False on failure
        """
        EDClog.write("ECHOdbHandler::polypointInsert\n")
        qStr = "insert into polypoints (granID,latitude,longitude)"
        qStr += " values('" + gid + "','" + str(lat) + "','" + str(lon) + "');"
        if not self.makeDBinsert(qStr):
            EDClog.write("\tDB polyPoint insertion failure for granule {}\n".format(gid))
            return False

        EDClog.write("\tDB polyPoint insertion success for granule {}\n".format(gid))
        return True

    def update(self, ero):
        """
        :param ero: The ECHO Request Object containing collections and granules
        """
        for c in ero.collContainer:
            cid = c.getid()
            processGranules = False
            if c.getNumGranules() > 0:
                # If there is at least 1 granule for the collection we can do the
                # DB check
                processGranules = True
                qStr = "select shortName from collections where collID = '{}'".format(cid)
                qResults = self.makeDBquery(qStr)
                if not qResults:
                    # Collection not already in DB, try to add it
                    if not self.collectionInsert(c):
                        # Collection insert failed, thus all granules and
                        # granule polypoints become pending DB transactions as well
                        c.setInsertFailed(True)
                        processGranules = False
                        for g in c.granContainer:
                            g.setInsertFailed(True)
                            for pp in g.polyPoints:
                                pp.setInsertFailed(True)

            if processGranules:
                for g in c.granContainer:
                    gid = g.getgranuleid()
                    processPolyPoints = False
                    if g.getDownloadStatus() == 1:
                        # successful download, note that we don't have to check if the
                        # granule is already in the DB, because the ECHOdownloader did
                        # that check prior to adding the granule to the download queue.
                        processPolyPoints = True
                        if not self.granuleInsert(g, cid):
                            # Granule insert failed, thus all polypoints become
                            # pending DB transactions as well
                            g.setInsertFailed(True)
                            processPolyPoints = False
                            for pp in g.polyPoints:
                                pp.setInsertFailed(True)

                    if processPolyPoints and g.getPolyPointStatus():
                        for pp in g.polyPoints:
                            lat = pp.getLatitude()
                            lon = pp.getLongitude()
                            if not self.polypointInsert(gid, lat, lon):
                                # Individual PolyPoint record insert failed
                                pp.setInsertFailed(True)


class ECHOptxHandler(object):
    # Create an instance of a parser object, and give it the ability
    # to remove comments from XML data
    txparser = ET.XMLParser(remove_comments=True)

    def __init__(self, c_pending_file, g_pending_file, p_pending_file, dbh):

        self.cpf = c_pending_file
        self.gpf = g_pending_file
        self.ppf = p_pending_file
        self.dbHandle = dbh
        self.processC = False
        self.processG = False
        self.processP = False

    def havePending(self):
        # If any of the pending transaction files exist, we have pending
        if os.access(self.cpf, os.F_OK):
            self.processC = True
        if os.access(self.gpf, os.F_OK):
            self.processG = True
        if os.access(self.ppf, os.F_OK):
            self.processP = True
        return self.processC or self.processG or self.processP

    def openPending(self):
        EDClog.write("ECHOptxHandler::openPending\n\tOpening pending transaction file(s)...\n")

        if self.processC:
            try:
                self.cpendfh = open(self.cpf, 'r')
            except IOError:
                EDClog.write("\tCouldn't open pending collection transaction file {}\n".format(self.cpf))
                raise SystemExit

        if self.processG:
            try:
                self.gpendfh = open(self.gpf, 'r')
            except IOError:
                EDClog.write("\tCouldn't open pending granule transaction file {}\n".format(self.gpf))
                raise SystemExit

        if self.processP:
            try:
                self.ppendfh = open(self.ppf, 'r')
            except IOError:
                EDClog.write("\tCouldn't open pending polypoint transaction file {}\n".format(self.ppf))
                raise SystemExit

    def processPending(self):
        """
        NOTE: Pending database transactions MUST be handled in the
              following order to satisfy the database referential
              integrity constraints!

              1. Pending COLLECTION transactions
              2. Pending GRANULE transactions
              3. Pending POLYPOINT transactions

        NOTE: If we EVER return from 'processtx' it means the entire
              transaction file was processed successfully, and we
              MUST then remove it.
        """

        EDClog.write("ECHOptxHandler::processPending\n")
        if self.processC:
            EDClog.write("\tProcessing pending collection transactions...\n")
            self.processtx('C', self.cpendfh, self.cpf)
            try:
                os.remove(self.cpf)
            except OSError:
                EDClog.write("\tProcessed BUT couldn't remove pending file {}\n".format(self.cpf))
                raise SystemExit
            else:
                EDClog.write("\tProcessed and removed pending file {}\n".format(self.cpf))

        if self.processG:
            EDClog.write("\tProcessing pending granule transactions...\n")
            self.processtx('G', self.gpendfh, self.gpf)
            try:
                os.remove(self.gpf)
            except OSError:
                EDClog.write("\tProcessed BUT couldn't remove pending file {}\n".format(self.gpf))
                raise SystemExit
            else:
                EDClog.write("\tProcessed and removed pending file {}\n".format(self.gpf))

        if self.processP:
            EDClog.write("\tProcessing pending polpyPoint transactions...\n")
            self.processtx('P', self.ppendfh, self.ppf)
            try:
                os.remove(self.ppf)
            except OSError:
                EDClog.write("\tProcessed BUT couldn't remove pending file {}\n".format(self.ppf))
                raise SystemExit
            else:
                EDClog.write("\tProcessed and removed pending file {}\n".format(self.ppf))

    def processtx(self, ttype, tfh, tfn):

        if ttype == 'C':
            xmltag = "collection"
            fields = ['collID', 'shortName', 'archCenter', 'collDesc', 'begDateTime', 'endDateTime', 'doi']
        elif ttype == 'G':
            xmltag = "granule"
            fields = ['granID', 'collID', 'granuleUR', 'sizeMB', 'begDateTime', 'endDateTime', 'hasPolyPoints',
                      'w_bound', 's_bound', 'e_bound', 'n_bound', 'localFileName']
        elif ttype == 'P':
            xmltag = "polypoint"
            fields = ['granID', 'latitude', 'longitude']
        else:
            EDClog.write("ECHOptxHandler::processtx\n")
            EDClog.write("\t****FATAL: Internal error, invalid transaction type ({})\n".format(ttype))
            raise SystemExit

        try:
            pendTree = ET.parse(tfh, self.txparser)  # Use XML 'txparser' defined as class variable
        except ET.ParseError:
            EDClog.write("ECHOptxHandler::processtx\n")
            EDClog.write(
                "\tFATAL: Could not parse pending transaction XML file ({}), problem with XML syntax\n".format(tfn))
            raise SystemExit
        else:
            # close the pending transaction file in case we need re-open it in write
            # mode to save pending transaction information (if another failure occurs)
            tfh.close()

        pendRoot = pendTree.getroot()
        for transaction in pendRoot.findall(xmltag):
            if ttype == 'C':
                fStr = "insert into collections ("
            elif ttype == 'G':
                fStr = "insert into granules ("
            else:
                fStr = "insert into polypoints ("

            # Get all the field values for the current transaction and
            # build the first half of the SQL insert statement (field names)
            values = []
            n = 0
            for f in fields:
                values.append(transaction.find(f).text)
                fStr += f
                if n < (len(fields) - 1):
                    fStr += ','
                n += 1
            fStr += ')'

            # Now build the second half of the SQL insert statement (field values)
            # This i
            n = 0
            vStr = " values("
            for v in values:
                if fields[n] == "begDateTime" or fields[n] == "endDateTime":
                    if v == 'null':
                        tStr = "convert(null,datetime)"
                    else:
                        tStr = "convert('" + v + "',datetime)"
                else:
                    tStr = "'" + v + "'"
                if n < (len(values) - 1):
                    tStr += ','
                n += 1
                vStr += tStr
            vStr += ');'

            qStr = fStr + vStr
            EDClog.write(qStr + "\n")

            if not self.dbHandle.makeDBinsert(qStr):
                #
                # Insert failure. We need to save the state of the
                # pending transaction XML data.  Therefore, we need to overwrite
                # the pending transaction state file, and abort
                #
                try:
                    tfh = open(tfn, 'w')
                except IOError:
                    EDClog.write("ECHOptxHandler::processtx\n")
                    EDClog.write("\tCouldn't re-open pending transaction file {}\n".format(tfn))
                else:
                    tfh.write(ET.tostring(pendRoot, pretty_print=True))
                    tfh.close()

                raise SystemExit

            else:
                #
                # Insert success.  We now need to REMOVE the current transaction
                # element from the XML root string, in case we need to re-write
                # the transaction file in the event of a database insert error
                pendRoot.remove(transaction)

    def savePendTx(self, ero):
        """
        :param ero: ECHO Request Object containing all collections, granules
         and polypoint objects
        """

        cxmlroot = ET.Element("data")
        gxmlroot = ET.Element("data")
        pxmlroot = ET.Element("data")

        for c in ero.collContainer:
            cid = c.getid()
            if c.getInsertFailed():
                self.makeCelement(c, cxmlroot)
            for g in c.granContainer:
                gid = g.getgranuleid()
                if g.getInsertFailed():
                    self.makeGelement(g, cid, gxmlroot)
                for p in g.polyPoints:
                    if p.getInsertFailed():
                        self.makePelement(p, gid, pxmlroot)

        if len(cxmlroot) > 0:
            self.writePendingTx(self.cpf, cxmlroot)

        if len(gxmlroot) > 0:
            self.writePendingTx(self.gpf, gxmlroot)

        if len(pxmlroot) > 0:
            self.writePendingTx(self.ppf, pxmlroot)

    def writePendingTx(self, fname, xmlroot):
        EDClog.write("ECHOptxHandler::writePendingTx\n")
        try:
            fh = open(fname, "a")
        except IOError:
            EDClog.write("\tFailed to open pending transaction file {}\n".format(fname))
            raise SystemExit

        try:
            fh.write(ET.tostring(xmlroot, pretty_print=True))
        except IOError:
            EDClog.write("\tFailed to write to pending transaction file {}\n".format(fname))
            raise SystemExit

        EDClog.write("\tWrote pending transaction file {}\n".format(fname))

        fh.close()

    def makeCelement(self, cobj, croot):
        ce = ET.SubElement(croot, "collection")
        collid = ET.SubElement(ce, 'collID')
        collid.text = cobj.getid()
        sn = ET.SubElement(ce, "shortName")
        sn.text = cobj.getshortname()
        ac = ET.SubElement(ce, "archCenter")
        ac.text = cobj.getarchcenter()
        desc = ET.SubElement(ce, "collDesc")
        desc.text = cobj.getdesc()
        bdt = ET.SubElement(ce, 'begDateTime')
        bdt.text = cobj.getbegdate()
        edt = ET.SubElement(ce, 'endDateTime')
        edt.text = cobj.getenddate()
        doi = ET.SubElement(ce, 'doi')
        doi.text = cobj.getdoi()

    def makeGelement(self, gobj, cid, groot):
        ge = ET.SubElement(groot, "granule")
        granidelement = ET.SubElement(ge, "granID")
        granidelement.text = gobj.getgranuleid()
        collidelement = ET.SubElement(ge, "collID")
        collidelement.text = cid
        granurelement = ET.SubElement(ge, "granuleUR")
        granurelement.text = gobj.getgranuleur()
        gransizeelement = ET.SubElement(ge, "sizeMB")
        gransizeelement.text = str(gobj.getGranuleSizeMB())
        bdtelement = ET.SubElement(ge, 'begDateTime')
        bdtelement.text = gobj.getgranulebd()
        edtelement = ET.SubElement(ge, 'endDateTime')
        pptelement = ET.SubElement(ge, 'hasPolyPoints')
        pptelement.text = str(gobj.getPolyPointStatus())
        edtelement.text = gobj.getgranuleed()
        wbelement = ET.SubElement(ge, 'w_bound')
        wbelement.text = str(gobj.getgranulewb())
        sbelement = ET.SubElement(ge, 's_bound')
        sbelement.text = str(gobj.getgranulesb())
        ebelement = ET.SubElement(ge, 'e_bound')
        ebelement.text = str(gobj.getgranuleeb())
        nbelement = ET.SubElement(ge, 'n_bound')
        nbelement.text = str(gobj.getgranulenb())
        lfelement = ET.SubElement(ge, 'localFileName')
        lfelement.text = gobj.getLocalFileName()

    def makePelement(self, pobj, gid, proot):
        pe = ET.SubElement(proot, "polypoint")
        granidelement = ET.SubElement(pe, "granID")
        granidelement.text = gid
        latelement = ET.SubElement(pe, "latitude")
        latelement.text = str(pobj.getLatitude())
        lonelement = ET.SubElement(pe, "longitude")
        lonelement.text = str(pobj.getLongitude())


if __name__ == '__main__':

    runMgr = runManager()
    EDClog = runMgr.getLogFH()

    # The request object is used to manage request information and
    # retrieved information.
    echoReqObj = ECHOrequest(runMgr)

    #############################################################################
    # Per discussion with Lanxi Min on 9/2/2015, we decided that to
    # insure local 'echo' database integrity, the first process should
    # be to try to handle any pending database transactions that might
    # have failed from a previous EDClient run. Therefore, do not move
    # the position of the 'ECHOptxHandler' object creation line. Note that
    # the creation of the DB handler object 'edbhand' had to be moved here
    # so that the transaction handler can use it if pending transactions
    # have to be processed.
    #
    # Create local 'echo' database handler object. Note that there is
    # no password information hardcoded here. Just pass it (1) the
    # database username, (2) the database name, and (3) the database
    # server address.
    #
    # On the system you are running 'EDClient.py' you must create a
    # file called '.my.cnf' in your HOME directory with the following
    # format/content:
    #
    # [client]
    # user = mark
    # password = "<your database password, including the quotes>"

    edbhand = ECHOdbHandler("mark", "echo", "asrcserv3.asrc.cestm.albany.edu")

    # v1.2.0 pending DB transactions are processed ONLY if user has
    # enabled DB tracking
    ptxObj = ECHOptxHandler("_ptxC.xml", "_ptxG.xml", "_ptxP.xml", edbhand)
    if echoReqObj.getDBflag() == "True":
        if ptxObj.havePending():
            ptxObj.openPending()
            # If any db transaction problems occur in 'processPending' EDClient
            # will terminate itself
            ptxObj.processPending()

    #############################################################################

    # Make ECHO client object to manage communication with web service
    echoClient = ECHOclient(runMgr.getMaxFiles())

    # Get collection and granule information from ECHO
    echoReqObj.getReqData(echoClient)

    # Collection and granule information for the user's request
    # has been stored, close the client connection to the web service
    echoClient.logout()

    # Is download requested, or just information query?
    if runMgr.getopMode() == 'D':

        # Since download mode was requested, we might as well
        # integrate any pending granule downloads into the current
        # collection/granule objects.  Load the pending download
        # information and then get rid of the current file.  Exit
        # on any failures here
        #
        # v1.2.0 In the case of a useDB=False run, forget about any
        # pending file downloads
        if echoReqObj.getDBflag() == "True":
            if echoReqObj.getHavePendDwnld():
                echoReqObj.loadPendDwnld()
                echoReqObj.zapPending()

        # v1.2.0 Downloader ONLY needs the DB handle object for
        # peeking into the local 'echo' database to see if a
        # granule has already been downloaded, IF AND ONLY IF this
        # is a useDB=True request.  The downloader will only use
        # the DB check if the request object DB flag is set true
        edloader = ECHOdownloader(echoReqObj, edbhand)
        edloader.downloadGranules(echoReqObj)
        # Remove (cleanup) any partial file downloads
        edloader.cleanup(echoReqObj)

        # v1.2.0 Only save pending file downloads and update local
        # database with new collection and granule information, and
        # save any DB transaction failures IF AND ONLY IF this was
        # a useDB=True request
        if echoReqObj.getDBflag() == "True":
            echoReqObj.savePending()  # file downloads
            edbhand.update(echoReqObj)
            ptxObj.savePendTx(echoReqObj)

    else:  # Query ECHO only
        for i in range(echoReqObj.numCollections):
            echoReqObj.collContainer[i].showCollectionInfo()
            echoReqObj.collContainer[i].showGranuleInfo()
