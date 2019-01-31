#!/usr/bin/env python

import sys,os,getopt
sys.path.insert(0, 'simple-salesforce')
from simple_salesforce import Salesforce
import urllib
import urllib2
import csv
import time
import gzip
import shutil
import traceback

from datetime import date, timedelta, datetime
from StringIO import StringIO

import os

sys.path.insert(0, 'ds-integration')
from DefenseStorm import DefenseStorm

class integration(object):

    def getSalesForceLookupList(self, ObjectName, ElementName):
        entries = {}
        query = 'SELECT Id,%s From %s' %(ElementName, ObjectName)
    
        sf_data = self.sf.query_all(query)
        self.ds.log('INFO', "Lookup %s totalSize: %s" %(ObjectName, sf_data['totalSize']))
        for item in sf_data['records']:
            entries[item['Id']] = item[ElementName]
        self.ds.log('INFO', "%s Entries: %s" %(ObjectName, len(entries.keys())))
        return entries
    
    def getEventLogs(self, dir):
        ''' Query salesforce service using REST API '''
    
        # query Ids from Event Log File
        query = 'SELECT Id, EventType, Logdate From EventLogFile Where LogDate = Last_n_Days:2'
        res_dict = self.sf.query_all(query)
    
        # capture record result size to loop over
        total_size = res_dict['totalSize']
    
        # provide feedback if no records are returned
        if total_size < 1:
            self.ds.log('INFO', "No EventLogFiles were returned")
            sys.exit()
    
        # If directory doesn't exist, create one
        os.makedirs(dir)
    
        # loop over elements in result and download each file locally
        for i in range(total_size):
            # pull attributes out of JSON for file naming
            ids = res_dict['records'][i]['Id']
            types = res_dict['records'][i]['EventType']
            dates = res_dict['records'][i]['LogDate']
    
            # create REST API request
            url = self.ds.config_get('salesforce', 'instance_url') + '/services/data/v33.0/sobjects/EventLogFile/'+ids+'/LogFile'
    
            headers = {'Authorization' : 'Bearer ' + self.sf.session_id, 'X-PrettyPrint' : '1', 'Accept-encoding' : 'gzip'}
    
            # begin profiling
            start = time.time()
    
            # open connection
            req = urllib2.Request(url, None, headers)
            res = urllib2.urlopen(req)
    
            # provide feedback to user
            self.ds.log('DEBUG', 'Downloading: ' + dates[:10] + '-' + types + '.csv to ' + os.getcwd() + '/' + dir)
    
            # if the response is gzip-encoded as expected
            # compression code from http://bit.ly/pyCompression
            if res.info().get('Content-Encoding') == 'gzip':
                # buffer results
                buf = StringIO(res.read())
                # gzip decode the response
                f = gzip.GzipFile(fileobj=buf)
                data = f.read()
                # close buffer
                buf.close()
            else:
                # buffer results
                buf = StringIO(res.read())
                # get the value from the buffer
                data = buf.getvalue()
                buf.close()
    
            # write buffer to CSV with following naming convention yyyy-mm-dd-eventtype.csv
            file = open(dir + '/' +dates[:10]+'-'+types+'.csv', 'w')
            file.write(data)
    
            # end profiling
            end = time.time()
            secs = end - start
    
            self.ds.log('INFO', 'File: ' + dates[:10] + '-' + types + '.csv to ' + os.getcwd() + '/' + dir + ' elapsed time: ' + str('%0.2f' %secs) + ' seconds')
    
            file.close
            i = i + 1
    
            # close connection
            res.close
    
    
    def handleFiles(self, datadir, filelist):
        for item in filelist:
            start = time.time()
            file=item['filename']
            self.ds.log('DEBUG', 'Starting sending file: %s' %item['filename'])
            type=item['type']
            with open(datadir+'/'+file) as f:
                header = f.readline()
                header = header.replace('\"','')
                header = header.replace('\n','')
                elementList = header.split(",")
                f.seek(0)
                for line in csv.DictReader(f):
                    try:
                        if 'USER_ID_DERIVED' in line.keys():
                            line['USER'] = self.UserList[line['USER_ID_DERIVED']]
                    except KeyError:
                        pass
                    try:
                        if 'ORGANIZATION_ID' in line.keys():
                            line['ORGANIZATION'] = self.OrganizationList[line['ORGANIZATION_ID']]
                    except KeyError:
                        pass
                    try:
                        if 'REPORT_ID' in line.keys():
                            line['REPORT'] = self.ReportList[line['REPORT_ID']]
                    except KeyError:
                        pass
                    try:
                        if 'DASHBOARD_ID' in line.keys():
                            line['DASHBOARD'] = self.DashboardList[line['DASHBOARD_ID']]
                    except KeyError:
                        pass
                    try:
                        if 'DOCUMENT_ID' in line.keys():
                            line['DOCUMENT'] = self.DocumentList[line['DOCUMENT_ID']]
                    except KeyError:
                        pass
                    try:
                        if 'DASHBOARD_COMPONENT_ID' in line.keys():
                            line['DASHBOARD_COMPONENT'] = self.DashboardComponentList[line['DASHBOARD_COMPONENT_ID']]
                    except KeyError:
                        pass
                    try:
                        if 'SITE_ID' in line.keys():
                            line['SITE'] = self.SiteList[line['SITE_ID']]
                    except KeyError:
                        pass
    
                    try:
                        if 'EVENT_TYPE' in line.keys():
                            action = line['EVENT_TYPE']
                    except KeyError:
                        action = ''
                        pass

                    extension = {}
                    leftovers = []
                    for key in line:
                        if line[key] == '':
                            continue
                        elif key == 'EVENT_TYPE':
                            extension['cat'] = line[key]
                        #elif key == 'USER_ID_DERIVED':
                        #    extension['duid'] = line[key]
                        elif key == 'USER':
                            extension['duser'] = line[key]
                        elif key == 'REQUEST_STATUS':
                            if line[key] == 'S':
                                extension['outcome'] = 'Success'
                            elif line[key] == 'F':
                                extension['outcome'] = 'Failure'
                            elif line[key] == 'U':
                                extension['outcome'] = 'Undefined'
                            elif line[key] == 'A':
                                extension['outcome'] = 'Authorization Error'
                            elif line[key] == 'R':
                                extension['outcome'] = 'Redirect'
                            elif line[key] == 'N':
                                extension['outcome'] = 'Not Found'
                            else:
                                extension['outcome'] = line[key]
                        elif key == 'LOGIN_STATUS':
                            extension['reason'] = line[key]
                        elif key == 'URI':
                            extension['request'] = line[key]
                        elif key == 'USER_AGENT':
                            extension['requestClientApplication'] = line[key]
                        elif key == 'LOGIN_KEY':
                            extension['requestCookies'] = line[key]
                        elif key == 'TIMESTAMP':
                            extension['rt'] = str(int(time.mktime(time.strptime(line[key], '%Y%m%d%H%M%S.%f')))*1000)
                        elif key == 'SOURCE_IP':
                            extension['sourceTranslatedAddress'] = line[key]
                        elif key == 'CLIENT_IP':
                            extension['src'] = line[key]
                        elif key == 'ENTITY_NAME':
                            extension['cs1Label'] = 'entity_name'
                            extension['cs1'] = line[key]
                        elif key == 'CLIENT_IP':
                            extension['cs2Label'] = 'client_ip'
                            extension['cs2'] = line[key]
                        else:
                            leftovers.extend([key + '=' + line[key]])
    
                    extension['msg'] = ' '.join(leftovers).replace('=','\\=')
    
                    self.ds.writeCEFEvent(type=type, action=action, dataDict=extension)

            end = time.time()
            secs = end - start
            self.ds.log('INFO', 'Completed events from file: %s elapsed time: %s' %(item['filename'], str('%0.2f' %secs)))
    
    def dirFile(self, datadir):
       dirlist = [ f for f in os.listdir(datadir) if os.path.isfile(os.path.join(datadir,f)) ]
       filelist = []
       for filename in dirlist:
          types=filename[11:-4]
          mydict = { 'type': types, 'filename': filename }
          filelist.append(mydict)
       return filelist
    
    def run(self):
        try:
            # SalesForce Credentials
            self.username = self.ds.config_get('salesforce', 'username')
            self.password = self.ds.config_get('salesforce', 'password')
            self.security_token = self.ds.config_get('salesforce', 'security_token')
            self.instance_url = self.ds.config_get('salesforce', 'instance_url')
            # CEF Info
    
        except getopt.GetoptError:
            self.ds.log('CRITICAL', 'Error reading config values')
            self.ds.log('CRITICAL', traceback.print_exc())
            sys.exit(2)
        try:
            self.sf = Salesforce(instance_url=self.instance_url, username=self.username, password=self.password, security_token=self.security_token)
        except getopt.GetoptError:
            self.ds.log('CRITICAL', 'Error Logging into SalesFoce')
            self.ds.log('CRITICAL', traceback.print_exc())
            sys.exit(2)
    
        if self.dir == None:
            self.ds.log('INFO', 'Processing events from Salesforce')
            self.dir = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
            self.getEventLogs(self.dir)
        else:
            self.ds.log('INFO', 'Processing events from directory: ' + self.dir)
    
        self.filelist = self.dirFile(self.dir)
    
        self.OrganizationList = self.getSalesForceLookupList('Organization', 'Name')
        self.ReportList = self.getSalesForceLookupList('Report', 'Name')
        self.DashboardList = self.getSalesForceLookupList('Dashboard', 'Title')
        self.DocumentList = self.getSalesForceLookupList('Document', 'Name')
        self.DashboardComponentList = self.getSalesForceLookupList('DashboardComponent', 'Name')
        self.SiteList = self.getSalesForceLookupList('Site', 'Name')
        self.UserList = self.getSalesForceLookupList('User', 'Email')
    
        self.handleFiles(self.dir, self.filelist)
        if self.cleanup:
            shutil.rmtree(self.dir)

    def usage(self):
        print
        print os.path.basename(__file__)
        print
        print '  No Options: Download yesterdays files from SF and process'
        print
        print '  -t    Testing mode.  Do all the work but do not send events to GRID via '
        print '        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\''
        print '        in the current directory'
        print
        print '  -d <directory>'
        print '        Rerun with a set of CSV files on disk in the specificed directory'
        print '        NOTE: This will not delete the directory after successful run'
        print
        print '  -n    Do not cleanup the download directory after run'
        print
        print '  -l    Log to stdout instead of syslog Local6'
        print
    
    def __init__(self, argv):

        self.testing = False
        self.cleanup = True
        self.send_syslog = True
        self.ds = None
        self.dir = None
    
        try:
            opts, args = getopt.getopt(argv,"htnld:",["datedir="])
        except getopt.GetoptError:
            self.usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                self.usage()
                sys.exit()
            elif opt in ("-d"):
                self.dir = arg
                self.cleanup = False
            elif opt in ("-t"):
                self.testing = True
            elif opt in ("-n"):
                self.cleanup = False
            elif opt in ("-l"):
                self.send_syslog = False
    
        try:
            self.ds = DefenseStorm('salesforceEventLogs', testing=self.testing, send_syslog = self.send_syslog)
        except Exception ,e:
            traceback.print_exc()
            try:
                self.ds.log('ERROR', 'ERROR: ' + str(e))
            except:
                pass


if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()