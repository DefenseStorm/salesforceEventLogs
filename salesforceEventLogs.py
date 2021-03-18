#!/usr/bin/env python3

import sys,os,getopt
#sys.path.insert(0, 'simple-salesforce')
from simple_salesforce import Salesforce
from urllib import request
#import urllib2
import csv
import time
import gzip
import shutil
import traceback

from datetime import date, timedelta, datetime
#from StringIO import StringIO
import io

import os

sys.path.insert(0, 'ds-integration')
from DefenseStorm import DefenseStorm

class integration(object):

    # Field mappings are local fields = GRID fields
    JSON_field_mappings = {
        'CLIENT_IP' : 'ip_src',
        'USER' : 'username',
        'EVENT_TYPE' : 'category',
        #'TIMESTAMP_DERIVED' : 'timestamp',
        'FILE_TYPE' : 'file_type',
        'SOURCE_IP' : 'src_ip',
        'BROWSER_TYPE' : 'http_user_agent',
        'URI' : 'http_path',
        'LOGIN_STATUS' : 'status',
        'DELEGATED_USER_NAME' : 'src_username'
    }


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
        if self.interval == 'hourly':
            state = self.ds.get_state(self.state_dir)
            if state  != None:
                query = 'SELECT Id, EventType, Interval, LogDate, LogFile, Sequence From EventLogFile Where Interval = \'hourly\' and LogDate > %s Order By LogDate ASC' %state
            else:
                query = 'SELECT Id, EventType, Interval, LogDate, LogFile, Sequence From EventLogFile Where LogDate >= YESTERDAY and Interval = \'hourly\' Order By LogDate ASC'
        elif self.interval == 'daily':
            query = 'SELECT Id, EventType, Logdate, Interval From EventLogFile Where LogDate = Last_n_Days:2'
        else:
            self.ds.log('ERROR', "Bad entry for 'interval' in conf file")
            sys.exit()

        res_dict = self.sf.query_all(query)
    
        # capture record result size to loop over
        total_size = res_dict['totalSize']

        last_time = None
        for item in res_dict['records']:
            last_time = item['LogDate']
    
        # provide feedback if no records are returned
        if total_size < 1:
            self.ds.log('INFO', "No EventLogFiles were returned")
            sys.exit()
    
        # If directory doesn't exist, create one
        try:
            os.makedirs(dir)
        except:
            self.ds.log('ERROR', "Directory (%s) already exists...cleaning up to try to recover" %dir)
            try:
                shutil.rmtree(self.dir)
                os.makedirs(dir)
            except:
                sys.exit()
    
        # loop over elements in result and download each file locally
        for i in range(total_size):
            # pull attributes out of JSON for file naming
            ids = res_dict['records'][i]['Id']
            types = res_dict['records'][i]['EventType']
            dates = res_dict['records'][i]['LogDate']
            self.new_state = dates
    
            # create REST API request
            url = self.ds.config_get('salesforce', 'instance_url') + '/services/data/v33.0/sobjects/EventLogFile/'+ids+'/LogFile'
    
            headers = {'Authorization' : 'Bearer ' + self.sf.session_id, 'X-PrettyPrint' : '1', 'Accept-encoding' : 'gzip'}
    
            # begin profiling
            start = time.time()
    
            # open connection
            req = request.Request(url, headers=headers)
            res = request.urlopen(req)
    
            # provide feedback to user
            self.ds.log('DEBUG', 'Downloading: ' + dates + '-' + types + '.csv to ' + os.getcwd() + '/' + dir)
    
            # if the response is gzip-encoded as expected
            # compression code from http://bit.ly/pyCompression
            if res.info().get('Content-Encoding') == 'gzip':
                # buffer results
                buf = io.BytesIO(res.read())
                # gzip decode the response
                f = gzip.GzipFile(fileobj=buf)
                data = f.read()
                # close buffer
                buf.close()
            else:
                # buffer results
                buf = io.BytesIO(res.read())
                # get the value from the buffer
                data = buf.getvalue()
                buf.close()
    
            # write buffer to CSV with following naming convention yyyy-mm-dd-eventtype.csv
            file = open(dir + '/' +dates+'-'+types+'.csv', 'w')
            file.write(data.decode("utf-8"))
    
            # end profiling
            end = time.time()
            secs = end - start
    
            self.ds.log('INFO', 'File: ' + dates + '-' + types + '.csv to ' + os.getcwd() + '/' + dir + ' elapsed time: ' + str('%0.2f' %secs) + ' seconds')
    
            file.close
            i = i + 1
    
            # close connection
            res.close
    
    
    def handleFiles(self, datadir, filelist):
        for item in filelist:
            start = time.time()
            file=item['filename']
            self.ds.log('INFO', 'Starting sending file: %s' %item['filename'])
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
                    #try:
                        #if 'ENTITY_ID' in line.keys():
                            #line['ATTACHMENT'] = self.AttachmentList[line['ENTITY_ID']]
                    #except KeyError:
                        #pass
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
                    line['message'] = line['EVENT_TYPE']
                    self.ds.writeJSONEvent(line, JSON_field_mappings = self.JSON_field_mappings, flatten=False)

            end = time.time()
            secs = end - start
            self.ds.log('INFO', 'Completed events from file: %s elapsed time: %s' %(item['filename'], str('%0.2f' %secs)))
    
    def dirFile(self, datadir):
       filelist = []

       if not os.path.isdir(datadir):
           self.ds.log('INFO', 'Data download directory (%s) does not exist' %datadir)
           return filelist

       dirlist = [ f for f in os.listdir(datadir) if os.path.isfile(os.path.join(datadir,f)) ]
       for filename in dirlist:
          types=filename[11:-4]
          mydict = { 'type': types, 'filename': filename }
          filelist.append(mydict)
       return filelist

    def getLookupTables(self):
        self.OrganizationList = self.getSalesForceLookupList('Organization', 'Name')
        self.ReportList = self.getSalesForceLookupList('Report', 'Name')
        self.DashboardList = self.getSalesForceLookupList('Dashboard', 'Title')
        self.DocumentList = self.getSalesForceLookupList('Document', 'Name')
        #self.AttachmentList = self.getSalesForceLookupList('Attachment', 'Name')
        self.DashboardComponentList = self.getSalesForceLookupList('DashboardComponent', 'Name')
        self.SiteList = self.getSalesForceLookupList('Site', 'Name')
        self.UserList = self.getSalesForceLookupList('User', 'Email')
    
    def run(self):
        try:
            # SalesForce Credentials
            self.username = self.ds.config_get('salesforce', 'username')
            self.password = self.ds.config_get('salesforce', 'password')
            self.security_token = self.ds.config_get('salesforce', 'security_token')
            self.instance_url = self.ds.config_get('salesforce', 'instance_url')
            # CEF Info
            # Other options
            self.interval = self.ds.config_get('salesforce', 'interval')
            self.state_dir = self.ds.config_get('salesforce', 'state_dir')
    
        except getopt.GetoptError:
            self.ds.log('CRITICAL', 'Error reading config values')
            self.ds.log('CRITICAL', traceback.print_exc())
            sys.exit(2)
        try:
            self.sf = Salesforce(instance_url=self.instance_url, username=self.username, password=self.password, security_token=self.security_token)
        except Exception as e:
            tb = traceback.format_exc()
            tb = tb.replace('\n', "")
            self.ds.log('CRITICAL', 'Error Logging into SalesFoce')
            self.ds.log('CRITICAL', '%s' %tb)
            sys.exit(2)
        try:
            if self.dir == None:
                self.ds.log('INFO', 'Processing events from Salesforce')
                if self.interval == 'hourly':
                    self.dir = date.today().strftime("%Y-%m-%d")
                else:
                    self.dir = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
                self.getEventLogs(self.dir)
            else:
                self.ds.log('INFO', 'Processing events from directory: ' + self.dir)

            self.filelist = self.dirFile(self.dir)
            if len(self.filelist) > 0:
                self.getLookupTables() 
                self.handleFiles(self.dir, self.filelist)
                if self.cleanup:
                    shutil.rmtree(self.dir)
            else:
                self.ds.log('INFO', 'No log files to process.  Error downloading or no work to do based on state file')

            if self.cleanup:
                self.ds.set_state(self.state_dir, self.new_state)
        except:
            self.ds.log('CRITICAL', 'Error handling salesforce events')
            self.ds.log('CRITICAL', traceback.print_exc())
            if self.cleanup:
                self.ds.log('CRITICAL', 'Attempting Cleanup')
                try:
                    shutil.rmtree(self.dir)
                except:
                    pass

    def usage(self):
        print('')
        print(os.path.basename(__file__))
        print
        print('  No Options: Download yesterdays files from SF and process')
        print
        print('  -t    Testing mode.  Do all the work but do not send events to GRID via ')
        print('        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\'')
        print('        in the current directory')
        print
        print('  -d <directory>')
        print('        Rerun with a set of CSV files on disk in the specificed directory')
        print('        NOTE: This will not delete the directory after successful run')
        print
        print('  -n    Do not cleanup the download directory after run')
        print
        print('  -l    Log to stdout instead of syslog Local6')
        print
    
    def __init__(self, argv):

        self.testing = False
        self.cleanup = True
        self.send_syslog = True
        self.ds = None
        self.dir = None
        self.new_state = None
    
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
                #self.cleanup = False
            elif opt in ("-t"):
                self.testing = True
            elif opt in ("-n"):
                self.cleanup = False
            elif opt in ("-l"):
                self.send_syslog = False
    
        try:
            self.ds = DefenseStorm('salesforceEventLogs', testing=self.testing, send_syslog = self.send_syslog)
        except Exception as e:
            traceback.print_exc()
            try:
                tb = traceback.format_exc()
                tb = tb.replace('\n', "")
                self.ds.log('CRITICAL', 'Error Logging into SalesFoce')
                self.ds.log('ERROR', 'ERROR: %s' %tb)
            except:
                pass


if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
