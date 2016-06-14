# this is written to retrive airnow data concatenate and add to pandas array for usage
import pandas as pd
from tools import search_listinlist
from datetime import datetime,timedelta
from numpy import array


class airnow:
    def __init__(self):

        self.username = 'Barry.Baker'
        self.password = 'p00pST!ck123'
        self.url = 'ftp.airnowgateway.org'
        self.dates = [datetime.strptime('2016-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2016-06-06 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.datestr = []
        self.ftp = None
        self.output = None
        self.objtype = 'AirNow'

    def retrieve_hourly_filelist(self):
        self.ftp.cwd('HourlyData')
        nlst = self.ftp.nlst('*')[1:]
        return nlst

    def openftp(self):
        from ftplib import FTP
        self.ftp = FTP(self.url)
        self.ftp.login(self.username, self.password)

    def convert_dates_tofnames(self):
        for i in self.dates:
            self.datestr.append(i.strftime('%Y%m%d%H.dat'))

    def download_single_rawfile(self, fname):
        localfile = open(fname, 'wb')
        self.ftp.retrbinary('RETR ' + fname, localfile.write, 1024)
        localfile.close()

    def download_rawfiles(self, flist):
        if flist.shape[0] < 2:
            print 'Downloading: ' + flist[0]
            self.download_single_rawfile(flist[0])
        else:
            for i in flist:
                print 'Downloading: ' + i
                self.download_single_rawfile(i)

    def download_hourly_files(self):
        print 'Connecting to FTP: ' + self.url
        self.openftp()
        print 'Retrieving Hourly file list'
        nlst = self.retrieve_hourly_filelist()
        self.convert_dates_tofnames()
        index1, index2 = search_listinlist(array(nlst), array(self.datestr))
        if index1.shape[0] < 1:
            self.ftp.cwd('Archive')
            year = self.dates[0].strftime('%Y')
            self.ftp.cwd(year)
            nlst = self.ftp.nlst('*')
            index1, index2 = search_listinlist(array(nlst), array(self.datestr))
            if index1.shape[0] < 1:
                print 'AirNow does not have hourly data at this time. Please try again'
            else:
                self.download_rawfiles(array(nlst)[array(index1)])

        else:
            self.download_rawfiles(array(nlst)[array(index1)])

    def load_airnow_files(self, fname=''):
        from glob import glob
        fnames = array(glob(fname))
        if fnames.shape[0] < 2:
            dft = pd.read_csv(fnames[0],delimiter='|', header=None, parse_dates=[[0,1]],infer_datetime_format=True)
            cols = ['datetime', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'Source']
            dft.columns = cols
        else:
            first = True
            for i in fnames:
                dft = pd.read_csv(i,delimiter='|', header=None, parse_dates=[[0,1]],infer_datetime_format=True)
                cols = ['datetime', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'Source']
                dft.columns = cols
                #dft['datetime_local'] =
                if first:
                    self.output = dft.copy()
                    first=False
                else:
                    self.output = self.output.append(dft)
        self.calc_datetime()

    def calc_datetime(self):
        #takes in an array of string dates and converts to numpy array of datetime objects
        dt = []
        for i in self.output.utcoffset.values:
            dt.append(timedelta(hours=i))
        self.output['datetime_local'] = self.output.datetime + dt
        
    def set_daterange(self,begin='',end=''):
        dates = pd.date_range(start=begin,end=end,freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates
