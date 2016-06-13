# this is written to retrive airnow data concatenate and add to pandas array for usage
import pandas as pd
from tools import search_listinlist
from datetime import datetime
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
            self.download_single_rawfile(flist[0])
        else:
            for i in flist:
                self.download_single_rawfile(i)

    def retrive_hourly_files(self):
        self.openftp()
        nlst = self.retrieve_hourly_filelist()
        index1, index2 = search_listinlist(array(nlst), array(self.datestr))
        if index1.shape < 1:
            self.ftp.cwd('Archive')
            year = self.dates[0].strftime('%Y')
            self.ftp.cwd(year)
            nlst = self.ftp.nlst('*')
            index1, index2 = search_listinlist(array(nlst), array(self.datestr))
            if index1.shape[0] < 1:
                print 'AirNow does not have hourly data at this time.  Please try again'
            else:
                first = True
                for i in nlst[index1]:
                    dft = pd.DataFrame(i, delimiter='|', header=None, infer_datetime_format=True)
                    cols = ['date', 'time', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'Source']
                    dft.columns = cols
                    dates = self.calc_datetime(dft.date.values,dft.date.values)
                    dft['datetime_local'] = dates
                    if first:
                        self.output = dft.copy()
                    else:
                        self.output = self.output.append(dft)

    def load_airnow_file(self, filename='intputfile'):
        dft = pd.DataFrame(filename, delimiter='|', header=None, infer_datetime_format=True)
        cols = ['date', 'time', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'Source']
        dft.columns = cols

    def calc_datetime(self,dates,times):
        #takes in an array of string dates and converts to numpy array of datetime objects
        dt = []
        for i,j in zip(dates,times):
            string = i + ' ' + j
            dt.append(datetime.strptime(string,'%y/%d/%m %H:%M'))
        return array(dt)

