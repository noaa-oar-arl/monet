# this is written to retrive airnow data concatenate and add to pandas array for usage
import os
from datetime import datetime, timedelta

import pandas as pd
from numpy import array

from tools import search_listinlist


class airnow:
    def __init__(self):

        self.username = ''
        self.password = ''
        self.datadir = '.'
        self.cwd = os.getcwd()
        self.url = 'ftp.airnowgateway.org'
        self.dates = [datetime.strptime('2016-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2016-06-06 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.datestr = []
        self.ftp = None
        self.df = None
        self.se_states = array(
                ['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TX',
                 'VA', 'WV'], dtype='|S14')
        self.ne_states = array(['CT', 'DE', 'DC', 'ME', 'MD', 'MA',
                                'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'],
                               dtype='|S20')
        self.nc_states = array(
                ['IL', 'IN', 'IA', 'KY', 'MI', 'MN', 'MO', 'OH', 'WI'],
                dtype='|S9')
        self.sc_states = array(['AR', 'LA', 'OK', 'TX'], dtype='|S9')
        self.r_states = array(['AZ', 'CO', 'ID', 'KS', 'MT', 'NE', 'NV', 'NM',
                               'ND', 'SD', 'UT', 'WY'], dtype='|S12')
        self.p_states = array(['CA', 'OR', 'WA'], dtype='|S10')
        self.objtype = 'AirNow'
        self.filelist = None
        self.monitor_file = os.getcwd() + '/monitoring_site_locations.dat'
        self.monitor_df = None
        self.savecols = ['datetime', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'datetime_local',
                         'Site_Name', 'Latitude', 'Longitude', 'CMSA_Name', 'MSA_Name', 'State_Name','Region']

    def retrieve_hourly_filelist(self):
        self.ftp.cwd('HourlyData')
        nlst = self.ftp.nlst('2*')
        return nlst

    def openftp(self):
        from ftplib import FTP
        self.ftp = FTP(self.url)
        self.ftp.login(self.username, self.password)

    def convert_dates_tofnames(self):
        self.datestr = []
        for i in self.dates:
            self.datestr.append(i.strftime('%Y%m%d%H.dat'))

    def download_single_rawfile(self, fname):
        localfile = open(fname, 'wb')
        self.ftp.retrbinary('RETR ' + fname, localfile.write, 1024)
        localfile.close()

    def download_rawfiles(self, flist, path='.'):
        import os
        if flist.shape[0] < 2:
            print 'Downloading: ' + flist[0]
            self.download_single_rawfile(flist[0])
        else:
            for i in flist:
                if os.path.exists(path + '/' + i) == False:
                    print 'Downloading: ' + i
                    self.download_single_rawfile(i)
                else:
                    print 'File Found in Path: ' + i

    def change_path(self):
        os.chdir(self.datadir)

    def change_back(self):
        os.chdir(self.cwd)

    def download_hourly_files(self, path='.'):
        from numpy import empty,where
        self.datadir = path
        self.change_path()
        print 'Connecting to FTP: ' + self.url
        self.openftp()
        print 'Retrieving Hourly file list'
        nlst = self.retrieve_hourly_filelist()
        self.convert_dates_tofnames()
        cwd = 'HourlyData'
        index1, index2 = search_listinlist(array(nlst), array(self.datestr))
        inarchive = empty(array(self.datestr).shape[0])
        inarchive[:] = True
        inarchive[index2] = False
        index = where(inarchive)[0]

        #download archive files first
        if index.shape[0] > 0:
            year = self.dates[0].strftime('%Y')
            self.ftp.cwd('/HourlyData/Archive/'+year)
            for i in array(self.datestr)[index]:
                if os.path.exists(i) == False:
                    print 'Downloading from Archive: ', i
                    self.download_single_rawfile(i)
                else:
                    print 'File found: ', i

        #now downlad all in the Current HourlyData
        index = where(inarchive==False)[0]
        if index.shape[0] > 0:
            year = self.dates[0].strftime('%Y')
            self.ftp.cwd('/HourlyData')
            for i in array(self.datestr)[index]:
                if os.path.exists(i) == False:
                    print 'Downloading from HourlyData: ' + i
                    self.download_single_rawfile(i)
                else:
                    print 'File found: ', i
        self.filelist = self.datestr
        self.change_back()
        self.ftp.close()

    def aggragate_files(self, output=''):
        from numpy import sort
        from datetime import datetime
        from StringIO import StringIO
        self.change_path()
        fnames = sort(self.filelist)
        a = ''
        for i in fnames:
            with open(i,'rb') as f:
                a = a + f.read()
        dft = pd.read_csv(StringIO(a),delimiter='|',header=None)
        cols = ['date','time', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'Source']
        dft.columns = cols
        dates = [datetime.strptime(x+' '+y,'%m/%d/%y %H:%M') for x,y in zip(dft.date.values,dft.time.values)]
        dft.drop('date',axis=1,inplace=True)
        dft.drop('time',axis=1,inplace=True)
        dft['datetime'] = dates
        self.df = dft.copy()
        self.calc_datetime()
        print '    Adding in Meta-data'
        self.get_station_locations()
        self.get_region2()
        self.df = self.df.copy().drop_duplicates()
        self.df = self.df[self.savecols]
        if output == '':
            output = 'AIRNOW.hdf'
        print 'Outputing data to: ' + output
        self.df.to_hdf(output, 'df', format='fixed')
        self.change_back()

    def calc_datetime(self):
        # takes in an array of string dates and converts to numpy array of datetime objects
        dt = []
        for i in self.df.utcoffset.values:
            dt.append(timedelta(hours=i))
        self.df['datetime_local'] = self.df.datetime + dt

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end, freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates

    def get_station_locations(self):
        self.read_monitor_file()
        self.df = pd.merge(self.df, self.monitor_df, on='SCS', how='left')

    def get_station_locations_remerge(self, df):
        df = pd.merge(df, self.monitor_df.drop(['Latitude', 'Longitude'], axis=1), on='SCS', how='left')
        return df

    def read_monitor_file(self):
        from glob import glob
        if os.path.isfile(self.monitor_file) == True:
            print '    Monitor Station Meta-Data Found: Compiling Dataset'
            fname = self.monitor_file
        else:
            self.openftp()
            self.ftp.cwd('Locations')
            self.download_single_rawfile(fname='monitoring_site_locations.dat')
            fname = glob('monitoring_site_locations.dat')[0]
        colsinuse = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
        f = pd.read_csv(fname, delimiter='|', header=None, usecols=colsinuse)
        f.columns = ['SCS', 'Site_Code', 'Site_Name', 'Status', 'Agency', 'Agency_Name', 'EPA_region', 'Latitude',
                     'Longitude', 'Elevation', 'GMT_Offset', 'Country_Code', 'CMSA_Code', 'CMSA_Name', 'MSA_Code',
                     'MSA_Name', 'State_Code', 'State_Name', 'County_Code', 'County_Name', 'City_Code']
        self.monitor_df = f.copy()

    def get_region(self):
        sr = self.df.State_Name.values
        for i in self.se_states:
            con = sr == i
            sr[con] = 'Southeast'
        for i in self.ne_states:
            con= sr == i
            sr[con] = 'Northeast'
        for i in self.nc_states:
            con= sr == i
            sr[con] = 'North Central'
        for i in self.sc_states:
            con= sr == i
            sr[con] = 'South Central'
        for i in self.p_states:
            con= sr == i
            sr[con] = 'Pacific'
        for i in self.r_states:
            con= sr == i
            sr[con] = 'Rockies'
        self.df['Region'] = array(sr)
