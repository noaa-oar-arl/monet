# this is written to retrive airnow data concatenate and add to pandas array for usage
from datetime import datetime, timedelta

import pandas as pd
from numpy import array

from tools import search_listinlist


class airnow:
    def __init__(self):

        self.username = 'Barry.Baker'
        self.password = 'p00pST!ck123'
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

    def aggragate_files(self, fname=''):
        from glob import glob
        from numpy import sort
        from datetime import datetime
        fnames = sort(array(glob(fname)))
        ff = []
        if fnames.shape[0] < 2:
            print 'Loading ' + fnames[0]
            dft = pd.read_csv(fnames[0], delimiter='|', header=None, parse_dates=[[0, 1]], infer_datetime_format=True,
                              na_values='-999')
            cols = ['datetime', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'Source']
            dft.columns = cols
            self.df = dft.copy()
        else:
            first = True
            for i in fnames:
                print 'Aggregating: ' + i
                dft = pd.read_csv(i, delimiter='|', header=None, parse_dates=[[0, 1]], infer_datetime_format=True,
                                  na_values='-999')
                cols = ['datetime', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'Source']
                dft.columns = cols
                dft.datetime = dft.datetime.values.astype('M8[s]').astype(datetime)
                dft.index = dft.datetime
                ff.append(dft)

        self.df = pd.concat(ff)
        self.calc_datetime()
        self.get_station_locations(path='monitoring_site_locations.dat')
        self.get_region()
        self.df = self.df.drop_duplicates()
        self.df.to_hdf('AIRNOW_.hdf', 'df', format='fixed')

    def calc_datetime(self):
        # takes in an array of string dates and converts to numpy array of datetime objects
        dt = []
        for i in self.df.utcoffset.values:
            dt.append(timedelta(hours=i))
        self.df['datetime_local'] = self.df.datetime + dt

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end, freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates

    def get_station_locations(self, path=''):
        import os
        from glob import glob

        if os.path.isfile(path):
            fname = path
        else:
            self.openftp()
            self.ftp.cwd('Locations')
            self.download_single_rawfile(fname='monitoring_site_locations.dat')
            fname = glob('monitoring_site_locations.dat')
        colsinuse = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
        f = pd.read_csv(fname, delimiter='|', header=None, usecols=colsinuse)
        f.columns = ['SCS', 'Site_Code', 'Site_Name', 'Status', 'Agency', 'Agency_Name', 'EPA_region', 'Latitude',
                     'Longitude', 'Elevation', 'GMT_Offset', 'Country_Code', 'CMSA_Code', 'CMSA_Name', 'MSA_Code',
                     'MSA_Name', 'State_Code', 'State_Name', 'County_Code', 'County_Name', 'City_Code']
        self.df = pd.merge(self.df, f, on='SCS', how='left')

    def get_region(self):
        sn = self.df.State_Name.values
        sr = []
        for i in sn:
            if i in self.se_states:
                sr.append('Southeast')
            elif i in self.ne_states:
                sr.append('Northeast')
            elif i in self.nc_states:
                sr.append('North Central')
            elif i in self.sc_states:
                sr.append('South Central')
            elif i in self.p_states:
                sr.append('Pacific')
            elif i in self.r_states:
                sr.append('Rockies')
            else:
                sr.append('????')
        self.df['Region'] = array(sr)