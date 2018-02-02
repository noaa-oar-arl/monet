from __future__ import print_function

import inspect
import os
# this is written to retrive airnow data concatenate and add to pandas array for usage
from builtins import object
from datetime import datetime, timedelta

import pandas as pd
from numpy import array


class AirNow(object):
    def __init__(self):
        self.datadir = '.'
        self.cwd = os.getcwd()
        self.url = None
        self.dates = [datetime.strptime('2016-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2016-06-06 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.datestr = []
        self.df = None
        self.se_states = array(
            ['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN',
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
        self.monitor_file = inspect.getfile(
            self.__class__)[:-16] + '/data/monitoring_site_locations.dat'
        self.monitor_df = None
        self.savecols = ['datetime', 'SCS', 'Site', 'utcoffset', 'Species', 'Units', 'Obs', 'datetime_local',
                         'Site_Name', 'Latitude', 'Longitude', 'CMSA_Name', 'MSA_Code', 'MSA_Name', 'State_Name',
                         'EPA_region']

    def convert_dates_tofnames(self):
        self.datestr = []
        for i in self.dates:
            self.datestr.append(i.strftime('%Y%m%d%H.dat'))

    def change_path(self):
        os.chdir(self.datadir)

    def change_back(self):
        os.chdir(self.cwd)

    def build_urls(self):
        from numpy import empty, where
        import wget
        import requests
        from glob import glob

        furls = []

        print('Retrieving AIRNOW files...')
        # 2017/20170131/HourlyData_2017012408.dat
        url = 'https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/'
        for i in self.dates:
            f = url + i.strftime('%Y/%Y%m%d/HourlyData_%Y%m%d%H.dat')
            furls.append(f)

        # files needed for comparison
        self.url = pd.Series(furls, index=None)

    def read_csv(self, fn):
        try:
            dft = pd.read_csv(fn, delimiter='|', header=None,
                              error_bad_lines=False)
            cols = ['date', 'time', 'SCS', 'Site', 'utcoffset',
                    'Species', 'Units', 'Obs', 'Source']
            dft.columns = cols
        except:
            cols = ['date', 'time', 'SCS', 'Site', 'utcoffset',
                    'Species', 'Units', 'Obs', 'Source']
            dft = pd.DataFrame(columns=cols)
        return dft

    def aggragate_files(self):
        import dask
        import dask.dataframe as dd

        print('Aggregating AIRNOW files...')
        self.build_urls()
        dfs = [dask.delayed(self.read_csv)(f) for f in self.url]
        dff = dd.from_delayed(dfs)
        df = dff.compute()
        df['datetime'] = pd.to_datetime(
            df.date + ' ' + df.time, format='%m/%d/%y %H:%M', exact=True, box=False)
        df.drop(['date', 'time'], axis=1, inplace=True)
        df['datetime_local'] = df.datetime + \
                               pd.to_timedelta(df.utcoffset, unit='H')
        self.df = df
        print('    Adding in Meta-data')
        self.get_station_locations()
        self.df = self.df[self.savecols]
        self.df.drop_duplicates(inplace=True)

    def calc_datetime(self):
        # takes in an array of string dates and converts to numpy array of datetime objects
        dt = []
        for i in self.df.utcoffset.values:
            dt.append(timedelta(hours=i))
        self.df['datetime_local'] = self.df.datetime + dt

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end,
                              freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates

    def get_station_locations(self):
        self.read_monitor_file()
        self.df = pd.merge(self.df, self.monitor_df, on='SCS', how='left')

    def get_station_locations_remerge(self, df):
        df = pd.merge(df, self.monitor_df.drop(
            ['Latitude', 'Longitude'], axis=1), on='SCS', how='left')
        return df

    def read_monitor_file(self):
        try:
            print('    Monitor Station Meta-Data Found: Compiling Dataset')
            monitor_url = 'https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/today/monitoring_site_locations.dat'
            colsinuse = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                         11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
            f = pd.read_csv(monitor_url, delimiter='|',
                            header=None, usecols=colsinuse)
            f.columns = ['SCS', 'Site_Code', 'Site_Name', 'Status', 'Agency', 'Agency_Name', 'EPA_region', 'Latitude',
                         'Longitude', 'Elevation', 'GMT_Offset', 'Country_Code', 'CMSA_Code', 'CMSA_Name', 'MSA_Code',
                         'MSA_Name', 'State_Code', 'State_Name', 'County_Code', 'County_Name', 'City_Code']
        except:
            from glob import glob
            if os.path.isfile(self.monitor_file) == True:
                print('    Monitor Station Meta-Data Found: Compiling Dataset')
                fname = self.monitor_file
            else:
                self.openftp()
                self.ftp.cwd('Locations')
                self.download_single_rawfile(
                    fname='monitoring_site_locations.dat')
            fname = glob('monitoring_site_locations.dat')[0]
            colsinuse = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                         11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
            f = pd.read_csv(fname, delimiter='|',
                            header=None, usecols=colsinuse)
            f.columns = ['SCS', 'Site_Code', 'Site_Name', 'Status', 'Agency', 'Agency_Name', 'EPA_region', 'Latitude',
                         'Longitude', 'Elevation', 'GMT_Offset', 'Country_Code', 'CMSA_Code', 'CMSA_Name', 'MSA_Code',
                         'MSA_Name', 'State_Code', 'State_Name', 'County_Code', 'County_Name', 'City_Code']

        self.monitor_df = f.copy()

    def get_region(self):
        sr = self.df.State_Name.copy().values
        for i in self.se_states:
            con = sr == i
            sr[con] = 'Southeast'
        for i in self.ne_states:
            con = sr == i
            sr[con] = 'Northeast'
        for i in self.nc_states:
            con = sr == i
            sr[con] = 'North Central'
        for i in self.sc_states:
            con = sr == i
            sr[con] = 'South Central'
        for i in self.p_states:
            con = sr == i
            sr[con] = 'Pacific'
        for i in self.r_states:
            con = sr == i
            sr[con] = 'Rockies'
        sr[sr == 'CC'] = 'Canada'
        sr[sr == 'MX'] = 'Mexico'

        self.df['Region'] = array(sr)
