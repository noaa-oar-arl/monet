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
        self.daily = False
        self.objtype = 'AirNow'
        self.filelist = None
        self.monitor_file = inspect.getfile(
            self.__class__)[:-16] + '/data/monitoring_site_locations.dat'
        self.monitor_df = None
        self.savecols = ['time', 'siteid', 'site', 'utcoffset', 'variable', 'units', 'obs', 'time_local', 'latitude', 'longitude', 'cmsa_name', 'msa_code', 'msa_name', 'state_name',
                         'epa_region']

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
            dft = pd.read_csv(fn, delimiter='|', header=None, error_bad_lines=False)
            cols = ['date', 'time', 'siteid', 'site', 'utcoffset', 'variable', 'units', 'obs', 'source']
            dft.columns = cols
        except:
            cols = ['date', 'time', 'siteid', 'site', 'utcoffset', 'variable', 'units', 'obs', 'source']
            dft = pd.DataFrame(columns=cols)
        dft['obs'] = dft.obs.astype(float)
        dft['siteid'] = dft.siteid.str.zfill(9)
        dft['utcoffset'] = dft.utcoffset.astype(int)
        return dft

    def aggragate_files(self):
        import dask
        import dask.dataframe as dd

        print('Aggregating AIRNOW files...')
        self.build_urls()
        dfs = [dask.delayed(self.read_csv)(f) for f in self.url]
        dff = dd.from_delayed(dfs)
        df = dff.compute()
        df['time'] = pd.to_datetime(
            df.date + ' ' + df.time, format='%m/%d/%y %H:%M', exact=True, box=False)
        df.drop(['date'], axis=1, inplace=True)
        df['time_local'] = df.time + pd.to_timedelta(df.utcoffset, unit='H')
        self.df = df
        print('    Adding in Meta-data')
        self.get_station_locations()
        self.df = self.df[self.savecols]
        self.df.drop_duplicates(inplace=True)

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end,
                              freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates

    def get_station_locations(self):
        from .epa_util import read_monitor_file
        self.monitor_df = read_monitor_file()
        #self.monitor_df = self.monitor_df.loc[self.monitor_df.siteid.notnull()]
        #self.monitor_df['siteid'] = self.monitor_df.siteid.astype(int).astype(str).str.zfill(9)
        self.df = pd.merge(self.df, self.monitor_df, on='siteid', how='left')

    def get_station_locations_remerge(self, df):
        df = pd.merge(df, self.monitor_df.drop(
            ['Latitude', 'Longitude'], axis=1), on='siteid', how='left')
        return df
