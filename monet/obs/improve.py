from __future__ import print_function

from builtins import object, zip
from datetime import datetime

import pandas as pd
from numpy import NaN, array


class IMPROVE(object):
    def __init__(self):
        self.datestr = []
        self.df = None
        self.se_states = array(['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV'], dtype='|S2')
        self.ne_states = array(['CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'], dtype='|S2')
        self.nc_states = array(['IL', 'IN', 'IA', 'KY', 'MI', 'MN', 'MO', 'OH', 'WI'], dtype='|S2')
        self.sc_states = array(['AR', 'LA', 'OK', 'TX'], dtype='|S2')
        self.r_states = array(['AZ', 'CO', 'ID', 'KS', 'MT', 'NE', 'NV', 'NM', 'ND', 'SD', 'UT', 'WY'], dtype='|S2')
        self.p_states = array(['CA', 'OR', 'WA'], dtype='|S2')

    def open_file(self, fname):
        """     This assumes that you have downloaded the data from
                        http://views.cira.colostate.edu/fed/DataWizard/Default.aspx
                The data is the IMPROVE Aerosol dataset
                Any number of sites
                Parameters included are All
                Fields include Dataset,Site,Date,Parameter,POC,Data_value,Unit,Latitude,Longitude,State,EPA Site Code
                        Options are delimited ','  data only and normalized skinny format

        Parameters
        ----------
        fname : type
            Description of parameter `fname`.
        output : type
            Description of parameter `output` (the default is '').

        Returns
        -------
        type
            Description of returned object.

        """
        from .epa_util import read_monitor_file

        self.df = pd.read_csv(fname, delimiter=',', parse_dates=[2], infer_datetime_format=True)
        self.df.rename(columns={'EPACode': 'SCS'}, inplace=True)
        self.df.rename(columns={'Val': 'Obs'}, inplace=True)
        self.df.rename(columns={'State': 'State_Name'}, inplace=True)
        self.df.rename(columns={'ParamCode': 'Species'}, inplace=True)
        self.df.rename(columns={'SiteCode': 'Site_Code'}, inplace=True)
        self.df.rename(columns={'Unit': 'Units'}, inplace=True)
        self.df.rename(columns={'Date': 'datetime'}, inplace=True)
        self.df.drop('Dataset', axis=1, inplace=True)
        self.df['datetime'] = pd.to_datetime(self.df.datetime, format='%Y%m%d')
        dropkeys = ['Latitude', 'Longitude', 'POC']
        monitor_df = read_monitor_file(network='IMPROVE').drop(dropkeys, axis=1)
        self.df = self.df.merge(monitor_df, how='left', on='SCS')
        self.df = self.df.dropna(subset=['Species', 'GMT_Offset']).drop_duplicates()
        self.df.Species.loc[self.df.Species == 'MT'] = 'PM10'
        self.df.Species.loc[self.df.Species == 'MF'] = 'PM2.5'
        self.df.Obs.loc[self.df.Obs < 0] = NaN
        self.df.dropna(subset=['Obs'], inplace=True)
        self.df['datetime_local'] = self.df.datetime + pd.to_timedelta(self.df.GMT_Offset.astype(float), unit='H')
        return self.df

    def load_hdf(self, fname, dates):
        """Short summary.

        Parameters
        ----------
        fname : type
            Description of parameter `fname`.
        dates : type
            Description of parameter `dates`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.df = pd.read_hdf(fname)
        self.get_date_range(self.dates)

    def get_date_range(self, dates):
        """Short summary.

        Parameters
        ----------
        dates : type
            Description of parameter `dates`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.dates = dates
        con = (self.df.datetime >= dates[0]) & (self.df.datetime <= dates[-1])
        self.df = self.df.loc[con]

    def set_daterange(self, begin='', end=''):
        """Short summary.

        Parameters
        ----------
        begin : type
            Description of parameter `begin` (the default is '').
        end : type
            Description of parameter `end` (the default is '').

        Returns
        -------
        type
            Description of returned object.

        """
        dates = pd.date_range(start=begin, end=end, freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates
