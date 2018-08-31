from __future__ import print_function

from builtins import object, zip
from datetime import datetime

import pandas as pd
from numpy import NaN, array


class IMPROVE(object):
    """Short summary.

    Attributes
    ----------
    datestr : type
        Description of attribute `datestr`.
    df : type
        Description of attribute `df`.
    daily : type
        Description of attribute `daily`.
    se_states : type
        Description of attribute `se_states`.
    ne_states : type
        Description of attribute `ne_states`.
    nc_states : type
        Description of attribute `nc_states`.
    sc_states : type
        Description of attribute `sc_states`.
    r_states : type
        Description of attribute `r_states`.
    p_states : type
        Description of attribute `p_states`.

    """

    def __init__(self):
        self.datestr = []
        self.df = None
        self.daily = True

    def add_data(self, fname, add_meta=False, delimiter='\t'):
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
        f = open(fname, 'r')
        lines = f.readlines()
        skiprows = 0
        skip = False
        for i, line in enumerate(lines):
            if line == 'Data\n':
                skip = True
                skiprows = i + 1
                break
        # if meta data is inlcuded
        if skip:
            df = pd.read_csv(
                fname,
                delimiter=delimiter,
                parse_dates=[2],
                infer_datetime_format=True,
                dtype={'EPACode': str},
                skiprows=skiprows)
        else:
            df = pd.read_csv(
                fname,
                delimiter=delimiter,
                parse_dates=[2],
                infer_datetime_format=True,
                dtype={'EPACode': str})
        df.rename(columns={'EPACode': 'epaid'}, inplace=True)
        df.rename(columns={'Val': 'Obs'}, inplace=True)
        df.rename(columns={'State': 'state_name'}, inplace=True)
        df.rename(columns={'ParamCode': 'variable'}, inplace=True)
        df.rename(columns={'SiteCode': 'siteid'}, inplace=True)
        df.rename(columns={'Unit': 'Units'}, inplace=True)
        df.rename(columns={'Date': 'time'}, inplace=True)
        df.drop('Dataset', axis=1, inplace=True)
        df['time'] = pd.to_datetime(df.time, format='%Y%m%d')
        df.columns = [i.lower() for i in df.columns]
        if pd.Series(df.keys()).isin(['epaid']).max():
            df['epaid'] = df.epaid.astype(str).str.zfill(9)
        if add_meta:
            dropkeys = ['latitude', 'longitude', 'poc']

            monitor_df = read_monitor_file(network='IMPROVE')  # .drop(
            # dropkeys, axis=1)
            df = df.merge(
                monitor_df, how='left', left_on='epaid', right_on='siteid')
            df.drop(['siteid_y', 'state_name_y'], inplace=True, axis=1)
            df.rename(
                columns={
                    'siteid_x': 'siteid',
                    'state_name_x': 'state_name'
                },
                inplace=True)
            #df = df.dropna(subset=['variable', 'gmt_offset']).drop_duplicates()
        #self.df.Variable.loc[self.df.Variable == 'MT'] = 'PM10'
        #self.df.Variable.loc[self.df.Variable == 'MF'] = 'PM2.5'
        try:
            df.obs.loc[df.obs < df.mdl] = NaN
        except:
            df.obs.loc[df.obs < -900] = NaN
        #self.df.dropna(subset=['obs'], inplace=True)
        # self.df['time_local'] = self.df.time + pd.to_timedelta(
        #    self.df.gmt_offset.astype(float), unit='H')
        self.df = df
        return df.copy()

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
        con = (self.df.time >= dates[0]) & (self.df.time <= dates[-1])
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
        dates = pd.date_range(
            start=begin, end=end, freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates
