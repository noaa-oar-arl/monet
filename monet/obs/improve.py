from __future__ import print_function
# this is written to retrive airnow data concatenate and add to pandas array for usage

from builtins import zip
from builtins import object
from datetime import datetime

import pandas as pd
from numpy import NaN, array


class improve(object):
    def __init__(self):
        self.datestr = []
        self.df = None
        self.se_states = array(['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV'], dtype='|S2')
        self.ne_states = array(['CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'], dtype='|S2')
        self.nc_states = array(['IL', 'IN', 'IA', 'KY', 'MI', 'MN', 'MO', 'OH', 'WI'], dtype='|S2')
        self.sc_states = array(['AR', 'LA', 'OK', 'TX'], dtype='|S2')
        self.r_states = array(['AZ', 'CO', 'ID', 'KS', 'MT', 'NE', 'NV', 'NM', 'ND', 'SD', 'UT', 'WY'], dtype='|S2')
        self.p_states = array(['CA', 'OR', 'WA'], dtype='|S2')

    def open_file(self, fname, output=''):
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
        self.df = pd.read_csv(fname, delimiter=',', parse_dates=[2], infer_datetime_format=True)
        self.df.rename(columns={'EPACode': 'SCS'}, inplace=True)
        self.df.rename(columns={'Value2': 'Obs'}, inplace=True)
        self.df.rename(columns={'State': 'State_Name'}, inplace=True)
        self.df.rename(columns={'ParamCode': 'Species'}, inplace=True)
        self.df.rename(columns={'SiteCode': 'Site_Code'}, inplace=True)
        self.df.rename(columns={'Unit': 'Units'}, inplace=True)
        self.df.rename(columns={'Date': 'datetime'}, inplace=True)
        self.df.drop('Dataset', axis=1, inplace=True)
        print('Adding in some Meta-Data')
        print('Calculating local time')
        self.df = self.get_local_datetime(self.df)
        self.df = self.df.copy().drop_duplicates()
        self.df.dropna(subset=['Species'], inplace=True)
        self.df.Species.loc[self.df.Species == 'MT'] = 'PM10'
        self.df.Species.loc[self.df.Species == 'MF'] = 'PM2.5'
        self.df.datetime = [datetime.strptime(i, '%Y%m%d') for i in self.df.datetime]
        if output == '':
            output = 'IMPROVE.hdf'
        print('Outputing data to: ' + output)
        self.df.Obs.loc[self.df.Obs < 0] = NaN
        self.df.dropna(subset=['Obs'], inplace=True)
        self.df.to_hdf(output, 'df', format='fixed', complevel=9, complib='zlib')

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

    def get_local_datetime(self, df):
        """Short summary.

        Parameters
        ----------
        df : type
            Description of parameter `df`.

        Returns
        -------
        type
            Description of returned object.

        """
        import pytz
        from numpy import unique
        from tzwhere import tzwhere
        tz = tzwhere.tzwhere(forceTZ=True, shapely=True)
        df.dropna(subset=['Latitude', 'Longitude'], inplace=True)
        lons, index = unique(df.Longitude.values, return_index=True)
        lats = df.Latitude.values[index]
        dates = df.datetime.values[index].astype('M8[s]').astype('O')
        df['utcoffset'] = 0
        for i, j, d in zip(lons, lats, dates):
            l = tz.tzNameAt(j, i, forceTZ=True)
            timezone = pytz.timezone(l)
            n = d.replace(tzinfo=pytz.UTC)
            r = d.replace(tzinfo=timezone)
            rdst = timezone.normalize(r)
            offset = (rdst.utcoffset()).total_seconds() // 3600
            df['utcoffset'].loc[df.Longitude == i] = offset

        df['datetime_local'] = df.datetime + pd.to_timedelta(df.utcoffset, 'H')
        return df
