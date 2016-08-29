# this is written to retrive airnow data concatenate and add to pandas array for usage
from datetime import datetime

import pandas as pd
from numpy import array


class improve:
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
        """
        This assumes that you have downloaded the data from 
        http://views.cira.colostate.edu/fed/DataWizard/Default.aspx
        
        The data is the IMPROVE Aerosol dataset
        Any number of sites
        Parameters included are All
        Fields include Dataset,Site,Date,Parameter,POC,Data_value,Unit,Latitude,Longitude,State,EPA Site Code
        Options are delimited ','  data only and normalized skinny format
        """
        
        self.df = pd.read_csv(fname, delimiter=',', parse_dates=[2], infer_datetime_format=True)
        self.df.rename(columns={'EPACode': 'SCS'}, inplace=True)
        self.df.rename(columns={'Value': 'Obs'}, inplace=True)
        self.df.rename(columns={'State': 'State_Name'}, inplace=True)
        self.df.rename(columns={'ParamCode': 'Species'}, inplace=True)
        print 'Adding in some Meta-Data'
        self.get_region()
        self.df = self.df.copy().drop_duplicates()
        self.df.dropna(subset=['Species'],inplace=True)
        self.df.Species[self.df.Species == 'MT'] = 'PM10'
        self.df.Species[self.df.Species == 'MF'] = 'PM2.5'
        if output == '':
            output = 'IMPROVE.hdf'
        print 'Outputing data to: ' + output
        self.df.to_hdf(output, 'df', format='fixed')

        
    def get_date_range(self, dates):
        self.dates = dates
        con = (self.df.datetime >= dates[0]) & (self.df.datetime <= dates[-1])
        self.df = self.df.loc[con]

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end, freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates

    def get_region(self):
        sr = self.df.State_Name.copy().values
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
        sr[sr == 'CC'] = 'Canada'
        sr[sr == 'MX'] = 'Mexico'
        self.df['Region'] = array(sr)
        
