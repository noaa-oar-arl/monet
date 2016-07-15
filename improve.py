# this is written to retrive airnow data concatenate and add to pandas array for usage
from datetime import datetime

import pandas as pd
from numpy import array


class improve:
    def __init__(self):
        self.datestr = []
        self.df = None
        self.se_states = array(['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TX', 'VA', 'WV'], dtype='|S2')
        self.ne_states = array(['CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'], dtype='|S2')
        self.nc_states = array(['IL', 'IN', 'IA', 'KY', 'MI', 'MN', 'MO', 'OH', 'WI'], dtype='|S2')
        self.sc_states = array(['AR', 'LA', 'OK', 'TX'], dtype='|S2')
        self.r_states = array(['AZ', 'CO', 'ID', 'KS', 'MT', 'NE', 'NV', 'NM', 'ND', 'SD', 'UT', 'WY'], dtype='|S2')
        self.p_states = array(['CA', 'OR', 'WA'], dtype='|S2')

    def open_file(self, fname, output=''):
        from StringIO import StringIO

        print 'Reading File: ' + fname
        with open(fname, 'rb') as f:
            data = f.read().split('\n\n')
        df = pd.read_csv(StringIO(data[-2]), delimiter=',', parse_dates=[2], infer_datetime_format=True,na_values=-999)
        df.rename(columns={'EPACode': 'SCS'}, inplace=True)
        obs = pd.Series(dtype=df['Vf:Value'].dtype)
        lat = pd.Series(dtype=df.Latitude.dtype)
        lon = pd.Series(dtype=df.Longitude.dtype)
        date = pd.Series(dtype=df.Date.dtype)
        sitecode = pd.Series(dtype=df.SiteCode.dtype)
        scs = pd.Series(dtype=df.SCS.dtype)
        units = pd.Series(dtype=df['Vf:Unit'].dtype)
        state = pd.Series(dtype=df.State.dtype)
        species = pd.Series(dtype='O')
        sitename = pd.Series(dtype=df.SiteName.dtype)
        for i in df.columns:
            if 'Value' in i:
                r = i.split(':')
                obs = obs.append(df[i]).reset_index(drop=True)
                lat = lat.append(df.Latitude).reset_index(drop=True)
                lon = lon.append(df.Longitude).reset_index(drop=True)
                date = date.append(df.Date).reset_index(drop=True)
                sitecode = sitecode.append(df.SiteCode).reset_index(drop=True)
                scs = scs.append(df.SCS).reset_index(drop=True)
                units = units.append(df[r[0] + ':Unit']).reset_index(drop=True)
                state = state.append(df.State).reset_index(drop=True)
                species = species.append(pd.Series([r[0] for i in range(df.Latitude.count())])).reset_index(drop=True)
                sitename = sitename.append(df.SiteName).reset_index(drop=True)
        self.df = pd.concat([obs, lat, lon, date, sitecode, scs, units, state, species, sitename], axis=1,
                            keys=['Obs', 'Latitude', 'Longitude', 'datetime', 'Site_Code', 'SCS', 'Units', 'State_Name',
                                  'Species', 'Site_Name'])
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
