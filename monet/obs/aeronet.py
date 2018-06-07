from __future__ import division, print_function

# this is written to retrive airnow data concatenate and add to pandas array for usage
from builtins import object, str
from datetime import datetime

import pandas as pd
from past.utils import old_div


def dateparse(x):
    return pd.datetime.strptime(x, '%d:%m:%Y %H:%M:%S')


class AERONET(object):
    def __init__(self):
        from numpy import concatenate, arange
        self.baseurl = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?'
        self.dates = [datetime.strptime('2016-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2016-06-10 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.datestr = []
        self.df = pd.DataFrame()
        self.daily = None
        self.prod = None
        self.inv_type = None
        self.objtype = 'AERONET'
        self.usecols = concatenate((arange(30), arange(65, 83)))
        self.latlonbox = None  # [21.1,-131.6686,53.04,-58.775] #[latmin,lonmin,latmax,lonmax]
        self.station_df = None
        self.colnames = ['time', 'siteid', 'Day_of_Year', 'Day_of_Year(Fraction)', 'AOD_1640nm',
                         'AOD_1020nm', 'AOD_870nm', 'AOD_865nm', 'AOD_779nm', 'AOD_675nm', 'AOD_667nm', 'AOD_620nm',
                         'AOD_560nm', 'AOD_555nm', 'AOD_551nm', 'AOD_532nm', 'AOD_531nm', 'AOD_510nm', 'AOD_500nm',
                         'AOD_490nm', 'AOD_443nm', 'AOD_440nm', 'AOD_412nm', 'AOD_400nm', 'AOD_380nm', 'AOD_340nm',
                         'Precipitable_Water(cm)', 'AOD_681nm', 'AOD_709nm',
                         '440-870_Angstrom_Exponent', '380-500_Angstrom_Exponent', '440-675_Angstrom_Exponent',
                         '500-870_Angstrom_Exponent', '340-440_Angstrom_Exponent', '440-675_Angstrom_Exponent[Polar]',
                         'Data_Quality_Level', 'AERONET_Instrument_Number', 'AERONET_Site_Name', 'latitude',
                         'longitude', 'Elevation(m)', 'Solar_Zenith_Angle(Degrees)', 'Optical_Air_Mass',
                         'Sensor_Temperature(Degrees_C)', 'Ozone(Dobson)', 'NO2(Dobson)', 'Last_Date_Processed']
        self.url = None

    def build_url(self):
        sy = self.dates.min().strftime('%Y')
        sm = self.dates.min().strftime('%m').zfill(2)
        sd = self.dates.min().strftime('%d').zfill(2)
        sh = self.dates.min().strftime('%H').zfill(2)
        ey = self.dates.max().strftime('%Y').zfill(2)
        em = self.dates.max().strftime('%m').zfill(2)
        ed = self.dates.max().strftime('%d').zfill(2)
        eh = self.dates.max().strftime('%H').zfill(2)
        if self.prod in ['AOD10', 'AOD15', 'AOD20', 'SDA10', 'SDA15', 'SDA20', 'TOT10', 'TOT15', 'TOT20']:
            base_url = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?'
            inv_type = ''
        else:
            base_url = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_inv_v3?'
            if self.inv_type == 'ALM15':
                inv_type = '&ALM15=1'
            else:
                inv_type = '&AML20=1'
        date_portion = 'year=' + sy + '&month=' + sm + '&day=' + sd + '&hour=' + sh + '&year2=' + ey + '&month2=' + em + '&day2=' + ed + '&hour2=' + eh
        if self.inv_type is not '':
            product = '&product=' + self.prod
        else:
            product = '&' + self.prod + '=1'
        time = '&AVG=' + str(self.daily)
        if self.latlonbox is None:
            latlonbox = ''
        else:
            lat1 = str(self.latlonbox[0])
            lon1 = str(self.latlonbox[1])
            lat2 = str(self.latlonbox[2])
            lon2 = str(self.latlonbox[3])
            latlonbox = '&lat1=' + lat1 + '&lat2=' + lat2 + '&lon1=' + lon1 + '&lon2=' + lon2
        self.url = base_url + date_portion + product + inv_type + time + '&if_no_html=1'
        #
        # self.url = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?year=' + sy + '&month=' + sm + '&day=' + sd + \
        #            '&hour=' + sh + '&year2=' + ey + '&month2=' + em + '&day2=' + ed + '&hour2=' + eh + '&AOD15=1&AVG=10&if_no_html=1'
        #
        #
        # self.url = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?year=' + sy + '&month=' + sm + '&day=' + sd + '&hour=' + sh + '&year2=' + ey + \
        #            '&month2=' + em + '&day2=' + ed + '&hour2=' + eh + '&lat1=' + lat1 + '&lat2=' + lat2 + '&lon1=' + lon1 + '&lon2=' + lon2 + '&AOD15=1&AVG=10&if_no_html=1'

    def read_aeronet(self):
        print('Reading Aeronet Data...')
        # header = self.get_columns()
        df = pd.read_csv(self.url, engine='python', header=None, skiprows=6, parse_dates={'time': [1, 2]},
                         date_parser=dateparse, na_values=-999)
        #df.rename(columns={'date_time': 'time'}, inplace=True)
        columns = self.get_columns()
        df.columns = columns  # self.get_columns()
        df.index = df.time
        df.rename(columns={'site_latitude(degrees)': 'latitude', 'site_longitude(degrees)': 'longitude', 'site_elevation(m)': 'elevation', 'aeronet_site': 'siteid'}, inplace=True)
        df.dropna(subset=['latitude', 'longitude'], inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        #self.df = df.groupby('siteid').resample('H').mean().reset_index()
        # self.calc_550nm()
        self.df = df
        # df = pd.read_csv(self.url, engine='python', header=None, skiprows=6, parse_dates={'time': [1, 2]},
        #                  date_parser=dateparse, usecols=self.usecols, na_values=-999)
        # #df.rename(columns={'date_time': 'time'}, inplace=True)
        # df.columns = [i.lower() for i in self.colnames]
        # df.index = df.time
        # df.dropna(subset=['latitude', 'longitude'], inplace=True)
        # self.df = df.groupby('siteid').resample('H').mean().reset_index()
        # self.calc_550nm()
        # self.df.dropna(subset=['latitude', 'longitude'])

    def get_columns(self):
        header = pd.read_csv(self.url, skiprows=5, header=None, nrows=1).values.flatten()
        final = ['time']
        for i in header:
            if "Date(" in i or 'Time(' in i:
                pass
            else:
                final.append(i.lower())
        return final

    def add_data(self, dates=None, product='AOD15', latlonbox=None, daily=False, calc_550=True, inv_type=None, freq=None, detect_dust=False):
        self.latlonbox = latlonbox
        if dates is None:  # get the current day
            self.dates = pd.date_range(start=pd.to_datetime('today'), end=pd.to_datetime('now'), freq='H')
        else:
            self.dates = dates
        self.prod = product.upper()
        if daily:
            self.daily = 20  # daily data
        else:
            self.daily = 10  # all points
        if inv_type is None:
            self.inv_type = 'ALM15'
        else:
            self.inv_type = inv_type
        self.build_url()
        self.read_aeronet()
        if freq is not None:
            self.df = self.df.groupby('siteid').resample(freq).mean().reset_index()
        if detect_dust:
            self.dust_detect()
        if calc_550:
            self.calc_550nm()
        return self.df


    def calc_550nm(self):
        """Since AOD at 500nm is not calculated we use the extrapolation of
        V. Cesnulyte et al (ACP,2014) for the calculation

        aod550 = aod500 * (550/500) ^ -alpha
        """
        self.df['aod_550nm'] = self.df.aod_500nm * (old_div(550., 500.)) ** (-self.df['440-870_angstrom_exponent'])

    def dust_detect(self):
        """ [Dubovik et al., 2002]. AOD_1020 > 0.3 and AE(440,870) < 0.6"""
        self.df['dust'] = (self.df['aod_1020nm'] > 0.3) & (self.df['440-870_angstrom_exponent'] < 0.6)

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end, freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates
