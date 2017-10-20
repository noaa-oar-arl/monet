# this is written to retrive airnow data concatenate and add to pandas array for usage
import os
from datetime import datetime
import pandas as pd
import wget


class aeronet:
    def __init__(self):
        from numpy import concatenate,arange
        self.baseurl = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?'
        self.datadir = '.'
        self.cwd = os.getcwd()
        self.dates = [datetime.strptime('2016-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2016-06-10 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.datestr = []
        self.df = None
        self.objtype = 'AERONET'
        self.usecols = concatenate((arange(30),arange(65,83)))
        self.latlonbox = None #[21.1,-131.6686,53.04,-58.775] #[latmin,lonmin,latmax,lonmax]
        self.station_df = None
        self.colnames = ['Site_Name','date','time','Day_of_Year','Day_of_Year(Fraction)','AOD_1640nm','AOD_1020nm','AOD_870nm','AOD_865nm','AOD_779nm','AOD_675nm','AOD_667nm','AOD_620nm','AOD_560nm','AOD_555nm','AOD_551nm','AOD_532nm','AOD_531nm','AOD_510nm','AOD_500nm','AOD_490nm','AOD_443nm','AOD_440nm','AOD_412nm','AOD_400nm','AOD_380nm','AOD_340nm','Precipitable_Water(cm)','AOD_681nm','AOD_709nm','440-870_Angstrom_Exponent','380-500_Angstrom_Exponent','440-675_Angstrom_Exponent','500-870_Angstrom_Exponent','340-440_Angstrom_Exponent','440-675_Angstrom_Exponent[Polar]','Data_Quality_Level','AERONET_Instrument_Number','AERONET_Site_Name','Latitude','Longitude','Elevation(m)','Solar_Zenith_Angle(Degrees)','Optical_Air_Mass','Sensor_Temperature(Degrees_C)','Ozone(Dobson)','NO2(Dobson)','Last_Date_Processed']
        self.url = None

    def build_url(self):
        sy=self.dates.min().strftime('%Y')
        sm=self.dates.min().strftime('%m')
        sd=self.dates.min().strftime('%d')
        sh=self.dates.min().strftime('%H')
        ey=self.dates.max().strftime('%Y')
        em=self.dates.max().strftime('%m')
        ed=self.dates.max().strftime('%d')
        eh=self.dates.max().strftime('%H')
        if self.latlonbox == None:
            self.url = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?year='+sy+'&month='+sm+'&day='+sd+'&hour='+sh+'&year2='+ey+'&month2='+em+'&day2='+ed+'&hour2='+eh+'&AOD15=1&AVG=10&if_no_html=1'
        else:
            lat1 = str(self.latlonbox[0])
            lon1 = str(self.latlonbox[1])
            lat2 = str(self.latlonbox[2])
            lon2 = str(self.latlonbox[3])
            self.url = 'https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?year='+sy+'&month='+sm+'&day='+sd+'&hour='+sh+'&year2='+ey+'&month2='+em+'&day2='+ed+'&hour2='+eh+'&lat1='+lat1+'&lat2='+lat2+'&lon1='+lon1+'&lon2='+lon2+'&AOD15=1&AVG=10&if_no_html=1'

    def read_aeronet(self):
        import requests
        from io import StringIO
        print 'Reading Aeronet Data...'
        s = requests.get(self.url).text
        df = pd.read_csv(StringIO(s),usecols=self.usecols,names=self.colnames,parse_dates=[[1,2]],infer_datetime_format=True,header=None,sep=None,engine='python',na_values=-999,skiprows=6)
        df.rename(columns={'date_time':'datetime'},inplace=True)
        df.index = df.datetime
        df.dropna(subset=['Latitude','Longitude'],inplace=True)
        self.df = df.groupby('Site_Name').resample('H').mean().reset_index()
        self.calc_550nm()
            
    def calc_550nm(self):
        """Since AOD at 500nm is not calculated we use the extrapolation of 
        V. Cesnulyte et al (ACP,2014) for the calculation

        aod550 = aod500 * (550/500) ^ -alpha
        Following P.B. Russell et al. """ 
        self.df['AOD_550'] = self.df.AOD_500nm * (550./500.) ** (-self.df['440-870_Angstrom_Exponent'])

    def dust_detect(self):
        """ [Dubovik et al., 2002]. AOD_1020 > 0.3 and AE(440,870) < 0.6"""
        self.df['DUST'] = (self.df['AOD_1020nm'] > 0.3) & (self.df['440-870_Angstrom_Exponent'] < 0.6)

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end, freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates
