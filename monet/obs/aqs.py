from __future__ import print_function

import inspect
import os
# this is a class to deal with aqs data
from builtins import object, range, zip
from datetime import datetime
from zipfile import ZipFile

import dask
import dask.dataframe as dd
import pandas as pd
import requests
from numpy import arange, array

from .epa_util import read_monitor_file

pbar = dask.diagnostics.ProgressBar()
pbar.register()


class AQS(object):
    def __init__(self):
        #        self.baseurl = 'https://aqs.epa.gov/aqsweb/airdata/'
        self.objtype = 'AQS'
        self.baseurl = 'https://aqs.epa.gov/aqsweb/airdata/'
        self.dates = [datetime.strptime('2014-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2014-06-06 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.renamedhcols = ['datetime_local', 'datetime', 'State_Code', 'County_Code',
                             'Site_Num', 'Parameter_Code', 'POC', 'Latitude', 'Longitude',
                             'Datum', 'Parameter_Name', 'Obs', 'Units',
                             'MDL', 'Uncertainty', 'Qualifier', 'Method_type', 'Method_Code',
                             'Method_Name', 'State_Name', 'County_Name', 'Date_of_Last_Change']
        self.renameddcols = ['datetime_local', 'State_Code', 'County_Code', 'Site_Num',
                             'Parameter_Code', 'POC', 'Latitude', 'Longitude', 'Datum',
                             'Parameter_Name', 'Sample_Duration', 'Pollutant_Standard',
                             'Units', 'Event_Type', 'Observation_Count',
                             'Observation_Percent', 'Obs', '1st_Max_Value',
                             '1st_Max Hour', 'AQI', 'Method_Code', 'Method_Name',
                             'Local_Site_Name', 'Address', 'State_Name', 'County_Name',
                             'City_Name', 'MSA_Name', 'Date_of_Last_Change']
        self.savecols = ['datetime_local', 'datetime', 'SCS',
                         'Latitude', 'Longitude', 'Obs', 'Units', 'Species']
        self.df = None  # hourly dataframe
        self.monitor_file = inspect.getfile(
            self.__class__)[:-13] + '/data/monitoring_site_locations.dat'
        self.monitor_df = None
        self.d_df = None  # daily dataframe

    def load_aqs_file(self, url, network):
        if 'daily' in url:
            def dateparse(x): return pd.datetime.strptime(x, '%Y-%m-%d')
            df = pd.read_csv(url, parse_dates={'datetime_local': ["Date Local"]},
                             date_parser=dateparse)
            df.columns = self.renameddcols
            df['Pollutant_Standard'] = df.Pollutant_Standard.astype(str)
        else:
            df = pd.read_csv(url, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                               'datetime_local': ["Date Local", "Time Local"]},
                             infer_datetime_format=True)
            df.columns = self.renamedhcols

        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(
            df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop(['State_Name', 'County_Name'], axis=1, inplace=True)
        if 'daily' not in url:
            df.drop(['Datum', 'Qualifier'], axis=1, inplace=True)
        df = self.get_species(df)
        return df

    def build_url(self, param, year, daily=False):
        if daily:
            beginning = self.baseurl + 'daily_'
        else:
            beginning = self.baseurl + 'hourly_'
        if param == 'OZONE':
            code = '44201_'
        elif param == 'PM2.5':
            code = '88101_'
        elif param == 'PM2.5_FRM':
            code = '88502_'
        elif param == 'PM10':
            code = '88101_'
        elif param == 'SO2':
            code = '42401_'
        elif param == 'NO2':
            code = '42602_'
        elif param == 'CO':
            code = '42101_'
        elif param == 'NONOxNOy':
            code = 'NONOxNOy_'
        elif param == 'VOC':
            code = 'VOCS_'
        elif param == 'SPEC':
            code = 'SPEC_'
        elif param == 'WIND':
            code = 'WIND_'
        elif param == 'TEMP':
            code = 'TEMP_'
        elif param == 'RH':
            code = 'RH_DP_'
        elif param == 'WIND':
            code = 'WIND_'
        url = beginning + code + year + '.zip'
        return url

    def build_urls(self, params, dates, daily=False):
        years = pd.DatetimeIndex(dates).year.unique().astype(str)
        urls = []
        for i in params:
            for y in years:
                urls.append(self.build_url(i, y, daily=daily))
        return urls

    def add_data(self, dates, param=None, daily=False, network=None):
        import dask
        import dask.dataframe as dd

        if param is None:
            params = ['SPEC', 'PM10', 'PM2.5', 'PM2.5_FRM', 'CO', 'OZONE', 'SO2', 'VOC', 'NONOXNOY', 'WIND', 'TEMP', 'RHDP']
        else:
            params = param
        urls = self.build_urls(params, dates, daily=daily)
        dfs = [dask.delayed(self.load_aqs_file)(i, network) for i in urls]
        dff = dd.from_delayed(dfs)
        self.df = dff.compute()
        self.df = self.change_units(self.df)
        if self.monitor_df is None:
            self.monitor_df = read_monitor_file()
        if daily:
            monitor_drop = ['MSA_Name', 'City_Name', u'Local_Site_Name', u'Address', 'Datum']
            self.monitor_df.drop(monitor_drop, axis=1, inplace=True)
        else:
            monitor_drop = [u'Datum']
            self.monitor_df.drop(monitor_drop, axis=1, inplace=True)
        if network is not None:
            monitors = self.monitor_df.loc[self.monitor_df.isin([network])].drop_duplicates(subset=['SCS'])
        else:
            monitors = self.monitor_df.drop_duplicates(subset=['SCS', 'Latitude', 'Longitude'])
        self.df = pd.merge(self.df, monitors, how='left', on=['SCS', 'Latitude', 'Longitude'])
        if daily:
            self.df['datetime'] = self.df.datetime_local - pd.to_timedelta(self.df.GMT_Offset, unit='H')

    def get_species(self, df, voc=False):
        pc = df.Parameter_Code.unique()
        df['Species'] = ''
        if voc:
            df['Species'] = df.Parameter_Name.str.upper()
            return df
        for i in pc:
            con = df.Parameter_Code == i
            if (i == 88101) | (i == 88502):
                df.loc[con, 'Species'] = 'PM2.5'
            if i == 44201:
                df.loc[con, 'Species'] = 'OZONE'
            if i == 81102:
                df.loc[con, 'Species'] = 'PM10'
            if i == 42401:
                df.loc[con, 'Species'] = 'SO2'
            if i == 42602:
                df.loc[con, 'Species'] = 'NO2'
            if i == 42101:
                df.loc[con, 'Species'] = 'CO'
            if i == 62101:
                df.loc[con, 'Species'] = 'TEMP'
            if i == 88305:
                df.loc[con, 'Species'] = 'OC'
            if i == 88306:
                df.loc[con, 'Species'] = 'NO3f'
            if (i == 88307):
                df.loc[con, 'Species'] = 'ECf'
            if i == 88316:
                df.loc[con, 'Species'] = 'ECf_optical'
            if i == 88403:
                df.loc[con, 'Species'] = 'SO4f'
            if i == 88312:
                df.loc[con, 'Species'] = 'TCf'
            if i == 88104:
                df.loc[con, 'Species'] = 'Alf'
            if i == 88107:
                df.loc[con, 'Species'] = 'Baf'
            if i == 88313:
                df.loc[con, 'Species'] = 'BCf'
            if i == 88109:
                df.loc[con, 'Species'] = 'Brf'
            if i == 88110:
                df.loc[con, 'Species'] = 'Cdf'
            if i == 88111:
                df.loc[con, 'Species'] = 'Caf'
            if i == 88117:
                df.loc[con, 'Species'] = 'Cef'
            if i == 88118:
                df.loc[con, 'Species'] = 'Csf'
            if i == 88203:
                df.loc[con, 'Species'] = 'Cl-f'
            if i == 88115:
                df.loc[con, 'Species'] = 'Clf'
            if i == 88112:
                df.loc[con, 'Species'] = 'Crf'
            if i == 88113:
                df.loc[con, 'Species'] = 'Cof'
            if i == 88114:
                df.loc[con, 'Species'] = 'Cuf'
            if i == 88121:
                df.loc[con, 'Species'] = 'Euf'
            if i == 88143:
                df.loc[con, 'Species'] = 'Auf'
            if i == 88127:
                df.loc[con, 'Species'] = 'Hff'
            if i == 88131:
                df.loc[con, 'Species'] = 'Inf'
            if i == 88126:
                df.loc[con, 'Species'] = 'Fef'
            if i == 88146:
                df.loc[con, 'Species'] = 'Laf'
            if i == 88128:
                df.loc[con, 'Species'] = 'Pbf'
            if i == 88140:
                df.loc[con, 'Species'] = 'Mgf'
            if i == 88132:
                df.loc[con, 'Species'] = 'Mnf'
            if i == 88142:
                df.loc[con, 'Species'] = 'Hgf'
            if i == 88134:
                df.loc[con, 'Species'] = 'Mof'
            if i == 88136:
                df.loc[con, 'Species'] = 'Nif'
            if i == 88147:
                df.loc[con, 'Species'] = 'Nbf'
            if i == 88310:
                df.loc[con, 'Species'] = 'NO3f'
            if i == 88152:
                df.loc[con, 'Species'] = 'Pf'
            if i == 88303:
                df.loc[con, 'Species'] = 'K+f'
            if i == 88176:
                df.loc[con, 'Species'] = 'Rbf'
            if i == 88162:
                df.loc[con, 'Species'] = 'Smf'
            if i == 88163:
                df.loc[con, 'Species'] = 'Scf'
            if i == 88154:
                df.loc[con, 'Species'] = 'Sef'
            if i == 88165:
                df.loc[con, 'Species'] = 'Sif'
            if i == 88166:
                df.loc[con, 'Species'] = 'Agf'
            if i == 88302:
                df.loc[con, 'Species'] = 'Na+f'
            if i == 88184:
                df.loc[con, 'Species'] = 'Naf'
            if i == 88168:
                df.loc[con, 'Species'] = 'Srf'
            if i == 88403:
                df.loc[con, 'Species'] = 'SO4f'
            if i == 88169:
                df.loc[con, 'Species'] = 'Sf'
            if i == 88170:
                df.loc[con, 'Species'] = 'Taf'
            if i == 88172:
                df.loc[con, 'Species'] = 'Tbf'
            if i == 88160:
                df.loc[con, 'Species'] = 'Snf'
            if i == 88161:
                df.loc[con, 'Species'] = 'Tif'
            if i == 88312:
                df.loc[con, 'Species'] = 'TOT_Cf'
            if i == 88310:
                df.loc[con, 'Species'] = 'NON-VOLITILE_NO3f'
            if i == 88309:
                df.loc[con, 'Species'] = 'VOLITILE_NO3f'
            if i == 88186:
                df.loc[con, 'Species'] = 'Wf'
            if i == 88314:
                df.loc[con, 'Species'] = 'C_370nmf'
            if i == 88179:
                df.loc[con, 'Species'] = 'Uf'
            if i == 88164:
                df.loc[con, 'Species'] = 'Vf'
            if i == 88183:
                df.loc[con, 'Species'] = 'Yf'
            if i == 88167:
                df.loc[con, 'Species'] = 'Znf'
            if i == 88185:
                df.loc[con, 'Species'] = 'Zrf'
            if i == 88102:
                df.loc[con, 'Species'] = 'Sbf'
            if i == 88103:
                df.loc[con, 'Species'] = 'Asf'
            if i == 88105:
                df.loc[con, 'Species'] = 'Bef'
            if i == 88124:
                df.loc[con, 'Species'] = 'Gaf'
            if i == 88185:
                df.loc[con, 'Species'] = 'Irf'
            if i == 88180:
                df.loc[con, 'Species'] = 'Kf'
            if i == 88301:
                df.loc[con, 'Species'] = 'NH4+f'
            if (i == 88320) | (i == 88355):
                df.loc[con, 'Species'] = 'OCf'
            if (i == 88357) | (i == 88321):
                df.loc[con, 'Species'] = 'ECf'
            if i == 42600:
                df.loc[con, 'Species'] = 'NOY'
            if i == 42601:
                df.loc[con, 'Species'] = 'NO'
            if i == 42603:
                df.loc[con, 'Species'] = 'NOX'
            if (i == 61103) | (i == 61101):
                df.loc[con, 'Species'] = 'WS'
            if (i == 61104) | (i == 61102):
                df.loc[con, 'Species'] = 'WD'
            if i == 62201:
                df.loc[con, 'Species'] = 'RH'
            if i == 62103:
                df.loc[con, 'Species'] = 'DP'
        return df

    @staticmethod
    def change_units(df):
        units = df.Units.unique()
        for i in units:
            con = df.Units == i
            if i.upper() == 'Parts per billion Carbon'.upper():
                df.loc[con, 'Units'] = 'ppbC'
            if i == 'Parts per billion':
                df.loc[con, 'Units'] = 'ppb'
            if i == 'Parts per million':
                df.loc[con, 'Units'] = 'ppm'
            if i == 'Micrograms/cubic meter (25 C)':
                df.loc[con, 'Units'] = 'UG/M3'.lower()
            if i == 'Degrees Centigrade':
                df.loc[con, 'Units'] = 'C'
            if i == 'Micrograms/cubic meter (LC)':
                df.loc[con, 'Units'] = 'UG/M3'.lower()
            if i == 'Knots':
                df.loc[con, 'Obs'] *= 0.51444
                df.loc[con, 'Units'] = 'M/S'.lower()
            if i == 'Degrees Fahrenheit':
                df.loc[con, 'Obs'] = (df.loc[con, 'Obs'] + 459.67) * 5. / 9.
                df.loc[con, 'Units'] = 'K'
            if i == 'Percent relative humidity':
                df.loc[con, 'Units'] = '%'
        return df
