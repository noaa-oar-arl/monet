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
        self.renamedhcols = ['time_local', 'time', 'state_code', 'county_code',
                             'site_num', 'parameter_code', 'poc', 'latitude', 'longitude',
                             'datum', 'parameter_name', 'obs', 'units',
                             'mdl', 'uncertainty', 'qualifier', 'method_type', 'method_code',
                             'method_name', 'state_name', 'county_name', 'date_of_last_change']
        self.renameddcols = ['time_local', 'state_code', 'county_code', 'site_num',
                             'parameter_code', 'poc', 'latitude', 'longitude', 'datum',
                             'parameter_name', 'sample_duration', 'pollutant_standard',
                             'units', 'event_type', 'observation_Count',
                             'observation_Percent', 'obs', '1st_max_Value',
                             '1st_max_hour', 'aqi', 'method_code', 'method_name',
                             'local_site_name', 'address', 'state_name', 'county_name',
                             'city_name', 'msa_name', 'date_of_last_change']
        self.savecols = ['time_local', 'time', 'siteid',
                         'latitude', 'longitude', 'obs', 'units', 'variable']
        self.df = None  # hourly dataframe
        self.monitor_file = inspect.getfile(
            self.__class__)[:-13] + '/data/monitoring_site_locations.dat'
        self.monitor_df = None
        self.daily = False
        self.d_df = None  # daily dataframe

    def load_aqs_file(self, url, network):
        if 'daily' in url:
            def dateparse(x): return pd.datetime.strptime(x, '%Y-%m-%d')
            df = pd.read_csv(url, parse_dates={'time_local': ["Date Local"]},
                             date_parser=dateparse)
            df.columns = self.renameddcols
            df['pollutant_standard'] = df.pollutant_standard.astype(str)
            self.daily = True
        else:
            df = pd.read_csv(url, parse_dates={'time': ['Date GMT', 'Time GMT'],
                                               'time_local': ["Date Local", "Time Local"]},
                             infer_datetime_format=True)
            df.columns = self.renamedhcols

        df.loc[:, 'state_code'] = pd.to_numeric(df.state_code, errors='coerce')
        df.loc[:, 'site_num'] = pd.to_numeric(df.site_num, errors='coerce')
        df.loc[:, 'county_code'] = pd.to_numeric(
            df.county_code, errors='coerce')
        df['siteid'] = array(df['state_code'].values * 1.E7 + df['county_code'].values * 1.E4 + df['site_num'].values,
                             dtype='int32')
        df.drop(['state_name', 'county_name'], axis=1, inplace=True)
        df.columns = [i.lower() for i in df.columns]
        if 'daily' not in url:
            df.drop(['datum', 'qualifier'], axis=1, inplace=True)
        df = self.get_species(df)
        return df

    def build_url(self, param, year, daily=False, download=False):
        if daily:
            beginning = self.baseurl + 'daily_'
            fname = 'daily_'
        else:
            beginning = self.baseurl + 'hourly_'
            fname = 'hourly_'
        if param.upper() == 'OZONE':
            code = '44201_'
        elif param.upper() == 'PM2.5':
            code = '88101_'
        elif param.upper() == 'PM2.5_FRM':
            code = '88502_'
        elif param.upper() == 'PM10':
            code = '88101_'
        elif param.upper() == 'SO2':
            code = '42401_'
        elif param.upper() == 'NO2':
            code = '42602_'
        elif param.upper() == 'CO':
            code = '42101_'
        elif param.upper() == 'NONOxNOy'.upper():
            code = 'NONOxNOy_'
        elif param.upper() == 'VOC':
            code = 'VOCS_'  # https://aqs.epa.gov/aqsweb/airdata/daily_VOCS_2017.zip
        elif param.upper() == 'SPEC':
            code = 'SPEC_'
        elif param.upper() == 'WIND':
            code = 'WIND_'
        elif param.upper() == 'TEMP':
            code = 'TEMP_'
        elif param.upper() == 'RHDP':
            code = 'RH_DP_'
        elif param.upper() == 'WIND':
            code = 'WIND_'
        url = beginning + code + year + '.zip'
        fname = fname + code + year + '.zip'
        return url, fname

    def build_urls(self, params, dates, daily=False):
        years = pd.DatetimeIndex(dates).year.unique().astype(str)
        urls = []
        fnames = []
        for i in params:
            for y in years:
                url, fname = self.build_url(i, y, daily=daily)
                urls.append(url)
                fnames.append(fname)
        return urls, fnames

    def retrieve(self, url, fname):
        """rdate - datetime object. Uses year and month. Day and hour are not used.
           state - state abbreviation to retrieve data for
           Files are by year month and state.
        """
        import wget

        if not os.path.isfile(fname):
            print('Retrieving: ' + fname)
            print (url)
            print('\n')
            wget.download(url)
        else:
            print('File Exists: ' + fname)

    def add_data(self, dates, param=None, daily=False, network=None, download=False):
        import dask
        import dask.dataframe as dd
        if param is None:
            params = ['SPEC', 'PM10', 'PM2.5', 'PM2.5_FRM', 'CO', 'OZONE', 'SO2', 'VOC', 'NONOXNOY', 'WIND', 'TEMP', 'RHDP']
        else:
            params = param
        urls, fnames = self.build_urls(params, dates, daily=daily)
        if download:
            for url, fname in zip(urls, fnames):
                self.retrieve(url, fname)
            dfs = [dask.delayed(self.load_aqs_file)(i, network) for i in fnames]
        else:
            dfs = [dask.delayed(self.load_aqs_file)(i, network) for i in urls]
        dff = dd.from_delayed(dfs)
        self.df = dff.compute()
        self.df = self.change_units(self.df)
        if self.monitor_df is None:
            self.monitor_df = read_monitor_file()
        if daily:
            monitor_drop = ['msa_name', 'city_name', u'local_site_name', u'address', 'datum']
            self.monitor_df.drop(monitor_drop, axis=1, inplace=True)
        else:
            monitor_drop = [u'datum']
            self.monitor_df.drop(monitor_drop, axis=1, inplace=True)
        if network is not None:
            monitors = self.monitor_df.loc[self.monitor_df.isin([network])].drop_duplicates(subset=['siteid'])
        else:
            monitors = self.monitor_df.drop_duplicates(subset=['siteid', 'latitude', 'longitude'])
        self.df = pd.merge(self.df, monitors, how='left', on=['siteid', 'latitude', 'longitude'])
        if daily:
            self.df['time'] = self.df.time_local - pd.to_timedelta(self.df.gmt_offset, unit='H')

    def get_species(self, df, voc=False):
        pc = df.parameter_code.unique()
        df['variable'] = ''
        if voc:
            df['variable'] = df.parameter_name.str.upper()
            return df
        for i in pc:
            con = df.parameter_code == i
            if (i == 88101) | (i == 88502):
                df.loc[con, 'variable'] = 'PM2.5'
            if i == 44201:
                df.loc[con, 'variable'] = 'OZONE'
            if i == 81102:
                df.loc[con, 'variable'] = 'PM10'
            if i == 42401:
                df.loc[con, 'variable'] = 'SO2'
            if i == 42602:
                df.loc[con, 'variable'] = 'NO2'
            if i == 42101:
                df.loc[con, 'variable'] = 'CO'
            if i == 62101:
                df.loc[con, 'variable'] = 'TEMP'
            if i == 88305:
                df.loc[con, 'variable'] = 'OC'
            if i == 88306:
                df.loc[con, 'variable'] = 'NO3f'
            if (i == 88307):
                df.loc[con, 'variable'] = 'ECf'
            if i == 88316:
                df.loc[con, 'variable'] = 'ECf_optical'
            if i == 88403:
                df.loc[con, 'variable'] = 'SO4f'
            if i == 88312:
                df.loc[con, 'variable'] = 'TCf'
            if i == 88104:
                df.loc[con, 'variable'] = 'Alf'
            if i == 88107:
                df.loc[con, 'variable'] = 'Baf'
            if i == 88313:
                df.loc[con, 'variable'] = 'BCf'
            if i == 88109:
                df.loc[con, 'variable'] = 'Brf'
            if i == 88110:
                df.loc[con, 'variable'] = 'Cdf'
            if i == 88111:
                df.loc[con, 'variable'] = 'Caf'
            if i == 88117:
                df.loc[con, 'variable'] = 'Cef'
            if i == 88118:
                df.loc[con, 'variable'] = 'Csf'
            if i == 88203:
                df.loc[con, 'variable'] = 'Cl-f'
            if i == 88115:
                df.loc[con, 'variable'] = 'Clf'
            if i == 88112:
                df.loc[con, 'variable'] = 'Crf'
            if i == 88113:
                df.loc[con, 'variable'] = 'Cof'
            if i == 88114:
                df.loc[con, 'variable'] = 'Cuf'
            if i == 88121:
                df.loc[con, 'variable'] = 'Euf'
            if i == 88143:
                df.loc[con, 'variable'] = 'Auf'
            if i == 88127:
                df.loc[con, 'variable'] = 'Hff'
            if i == 88131:
                df.loc[con, 'variable'] = 'Inf'
            if i == 88126:
                df.loc[con, 'variable'] = 'Fef'
            if i == 88146:
                df.loc[con, 'variable'] = 'Laf'
            if i == 88128:
                df.loc[con, 'variable'] = 'Pbf'
            if i == 88140:
                df.loc[con, 'variable'] = 'Mgf'
            if i == 88132:
                df.loc[con, 'variable'] = 'Mnf'
            if i == 88142:
                df.loc[con, 'variable'] = 'Hgf'
            if i == 88134:
                df.loc[con, 'variable'] = 'Mof'
            if i == 88136:
                df.loc[con, 'variable'] = 'Nif'
            if i == 88147:
                df.loc[con, 'variable'] = 'Nbf'
            if i == 88310:
                df.loc[con, 'variable'] = 'NO3f'
            if i == 88152:
                df.loc[con, 'variable'] = 'Pf'
            if i == 88303:
                df.loc[con, 'variable'] = 'K+f'
            if i == 88176:
                df.loc[con, 'variable'] = 'Rbf'
            if i == 88162:
                df.loc[con, 'variable'] = 'Smf'
            if i == 88163:
                df.loc[con, 'variable'] = 'Scf'
            if i == 88154:
                df.loc[con, 'variable'] = 'Sef'
            if i == 88165:
                df.loc[con, 'variable'] = 'Sif'
            if i == 88166:
                df.loc[con, 'variable'] = 'Agf'
            if i == 88302:
                df.loc[con, 'variable'] = 'Na+f'
            if i == 88184:
                df.loc[con, 'variable'] = 'Naf'
            if i == 88168:
                df.loc[con, 'variable'] = 'Srf'
            if i == 88403:
                df.loc[con, 'variable'] = 'SO4f'
            if i == 88169:
                df.loc[con, 'variable'] = 'Sf'
            if i == 88170:
                df.loc[con, 'variable'] = 'Taf'
            if i == 88172:
                df.loc[con, 'variable'] = 'Tbf'
            if i == 88160:
                df.loc[con, 'variable'] = 'Snf'
            if i == 88161:
                df.loc[con, 'variable'] = 'Tif'
            if i == 88312:
                df.loc[con, 'variable'] = 'TOT_Cf'
            if i == 88310:
                df.loc[con, 'variable'] = 'NON-VOLITILE_NO3f'
            if i == 88309:
                df.loc[con, 'variable'] = 'VOLITILE_NO3f'
            if i == 88186:
                df.loc[con, 'variable'] = 'Wf'
            if i == 88314:
                df.loc[con, 'variable'] = 'C_370nmf'
            if i == 88179:
                df.loc[con, 'variable'] = 'Uf'
            if i == 88164:
                df.loc[con, 'variable'] = 'Vf'
            if i == 88183:
                df.loc[con, 'variable'] = 'Yf'
            if i == 88167:
                df.loc[con, 'variable'] = 'Znf'
            if i == 88185:
                df.loc[con, 'variable'] = 'Zrf'
            if i == 88102:
                df.loc[con, 'variable'] = 'Sbf'
            if i == 88103:
                df.loc[con, 'variable'] = 'Asf'
            if i == 88105:
                df.loc[con, 'variable'] = 'Bef'
            if i == 88124:
                df.loc[con, 'variable'] = 'Gaf'
            if i == 88185:
                df.loc[con, 'variable'] = 'Irf'
            if i == 88180:
                df.loc[con, 'variable'] = 'Kf'
            if i == 88301:
                df.loc[con, 'variable'] = 'NH4+f'
            if (i == 88320) | (i == 88355):
                df.loc[con, 'variable'] = 'OCf'
            if (i == 88357) | (i == 88321):
                df.loc[con, 'variable'] = 'ECf'
            if i == 42600:
                df.loc[con, 'variable'] = 'NOY'
            if i == 42601:
                df.loc[con, 'variable'] = 'NO'
            if i == 42603:
                df.loc[con, 'variable'] = 'NOX'
            if (i == 61103) | (i == 61101):
                df.loc[con, 'variable'] = 'WS'
            if (i == 61104) | (i == 61102):
                df.loc[con, 'variable'] = 'WD'
            if i == 62201:
                df.loc[con, 'variable'] = 'RH'
            if i == 62103:
                df.loc[con, 'variable'] = 'DP'
        return df

    @staticmethod
    def change_units(df):
        units = df.units.unique()
        for i in units:
            con = df.units == i
            if i.upper() == 'Parts per billion Carbon'.upper():
                df.loc[con, 'units'] = 'ppbC'
            if i == 'Parts per billion':
                df.loc[con, 'units'] = 'ppb'
            if i == 'Parts per million':
                df.loc[con, 'units'] = 'ppm'
            if i == 'Micrograms/cubic meter (25 C)':
                df.loc[con, 'units'] = 'UG/M3'.lower()
            if i == 'Degrees Centigrade':
                df.loc[con, 'units'] = 'C'
            if i == 'Micrograms/cubic meter (LC)':
                df.loc[con, 'units'] = 'UG/M3'.lower()
            if i == 'Knots':
                df.loc[con, 'obs'] *= 0.51444
                df.loc[con, 'units'] = 'M/S'.lower()
            if i == 'Degrees Fahrenheit':
                df.loc[con, 'obs'] = (df.loc[con, 'obs'] + 459.67) * 5. / 9.
                df.loc[con, 'units'] = 'K'
            if i == 'Percent relative humidity':
                df.loc[con, 'units'] = '%'
        return df
