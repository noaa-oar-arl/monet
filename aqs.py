# this is a class to deal with aqs data
import pandas as pd
from numpy import array, arange
from datetime import datetime


class aqs:
    def __init__(self):
        self.baseurl = 'http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/'
        self.dates = [datetime.strptime('2014-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2014-06-06 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.renamedhcols = ['datetime_local', 'datetime', 'State_Code', 'County_Code',
                             'Site_Num', 'Parameter_Code', 'POC', 'Latitude', 'Longitude',
                             'Datum', 'Parameter_Name', 'Obs_value', 'Units_of_Measure',
                             'MDL', 'Uncertainty', 'Qualifier', 'Method_type', 'Method_Code',
                             'Method_Name', 'State_Name', 'County_Name', 'Date_of_Last_Change']
        self.renameddcols = ['datetime_local', 'State_Code', 'County_Code', 'Site_Num',
                             'Parameter_Code', 'POC', 'Latitude', 'Longitude', 'Datum',
                             'Parameter_Name', 'Sample_Duration', 'Pollutant_Standard',
                             'Units_of_Measure', 'Event_Type', 'Observation_Count',
                             'Observation_Percent', 'Obs_value', '1st_Max_Value',
                             '1st_Max Hour', 'AQI', 'Method_Code', 'Method_Name',
                             'Local_Site_Name', 'Address', 'State_Name', 'County_Name',
                             'City_Name', 'CBSA_Name', 'Date_of_Last_Change']
        self.datadir = '~/Downloads'
        self.aqsdf = None

    def retrieve_aqs_hourly_pm25_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_88101_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                    'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_PM_25_88101_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_hourly_ozone_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_44201_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                    'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_OZONE_44201_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_hourly_pm10_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_81102_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                    'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_PM_10_81102_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_hourly_so2_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_42401_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                    'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_SO2_42401_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_hourly_no2_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_42602_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                    'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_NO2_42602_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_hourly_co_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_42101_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                    'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_CO_42101_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_co_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42101_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_CO_42101_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_ozone_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_44201_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_OZONE_44201_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_pm10_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_81102_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_PM_10_81102_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_so2_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42401_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_SO2_42401_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_no2_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42602_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_NO2_42602_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_pm25_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_88101_' + year + '.zip'
        filename = wget.download(url)
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='gzip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_PM_25_88101_' + year + '.hdf', 'df', format='table')
        self.aqsdf = df.copy()

    def load_aqs_pm25_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_HOURLY_PM_25_88101_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_ozone_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + + '/' + 'AQS_HOURLY_OZONE_44201_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_pm10_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + self.datadir + '/' + 'AQS_HOURLY_PM_10_81102_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_so2_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + self.datadir + '/' + 'AQS_HOURLY_SO2_42401_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_no2_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + self.datadir + '/' + 'AQS_HOURLY_NO2_42602_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_co_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_HOURLY_CO_42101_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_pm25_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_DAILY_PM_25_88101_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_ozone_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_DAILY_OZONE_44201_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_pm10_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_DAILY_PM_10_81102_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_so2_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_DAILY_SO2_42401_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_no2_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_DAILY_NO2_42602_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_co_data(self, dates):
        year = dates[0].strftime('%Y_')
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_DAILY_CO_42101_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs
