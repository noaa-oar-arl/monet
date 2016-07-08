# this is a class to deal with aqs data
import os
from datetime import datetime

import pandas as pd
from numpy import array, arange


class aqs:
    def __init__(self):
        self.baseurl = 'http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/'
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
                             'City_Name', 'CBSA_Name', 'Date_of_Last_Change']
        self.savecols = ['datetime_local', 'datetime', 'SCS', 'Latitude', 'Longitude',
                         'Parameter_Name', 'Obs', 'Units', 'State_Name', 'County_Name', 'Species', 'Region']
        self.se_states = array(
                ['Alabama', 'Florida', 'Georgia', 'Mississippi', 'North Carolina', 'South Carolina', 'Tennessee',
                 'Virginia', 'West Virginia'], dtype='|S14')
        self.ne_states = array(['Connecticut', 'Delaware', 'District Of Columbia', 'Maine', 'Maryland', 'Massachusetts',
                                'New Hampshire', 'New Jersey', 'New York', 'Pennsylvania', 'Rhode Island', 'Vermont'],
                               dtype='|S20')
        self.nc_states = array(
                ['Illinois', 'Indiana', 'Iowa', 'Kentucky', 'Michigan', 'Minnesota', 'Missouri', 'Ohio', 'Wisconsin'],
                dtype='|S9')
        self.sc_states = array(['Arkansas', 'Louisiana', 'Oklahoma', 'Texas'], dtype='|S9')
        self.r_states = array(['Arizona', 'Colorado', 'Idaho', 'Kansas', 'Montana', 'Nebraska', 'Nevada', 'New Mexico',
                               'North Dakota', 'South Dakota', 'Utah', 'Wyoming'], dtype='|S12')
        self.p_states = array(['California', 'Oregon', 'Washington'], dtype='|S10')
        self.datadir = '.'
        self.df = None

    def retrieve_aqs_hourly_pm25_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_88101_' + year + '.zip'

        print 'Downloading Hourly PM2.5: ' + url
        filename = wget.download(url, );
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')

        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_PM_25_88101_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_PM_25_88101_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_ozone_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_44201_' + year + '.zip'
        print 'Downloading Hourly Ozone: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_OZONE_44201_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_OZONE_44201_' + year + '.hdf', 'df', format='fixed')

        return df

    def retrieve_aqs_hourly_pm10_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_81102_' + year + '.zip'
        print 'Downloading Hourly PM10: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_PM_10_81102_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_PM_10_81102_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_so2_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_42401_' + year + '.zip'
        print 'Downloading Hourly SO2: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_SO2_42401_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_SO2_42401_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_no2_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_42602_' + year + '.zip'
        print 'Downloading Hourly NO2: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_NO2_42602_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_NO2_42602_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_co_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_42101_' + year + '.zip'
        print 'Downloading Hourly CO: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_CO_42101_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_CO_42101_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_nonoxnoy_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_NONOxNOy_' + year + '.zip'
        print 'Downloading Hourly NO NOx NOy: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_NONOXNOY_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_NONOXNOY_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_voc_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_VOC_' + year + '.zip'
        print 'Downloading Hourly VOC: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_VOC_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_VOC_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_SPEC_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_SPEC_' + year + '.zip'
        print 'Downloading CSN Speciation: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                                   'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_SPEC_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_HOURLY_SPEC_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_daily_co_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42101_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_CO_42101_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_CO_42101_' + year + '.hdf', 'df', format='fixed')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_ozone_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_44201_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_OZONE_44201_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_OZONE_44201_' + year + '.hdf', 'df', format='fixed')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_pm10_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_81102_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_PM_10_81102_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_PM_10_81102_' + year + '.hdf', 'df', format='fixed')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_so2_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42401_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_SO2_42401_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_SO2_42401_' + year + '.hdf', 'df', format='fixed')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_no2_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42602_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_NO2_42602_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_NO2_42602_' + year + '.hdf', 'df', format='fixed')
        self.aqsdf = df.copy()

    def retrieve_aqs_daily_pm25_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_88101_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, compression='zip', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_PM_25_88101_' + year + '.hdf'
        df.to_hdf(self.datadir + '/' + 'AQS_DAILY_PM_25_88101_' + year + '.hdf', 'df', format='fixed')
        self.aqsdf = df.copy()

    def load_aqs_pm25_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_PM_25_88101_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_pm25_data(dates)
        # aqs = pd.read_hdf(self.datadir + '/' + 'AQS_HOURLY_PM_25_88101_' + year + '.hdf')
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_voc_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_VOC_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_pm25_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_ozone_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_OZONE_44201_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_ozone_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_pm10_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_PM_10_81102_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_pm10_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_so2_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_SO2_42401_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_so2_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_no2_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_NO2_42602_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_no2_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_co_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_CO_42101_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_co_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_SPEC_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_SPEC_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_SPEC_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_nonoxnoy_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_HOURLY_NONOXNOY_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_nonoxnoy_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_all_hourly_data(self, dates):
        dfs = []
        dfs.append(self.load_aqs_co_data(dates))
        dfs.append(self.load_aqs_pm10_data(dates))
        dfs.append(self.load_aqs_ozone_data(dates))
        dfs.append(self.load_aqs_pm25_data(dates))
        dfs.append(self.load_aqs_SPEC_data(dates))
        dfs.append(self.load_aqs_no2_data(dates))
        dfs.append(self.load_aqs_so2_data(dates))
        dfs.append(self.load_aqs_voc_data(dates))
        dfs.append(self.load_aqs_nonoxnoy_data(dates))
        self.df = pd.concat(dfs, ignore_index=True)
        self.df = self.change_units(self.df).copy().drop_duplicates()

    def load_aqs_daily_pm25_data(self, dates):
        from datetime import timedelta
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_PM_25_88101_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_pm25_data(dates)
            aqs = pd.read_hdf(fname)
        aqs = pd.read_hdf(self.datadir + '/' + 'AQS_DAILY_PM_25_88101_' + year + '.hdf')
        con = (aqs.datetime >= dates[0] - timedelta(days=1)) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_ozone_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_OZONE_44201_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_pm25_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_pm10_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_PM_10_81102_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_pm25_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_so2_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_SO2_42401_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_pm25_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_no2_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_NO2_42602_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_pm25_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        self.aqsdf = aqs
        return aqs

    def load_aqs_daily_co_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_CO_42101_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_pm25_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def tzutc(self, lon, lat, dates):
        from tzwhere import tzwhere
        import pytz
        tz = tzwhere.tzwhere(forceTZ=True, shapely=True)
        a = dates.astype('M8[s]').astype('O')
        local, offset = [], []
        for i, j, d in zip(lon, lat, a):
            l = tz.tzNameAt(j, i, forceTZ=True)
            timezone = pytz.timezone(l)
            n = d.replace(tzinfo=pytz.UTC)
            r = d.replace(tzinfo=timezone)
            rdst = timezone.normalize(r)
            local.append(n - n.astimezone(timezone).utcoffset())
            offset.append((rdst.utcoffset() + rdst.dst()).total_seconds() // 3600)
        return array(local), array(offset)

    def get_region(self, df):
        sn = df.State_Name.values
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

        df['Region'] = array(sr)
        return df

    def get_species(self, df):
        pc = df.Parameter_Code.unique()
        if len(pc) < 2:
            if pc == 88101:
                df['Species'] = pd.Series(['PM2.5' for i in df.Parameter_Code], index=df.index)
            if pc == 44201:
                df['Species'] = pd.Series(['OZONE' for i in df.Parameter_Code], index=df.index)
            if pc == 81102:
                df['Species'] = pd.Series(['PM10' for i in df.Parameter_Code], index=df.index)
            if pc == 42401:
                df['Species'] = pd.Series(['SO2' for i in df.Parameter_Code], index=df.index)
            if pc == 42602:
                df['Species'] = pd.Series(['NO2' for i in df.Parameter_Code], index=df.index)
            if pc == 42101:
                df['Species'] = pd.Series(['CO' for i in df.Parameter_Code], index=df.index)
        else:
            df['Species'] = ''
            for i in pc:
                con = df.Parameter_Code == i
                if i == 88305:
                    df.loc[con, 'Species'] = 'OC'
                if i == 88306:
                    df.loc[con, 'Species'] = 'PM2.5_NO3'
                if i == 88307:
                    df.loc[con, 'Species'] = 'PM2.5_EC'
                if i == 88316:
                    df.loc[con, 'Species'] = 'PM2.5_EC_Optical'
                if i == 88403:
                    df.loc[con, 'Species'] = 'PM2.5_SO4'
                if i == 88312:
                    df.loc[con, 'Species'] = 'PM2.5_TOT_CARBON'
                if i == 42600:
                    df.loc[con, 'Species'] = 'NOY'
                if i == 42601:
                    df.loc[con, 'Species'] = 'NO'
                if i == 42603:
                    df.loc[con, 'Species'] = 'NOX'
        return df

    def change_units(self, df):
        units = df.Units.unique()
        for i in units:
            con = df.Units == i
            if i == 'Parts per billion':
                df.loc[con, 'Units'] = 'PPB'
            if i == 'Parts per million':
                df.loc[con, 'Units'] = 'PPM'
            if i == 'Micrograms/cubic meter (25 C)':
                df.loc[con, 'Units'] = 'UG/M3'
            if i == 'Degrees Centigrade':
                df.loc[con, 'Units'] = 'C'
            if i == 'Micrograms/cubic meter (LC)':
                df.loc[con, 'Units'] = 'UG/M3'
        return df
