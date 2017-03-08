# this is a class to deal with aqs data
import os
from datetime import datetime
from zipfile import ZipFile

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
        self.savecols = ['datetime_local', 'datetime', 'SCS', 'Latitude', 'Longitude','Obs', 'Units','Species', 'Region']
        self.se_states = array(
            ['Alabama', 'Florida', 'Georgia', 'Mississippi', 'North Carolina', 'South Carolina', 'Tennessee',
             'Virginia', 'West Virginia'], dtype='|S14')
        self.se_states_abv = array(
            ['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN',
             'VA', 'WV'], dtype='|S14')
        self.ne_states = array(['Connecticut', 'Delaware', 'District Of Columbia', 'Maine', 'Maryland', 'Massachusetts',
                                'New Hampshire', 'New Jersey', 'New York', 'Pennsylvania', 'Rhode Island', 'Vermont'],
                               dtype='|S20')
        self.ne_states_abv = array(['CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'],
                                   dtype='|S20')
        self.nc_states = array(
            ['Illinois', 'Indiana', 'Iowa', 'Kentucky', 'Michigan', 'Minnesota', 'Missouri', 'Ohio', 'Wisconsin'],
            dtype='|S9')
        self.nc_states_abv = array(['IL', 'IN', 'IA', 'KY', 'MI', 'MN', 'MO', 'OH', 'WI'],
                                   dtype='|S9')
        self.sc_states = array(['Arkansas', 'Louisiana', 'Oklahoma', 'Texas'], dtype='|S9')
        self.sc_states_abv = array(['AR', 'LA', 'OK', 'TX'], dtype='|S9')
        self.r_states = array(['Arizona', 'Colorado', 'Idaho', 'Kansas', 'Montana', 'Nebraska', 'Nevada', 'New Mexico',
                               'North Dakota', 'South Dakota', 'Utah', 'Wyoming'], dtype='|S12')
        self.r_states_abv = array(['AZ', 'CO', 'ID', 'KS', 'MT', 'NE', 'NV', 'NM', 'ND', 'SD', 'UT', 'WY'],
                                  dtype='|S12')
        self.p_states = array(['California', 'Oregon', 'Washington'], dtype='|S10')
        self.p_states_abv = array(['CA', 'OR', 'WA'], dtype='|S10')
        self.datadir = '.'
        self.cwd = os.getcwd()
        self.df = None
        self.monitor_file = os.getcwd() + '/monitoring_site_locations.dat'
        self.monitor_df = None

    def retrieve_aqs_hourly_pm25_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_88101_' + year + '.zip'

        print 'Downloading Hourly PM2.5: ' + url
        filename = wget.download(url, );
        print ''
        print 'Unpacking: ' + url
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')

        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_PM_25_88101_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_PM_25_88101_' + year + '.hdf', 'df', format='fixed')
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
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print df.keys()
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_OZONE_44201_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_OZONE_44201_' + year + '.hdf', 'df', format='fixed')

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
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_PM_10_81102_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_PM_10_81102_' + year + '.hdf', 'df', format='fixed')
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
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_SO2_42401_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_SO2_42401_' + year + '.hdf', 'df', format='fixed')
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
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        z = ZipFile(filename)
        z.extractall()
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_NO2_42602_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_NO2_42602_' + year + '.hdf', 'df', format='fixed')
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
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_CO_42101_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_CO_42101_' + year + '.hdf', 'df', format='fixed')
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
        ZipFile(filename).extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_NONOXNOY_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_NONOXNOY_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_voc_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_VOCS_' + year + '.zip'
        print 'Downloading Hourly VOCs: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df, voc=True)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_VOC_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_VOC_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_spec_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_SPEC_' + year + '.zip'
        print 'Downloading CSN Speciation: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_SPEC_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_SPEC_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_wind_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_WIND_' + year + '.zip'
        print 'Downloading AQS WIND: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_WIND_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_WIND_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_temp_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_TEMP_' + year + '.zip'
        print 'Downloading AQS TEMP: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_TEMP_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_TEMP_' + year + '.hdf', 'df', format='fixed')
        return df

    def retrieve_aqs_hourly_rhdp_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_RH_DP_' + year + '.zip'
        print 'Downloading AQS RH and DP: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        z = ZipFile(filename)
        z.extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                              'datetime_local': ["Date Local", "Time Local"]},
                         date_parser=dateparse)
        df.columns = self.renamedhcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_RHDP_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_RHDP_' + year + '.hdf', 'df', format='fixed')
        return df

    def load_aqs_pm25_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_PM_25_88101_' + year + '.hdf'
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
        fname = 'AQS_HOURLY_VOC_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_voc_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_ozone_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_OZONE_44201_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_ozone_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.Units = 'PPB'
        aqs.Obs = aqs.Obs.values * 1000.
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_pm10_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_PM_10_81102_' + year + '.hdf'
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
        fname = 'AQS_HOURLY_SO2_42401_' + year + '.hdf'
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
        fname = 'AQS_HOURLY_NO2_42602_' + year + '.hdf'
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
        fname = 'AQS_HOURLY_CO_42101_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_co_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_spec_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_SPEC_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_spec_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_wind_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_WIND_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_wind_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_temp_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_TEMP_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_temp_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_rhdp_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_RHDP_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_rhdp_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_nonoxnoy_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_NONOXNOY_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_nonoxnoy_data(dates)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_all_hourly_data(self, dates, datasets='all'):
        os.chdir(self.datadir)
        if datasets.upper() == 'PM':
            dfs = [self.load_aqs_pm10_data(dates), self.load_aqs_pm25_data(dates), self.load_aqs_spec_data(dates)]
        else:
            dfs = [self.load_aqs_co_data(dates), self.load_aqs_pm10_data(dates), self.load_aqs_ozone_data(dates),
                   self.load_aqs_pm25_data(dates), self.load_aqs_spec_data(dates), self.load_aqs_no2_data(dates),
                   self.load_aqs_so2_data(dates), self.load_aqs_voc_data(dates), self.load_aqs_nonoxnoy_data(dates),
                   self.load_aqs_wind_data(dates), self.load_aqs_temp_data(dates)]
        self.df = pd.concat(dfs, ignore_index=True)
        self.df = self.change_units(self.df).copy().drop_duplicates()
        os.chdir(self.cwd)
#        self.df.SCS = self.df.SCS.values.astype('int32')
        self.df = self.add_metro_metadata2(self.df)

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
        sr = df.State_Name.copy().values
        for i in self.se_states:
            con = sr == i
            sr[con] = 'Southeast'
        for i in self.ne_states:
            con = sr == i
            #print con.max()
            sr[con] = 'Northeast'
        for i in self.nc_states:
            con = sr == i
            sr[con] = 'North Central'
        for i in self.sc_states:
            con = sr == i
            sr[con] = 'South Central'
        for i in self.p_states:
            con = sr == i
            sr[con] = 'Pacific'
        for i in self.r_states:
            con = sr == i
            sr[con] = 'Rockies'
        sr[sr == 'CC'] = 'Canada'
        sr[sr == 'MX'] = 'Mexico'

        df['Region'] = array(sr)
        
#        df = self.change_states_to_abv(df)
        return df

    def change_states_to_abv(self, df):
        for i, j in enumerate(self.se_states):
            con = df['State_Name'] == j
            df.loc[con, 'State_Name'] = self.se_states_abv[i]
        for i, j in enumerate(self.nc_states):
            con = df['State_Name'] == j
            df.loc[con, 'State_Name'] = self.nc_states_abv[i]
        for i, j in enumerate(self.sc_states):
            con = df['State_Name'] == j
            df.loc[con, 'State_Name'] = self.sc_states_abv[i]
        for i, j in enumerate(self.p_states):
            con = df['State_Name'] == j
            df.loc[con, 'State_Name'] = self.p_states_abv[i]
        for i, j in enumerate(self.ne_states):
            con = df['State_Name'] == j
            df.loc[con, 'State_Name'] = self.ne_states_abv[i]
        for i, j in enumerate(self.r_states):
            con = df['State_Name'] == j
            df.loc[con, 'State_Name'] = self.r_states_abv[i]
        con = df['State_Name'] == 'Country Of Mexico'
        df.loc[con, 'State_Name'] = 'MX'
        con = df['State_Name'] == 'Alaska'
        df.loc[con, 'State_Name'] = 'AK'
        con = df['State_Name'] == 'Hawaii'
        df.loc[con, 'State_Name'] = 'HI'
        con = df['State_Name'] == 'Puerto Rico'
        df.loc[con, 'State_Name'] = 'PR'
        return df

    def get_species(self, df, voc=False):
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
            if pc == 62101:
                df['Species'] = pd.Series(['TEMP' for i in df.Parameter_Code], index=df.index)
        else:
            df['Species'] = ''
            for i in pc:
                con = df.Parameter_Code == i
                if i == 88305:
                    df.loc[con, 'Species'] = 'OC'
                if i == 88306:
                    df.loc[con, 'Species'] = 'NO3f'
                if i == 88307:
                    df.loc[con, 'Species'] = 'ECf'
                if i == 88316:
                    df.loc[con, 'Species'] = 'ECf_optical'
                if i == 88403:
                    df.loc[con, 'Species'] = 'SO4f'
                if i == 88312:
                    df.loc[con, 'Species'] = 'TCf'
                if i == 42600:
                    df.loc[con, 'Species'] = 'NOY'
                if i == 42601:
                    df.loc[con, 'Species'] = 'NO'
                if i == 42603:
                    df.loc[con, 'Species'] = 'NOX'
                if i == 61103:
                    df.loc[con, 'Species'] = 'WS'
                if i == 61104:
                    df.loc[con, 'Species'] = 'WD'
                if i == 62201:
                    df.loc[con, 'Species'] = 'RH'
                if i == 62103:
                    df.loc[con, 'Species'] = 'DP'
        if voc:
            df.Species = df.Parameter_Name.str.upper()

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

    def read_monitor_file(self):
        if type(self.monitor_df) == type(None):
            if os.path.isfile(self.monitor_file):
                fname = self.monitor_file
                colsinuse = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
                f = pd.read_csv(fname, delimiter='|', header=None, usecols=colsinuse)
                f.columns = ['SCS', 'Site_Code', 'Site_Name', 'Status', 'Agency', 'Agency_Name', 'EPA_region', 'Latitude',
                         'Longitude', 'Elevation', 'GMT_Offset', 'Country_Code', 'CMSA_Code', 'CMSA_Name', 'MSA_Code',
                         'MSA_Name', 'State_Code', 'State_Name', 'County_Code', 'County_Name', 'City_Code']
                
                self.monitor_df = f.drop_duplicates().dropna(subset=['SCS']).copy()
                self.monitor_df.SCS = self.monitor_df.SCS.values.astype('int32')
            else:
                print '   Monitor File not found.  Meta-Data city names not added'
                f = None

    # def add_metro_metadata(self):
    #     from numpy import NaN
    #     if os.path.isfile(self.monitor_file):
    #         print '    Monitor Station Meta-Data Found: Compiling Dataset'
    #         self.read_monitor_file()
    #         dfs = self.monitor_df[['SCS', 'MSA_Name']]
    #         for i in self.df.SCS.unique():
    #             con = self.df.SCS == i
    #             if dfs.loc[dfs.SCS.values == i]['MSA_Name'].count() > 0:
    #                 self.df.loc[dfs.SCS.values == i, 'MSA_Name'] = dfs.loc[dfs.SCS.values == i]['MSA_Name'].unique()
    #             else:
    #                 self.df.loc[con, 'MSA_Name'] = NaN

    #         self.df = pd.merge(self.df, dfs, on='SCS', how='left')
    #         self.df.drop('MSA_Name_y', axis=1, inplace=True)
    #         self.df.rename(columns={'MSA_Name_x': 'MSA_Name'}, inplace=True)

    def add_metro_metadata2(self,df):
        from numpy import NaN
        if type(self.monitor_df) != type(None):
            dfs = self.monitor_df[['SCS', 'MSA_Name','State_Name','County_Name','EPA_region']].drop_duplicates()
            dfs.SCS = dfs.SCS.values.astype('int32')
            df = pd.merge(df,dfs,on=['SCS'],how='left')
        elif os.path.isfile(self.monitor_file):
            print '    Monitor Station Meta-Data Found: Compiling Dataset'
            self.read_monitor_file()
            dfs = self.monitor_df[['SCS', 'MSA_Name','State_Name','County_Name','EPA_region']].drop_duplicates()
            dfs.SCS = dfs.SCS.values.astype('int32')
            df = pd.merge(df,dfs,on=['SCS'],how='left')
        
        return df

    def retrieve_aqs_daily_co_data(self, dates):
        import wget

        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42101_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        ZipFile(filename).extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')

        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_CO_42101_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_CO_42101_' + year + '.hdf', 'df', format='fixed')
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
        ZipFile(filename).extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_OZONE_44201_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_OZONE_44201_' + year + '.hdf', 'df', format='fixed')
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
        ZipFile(filename).extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_PM_10_81102_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_PM_10_81102_' + year + '.hdf', 'df', format='fixed')
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
        ZipFile(filename).extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_SO2_42401_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_SO2_42401_' + year + '.hdf', 'df', format='fixed')
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
        ZipFile(filename).extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_SO2_42401_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_SO2_42401_' + year + '.hdf', 'df', format='fixed')
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
        ZipFile(filename).extractall()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_NO2_42602_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_NO2_42602_' + year + '.hdf', 'df', format='fixed')
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
        ZipFile(filename).extractall()
        df = pd.read_csv(filename[:-4] + '.csv', parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        utc = self.tzutc(df.Longitude.values, df.Latitude.values, df.datetime_local.values)
        df['datetime'], df['utcoffset'] = utc[0], utc[1]
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_PM_25_88101_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_PM_25_88101_' + year + '.hdf', 'df', format='fixed')
        self.aqsdf = df.copy()
