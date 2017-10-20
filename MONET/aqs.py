# this is a class to deal with aqs data
import os
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
from numpy import array, arange
import inspect
import requests


class aqs:
    def __init__(self):
        #        self.baseurl = 'https://aqs.epa.gov/aqsweb/airdata/'
        self.baseurl = 'https://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/'
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
        self.savecols = ['datetime_local', 'datetime', 'SCS', 'Latitude', 'Longitude', 'Obs', 'Units', 'Species']
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
        self.df = None  # hourly dataframe
        self.monitor_file = inspect.getfile(self.__class__)[:-13] + '/data/monitoring_site_locations.dat'
        self.monitor_df = None
        self.d_df = None  # daily dataframe

    def check_file_size(self, url):
        test = requests.head(url).headers
        if int(test['Content-Length']) > 1000:
            return True
        else:
            return False

    def retrieve_aqs_hourly_pm25_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url1 = self.baseurl + 'hourly_88101_' + year + '.zip'
        if self.check_file_size(url1):
            print 'Downloading Hourly PM25 FRM: ' + url1
            filename = wget.download(url1)
            print ''
            print 'Unpacking: ' + url1
            dffrm = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                       'datetime_local': ["Date Local", "Time Local"]},
                                infer_datetime_format=True)
            dffrm.columns = self.renamedhcols
            dffrm['SCS'] = array(
                dffrm['State_Code'].values * 1.E7 + dffrm['County_Code'].values * 1.E4 + dffrm['Site_Num'].values,
                dtype='int32')
        else:
            dffrm = pd.DataFrame(columns=self.renamedhcols)
        url2 = self.baseurl + 'hourly_88502_' + year + '.zip'
        if self.check_file_size(url2):
            print 'Downloading Hourly PM25 NON-FRM: ' + url2
            filename = wget.download(url2)
            print ''
            print 'Unpacking: ' + url2
            dfnfrm = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                        'datetime_local': ["Date Local", "Time Local"]},
                                 infer_datetime_format=True)
            dfnfrm.columns = self.renamedhcols
            dfnfrm['SCS'] = array(
                dfnfrm['State_Code'].values * 1.E7 + dfnfrm['County_Code'].values * 1.E4 + dfnfrm['Site_Num'].values,
                dtype='int32')
        else:
            dfnfrm = pd.DataFrame(columns=self.renamedhcols)
        if self.check_file_size(url1) | self.check_file_size(url2):
            df = pd.concat([dfnfrm, dffrm], ignore_index=True)
            df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
            df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
            df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
            df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                              dtype='int32')

            # df.drop('Qualifier', axis=1, inplace=True)
            df = self.get_species(df)
            # df = self.get_region(df)
            df = df.copy()[self.savecols]
            df = self.add_metro_metadata2(df)
            df['Species'] = 'PM2.5'
            print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_PM_25_88101_' + year + '.hdf'
            df.to_hdf('AQS_HOURLY_PM_25_88101_' + year + '.hdf', 'df', format='table')
        else:
            df = pd.DataFrame(columns=self.renamedhcols)
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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_OZONE_44201_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_OZONE_44201_' + year + '.hdf', 'df', format='table')

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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        # df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_PM_10_81102_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_PM_10_81102_' + year + '.hdf', 'df', format='table')
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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        # df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_SO2_42401_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_SO2_42401_' + year + '.hdf', 'df', format='table')
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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        #        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_NO2_42602_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_NO2_42602_' + year + '.hdf', 'df', format='table')
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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        #        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_CO_42101_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_CO_42101_' + year + '.hdf', 'df', format='table')
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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        #        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_NONOXNOY_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_NONOXNOY_' + year + '.hdf', 'df', format='table')
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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df, voc=True)
        #        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_VOC_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_VOC_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_hourly_spec_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_SPEC_' + year + '.zip'
        if self.check_file_size(url):
            print 'Downloading PM Speciation: ' + url
            filename = wget.download(url)
            print ''
            print 'Unpacking: ' + url
            df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                    'datetime_local': ["Date Local", "Time Local"]},
                             infer_datetime_format=True)
            df.columns = self.renamedhcols
            df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
            df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
            df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
            df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                              dtype='int32')
            df.drop('Qualifier', axis=1, inplace=True)
            df = self.get_species(df)
            df = df.copy()[self.savecols]
            df = self.add_metro_metadata2(df)
            print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_SPEC_' + year + '.hdf'
            df.to_hdf('AQS_HOURLY_SPEC_' + year + '.hdf', 'df', format='table')
            return df
        else:
            return pd.DataFrame(columns=self.renamedhcols)

    def retrieve_aqs_hourly_wind_data(self, dates):
        import wget
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'hourly_WIND_' + year + '.zip'
        print 'Downloading AQS WIND: ' + url
        filename = wget.download(url)
        print ''
        print 'Unpacking: ' + url
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        #        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_WIND_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_WIND_' + year + '.hdf', 'df', format='table')
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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        #        df = self.get_region(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_TEMP_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_TEMP_' + year + '.hdf', 'df', format='table')
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
        df = pd.read_csv(filename, parse_dates={'datetime': ['Date GMT', 'Time GMT'],
                                                'datetime_local': ["Date Local", "Time Local"]},
                         infer_datetime_format=True)
        df.columns = self.renamedhcols
        df.loc[:, 'State_Code'] = pd.to_numeric(df.State_Code, errors='coerce')
        df.loc[:, 'Site_Num'] = pd.to_numeric(df.Site_Num, errors='coerce')
        df.loc[:, 'County_Code'] = pd.to_numeric(df.County_Code, errors='coerce')
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.drop('Qualifier', axis=1, inplace=True)
        df = self.get_species(df)
        df = df.copy()[self.savecols]
        df = self.add_metro_metadata2(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_HOURLY_RHDP_' + year + '.hdf'
        df.to_hdf('AQS_HOURLY_RHDP_' + year + '.hdf', 'df', format='table')
        return df

    def load_aqs_pm25_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = 'AQS_HOURLY_PM_25_88101_' + year + '.hdf'
        if os.path.isfile(fname):
            print "File Found, Loading: " + fname
            aqs = pd.read_hdf(fname)
        else:
            aqs = self.retrieve_aqs_hourly_pm25_data(dates)
        if aqs.empty:
            return aqs
        else:
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
        aqs.Units = 'ppb'
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
            print 'Retrieving Data'
            aqs = self.retrieve_aqs_hourly_spec_data(dates)
        if aqs.empty:
            return pd.DataFrame(columns=self.renamedhcols)
        else:
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

    def load_data(self, param, dates):
        if param == 'PM2.5':
            df = self.load_aqs_pm25_data(dates)
        elif param == 'PM10':
            df = self.load_aqs_pm10_data(dates)
        elif param == 'SPEC':
            df = self.load_aqs_spec_data(dates)
        elif param == 'CO':
            df = self.load_aqs_co_data(dates)
        elif param == 'OZONE':
            df = self.load_aqs_ozone_data(dates)
        elif param == 'SO2':
            df = self.load_aqs_so2_data(dates)
        elif param == 'VOC':
            df = self.load_aqs_voc_data(dates)
        elif param == 'NONOXNOY':
            df = self.load_aqs_nonoxnoy_data(dates)
        elif param == 'WIND':
            df = self.load_aqs_wind_data(dates)
        elif param == 'TEMP':
            df = self.load_aqs_temp_data(dates)
        elif param == 'RHDP':
            df = self.load_aqs_rhdp_data(dates)
        return df

    def load_daily_data(self, param, dates):
        if param == 'PM2.5':
            df = self.load_aqs_daily_pm25_data(dates)
        elif param == 'PM10':
            df = self.load_aqs_daily_pm10_data(dates)
        elif param == 'SPEC':
            df = self.load_aqs_daily_spec_data(dates)
        elif param == 'CO':
            df = self.load_aqs_daily_co_data(dates)
        elif param == 'OZONE':
            df = self.load_aqs_daily_no2_data(dates)
        elif param == 'SO2':
            df = self.load_aqs_daily_so2_data(dates)
        elif param == 'VOC':
            df = self.load_aqs_daily_voc_data(dates)
        elif param == 'NONOXNOY':
            df = self.load_aqs_daily_nonoxnoy_data(dates)
        elif param == 'WIND':
            df = self.load_aqs_daily_wind_data(dates)
        elif param == 'TEMP':
            df = self.load_aqs_daily_temp_data(dates)
        elif param == 'RHDP':
            df = self.load_aqs_daily_rhdp_data(dates)
        return df

    def load_all_hourly_data2(self, dates, datasets='all'):
        import dask
        import dask.dataframe as dd
        os.chdir(self.datadir)
        params = ['SPEC', 'PM10', 'PM2.5', 'CO', 'OZONE', 'SO2', 'VOC', 'NONOXNOY', 'WIND', 'TEMP', 'RHDP']
        dfs = [dask.delayed(self.load_data)(i, dates) for i in params]
        dff = dd.from_delayed(dfs)
        # dff = dff.drop_duplicates()
        self.df = dff.compute()
        self.df = self.change_units(self.df)
        #        self.df = pd.concat(dfs, ignore_index=True)
        #        self.df = self.change_units(self.df).drop_duplicates(subset=['datetime','SCS','Species','Obs']).dropna(subset=['Obs'])
        os.chdir(self.cwd)

    def load_all_daily_data(self, dates, datasets='all'):
        import dask
        import dask.dataframe as dd
        from dask.diagnostics import ProgressBar
        os.chdir(self.datadir)
        pbar = ProgressBar()
        pbar.register()
        params = ['SPEC', 'PM10', 'PM2.5', 'CO', 'OZONE', 'SO2', 'VOC', 'NONOXNOY', 'WIND', 'TEMP', 'RHDP']
        # dfs = [dask.delayed(self.load_daily_data)(i,dates) for i in params]
        #        print dfs
        # dff = dd.from_delayed(dfs)
        # self.d_df = dff.compute()
        dfs = [self.load_all_daily_data(i, dates) for i in params]
        self.d_df = self.change_units(self.d_df)
        os.chdir(self.cwd)

    def get_all_hourly_data(self, dates):
        os.chdir(self.datadir)
        dfs = [self.load_aqs_co_data(dates), self.load_aqs_pm10_data(dates), self.load_aqs_ozone_data(dates),
               self.load_aqs_pm25_data(dates), self.load_aqs_spec_data(dates), self.load_aqs_no2_data(dates),
               self.load_aqs_so2_data(dates), self.load_aqs_voc_data(dates), self.load_aqs_nonoxnoy_data(dates),
               self.load_aqs_wind_data(dates), self.load_aqs_temp_data(dates), self.load_aqs_rhdp_data(dates)]
        os.chdir(self.cwd)

    def load_all_hourly_data(self, dates, datasets='all'):
        os.chdir(self.datadir)
        if datasets.upper() == 'PM':
            dfs = [self.load_aqs_pm10_data(dates), self.load_aqs_pm25_data(dates), self.load_aqs_spec_data(dates)]
        else:
            dfs = [self.load_aqs_co_data(dates), self.load_aqs_pm10_data(dates), self.load_aqs_ozone_data(dates),
                   self.load_aqs_pm25_data(dates), self.load_aqs_spec_data(dates), self.load_aqs_no2_data(dates),
                   self.load_aqs_so2_data(dates), self.load_aqs_voc_data(dates), self.load_aqs_nonoxnoy_data(dates),
                   self.load_aqs_wind_data(dates), self.load_aqs_temp_data(dates),
                   self.load_aqs_rhdp_data(dates)]  # ,self.load_aqs_daily_spec_data(dates)]
        self.df = pd.concat(dfs, ignore_index=True)
        self.df = self.change_units(self.df).drop_duplicates()
        os.chdir(self.cwd)

    def load_aqs_daily_pm25_data(self, dates):
        from datetime import timedelta
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_PM25_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_pm25_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0] - timedelta(days=1)) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_ozone_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_OZONE_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_ozone_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_pm10_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_PM10_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_pm10_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_so2_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_SO2_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_so2_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_no2_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_NO2_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_no2_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        self.aqsdf = aqs
        return aqs

    def load_aqs_daily_co_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_CO_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_co_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_temp_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_TEMP_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_temp_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_rh_dp_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_RH_DP_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_rh_dp_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_spec_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_SPEC_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_spec_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_voc_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_VOC_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_voc_data(dates)
            aqs = pd.read_hdf(fname)
        con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
        aqs = aqs[con]
        aqs.index = arange(aqs.index.shape[0])
        return aqs

    def load_aqs_daily_wind_data(self, dates):
        year = dates[0].strftime('%Y')
        fname = self.datadir + '/' + 'AQS_DAILY_WIND_' + year + '.hdf'
        if os.path.isfile(fname):
            aqs = pd.read_hdf(fname)
        else:
            self.retrieve_aqs_daily_wind_data(dates)
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

    def read_monitor_file(self):
        if type(self.monitor_df) == type(None):
            if os.path.isfile(self.monitor_file):
                fname = self.monitor_file
                colsinuse = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
                f = pd.read_csv(fname, delimiter='|', header=None, usecols=colsinuse)
                f.columns = ['SCS', 'Site_Code', 'Site_Name', 'Status', 'Agency', 'Agency_Name', 'EPA_region',
                             'Latitude',
                             'Longitude', 'Elevation', 'GMT_Offset', 'Country_Code', 'CMSA_Code', 'CMSA_Name',
                             'MSA_Code',
                             'MSA_Name', 'State_Code', 'State_Name', 'County_Code', 'County_Name', 'City_Code']

                self.monitor_df = f.drop_duplicates().dropna(subset=['SCS']).copy()
                self.monitor_df.SCS = self.monitor_df.SCS.values.astype('int32')
            else:
                print '   Monitor File not found.  Meta-Data city names not added'
                f = None

    def add_metro_metadata2(self, df):
        from numpy import NaN
        if type(self.monitor_df) != type(None):
            dfs = self.monitor_df[['SCS', 'MSA_Name', 'State_Name', 'County_Name', 'EPA_region', 'MSA_Code',
                                   'GMT_Offset']].drop_duplicates()
            dfs.SCS = dfs.SCS.values.astype('int32')
            df = pd.merge(df, dfs, on=['SCS'], how='left')
        elif os.path.isfile(self.monitor_file):
            print '    Monitor Station Meta-Data Found: Compiling Dataset'
            self.read_monitor_file()
            dfs = self.monitor_df[
                ['SCS', 'MSA_Name', 'State_Name', 'County_Name', 'EPA_region', 'GMT_Offset']].drop_duplicates()
            dfs.SCS = dfs.SCS.values.astype('int32')
            df = pd.merge(df, dfs, on=['SCS'], how='left')
        return df

    def retrieve_aqs_daily_voc_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_VOCS_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        #        ZipFile(filename).extractall()
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_VOC_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_VOC_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_temp_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_TEMP_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_TEMP_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_TEMP_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_rh_dp_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_RH_DP_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_RH_DP_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_RH_DP_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_wind_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_WIND_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        #        ZipFile(filename).extractall()
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_WIND_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_WIND_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_co_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42101_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        #        ZipFile(filename).extractall()
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_CO_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_CO_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_ozone_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_44201_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        #        ZipFile(filename).extractall()
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_OZONE_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_OZONE_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_no2_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42602_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        #        ZipFile(filename).extractall()
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')

        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_NO2_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_NO2_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_so2_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_42401_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        #        ZipFile(filename).extractall()
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_SO2_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_SO2_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_pm10_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_81102_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        #        ZipFile(filename).extractall()
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_PM10_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_PM10_' + year + '.hdf', 'df', format='table')
        return df

    def retrieve_aqs_daily_pm25_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_88101_' + year + '.zip'
        if self.check_file_size(url):
            print 'Downloading: ' + url
            filename = wget.download(url);
            print ''
            print 'Unpacking: ' + url
            dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
            df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                             date_parser=dateparse)
            df.columns = self.renameddcols
            df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                              dtype='int32')
        else:
            df = pd.DataFrame()
        url2 = self.baseurl + 'daily_88502_' + year + '.zip'
        if self.check_file_size(url2):
            print 'Downloading: ' + url2
            filename = wget.download(url2)
            dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
            df2 = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                              date_parser=dateparse)
            df2.columns = self.renameddcols
            df2['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                               dtype='int32')
        else:
            df2 = pd.DataFrame()
        if self.check_file_size(url) | self.check_file_size(url2):
            df = pd.concat([df, df2], ignore_index=True)
            df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
            df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
            df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
            df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
            df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
            df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
            df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
            df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
            df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
            df.dropna(subset=['Parameter_Code'], inplace=True)
            df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
            df = self.read_monitor_and_site(df)
            df['SCS'] = df.SCS.astype(str).str.zfill(9)
            df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
            df = self.get_species(df)
            df.to_hdf('AQS_DAILY_PM25_' + year + '.hdf', 'df', format='table')
        else:
            df = pd.DataFrame()
        return df

    def retrieve_aqs_daily_spec_data(self, dates):
        import wget
        from numpy import NaN, int64
        i = dates[0]
        year = i.strftime('%Y')
        url = self.baseurl + 'daily_SPEC_' + year + '.zip'
        print 'Downloading: ' + url
        filename = wget.download(url);
        print ''
        print 'Unpacking: ' + url
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
        #        ZipFile(filename).extractall()
        df = pd.read_csv(filename, parse_dates={'datetime_local': ["Date Local"]},
                         date_parser=dateparse)
        df.columns = self.renameddcols
        df['SCS'] = array(df['State_Code'].values * 1.E7 + df['County_Code'].values * 1.E4 + df['Site_Num'].values,
                          dtype='int32')
        df.loc[df.Parameter_Code == 68101, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68102, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68108, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68103, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68105, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68104, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68106, 'Parameter_Code'] = NaN
        df.loc[df.Parameter_Code == 68107, 'Parameter_Code'] = NaN
        df.dropna(subset=['Parameter_Code'], inplace=True)
        df.loc[:, 'Parameter_Code'] = df.Parameter_Code.astype(int64)
        df = self.read_monitor_and_site(df)
        df['SCS'] = df.SCS.astype(str).str.zfill(9)
        df['datetime'] = df.datetime_local - pd.to_timedelta(df.GMT_Offset, unit='h')
        df = self.get_species(df)
        #        df['datetime'] =
        print 'Saving file to: ' + self.datadir + '/' + 'AQS_DAILY_SPEC_' + year + '.hdf'
        df.to_hdf('AQS_DAILY_SPEC_' + year + '.hdf', 'df', format='table')
        return df

    def read_monitor_and_site(self, df):
        site_url = 'https://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/aqs_sites.zip'  # has GMT Land Use and Location Setting (RURAL URBAN etc...)
        monitor_url = 'https://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/aqs_monitors.zip'  # has network info (CSN IMPROVE etc....)
        site = pd.read_csv(site_url)
        monitor = pd.read_csv(monitor_url, index_col=None, usecols=range(29))
        site['SCS'] = site['State Code'].astype(str).str.zfill(2) + site['County Code'].astype(str).str.zfill(3) + site[
            'Site Number'].astype(str).str.zfill(4)
        monitor['SCS'] = monitor['State Code'].astype(str).str.zfill(2) + monitor['County Code'].astype(str).str.zfill(
            3) + monitor['Site Number'].astype(str).str.zfill(4)
        site.columns = [i.replace(' ', '_') for i in site.columns]
        s = monitor.merge(site[['SCS', 'Land_Use', 'Location_Setting', 'GMT_Offset']], on=['SCS'], how='left')
        s.columns = [i.replace(' ', '_') for i in s.columns]
        s['SCS'] = pd.to_numeric(s.SCS, errors='coerce')
        return df.merge(s[['SCS', u'GMT_Offset', 'Networks', u'Land_Use', u'Location_Setting', 'Parameter_Code']],
                        on=['SCS', 'Parameter_Code'], how='left')
