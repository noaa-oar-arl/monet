"""
Data is taken from the Climate Reference Network.  This is to expand validation of
    the NOAA ARL model validation leveraging inhouse datasets.

    Data available at https://www.ncdc.noaa.gov/crn/qcdatasets.html

    Here we use the hourly data.

    Field#  Name                           Units
    ---------------------------------------------
       1    WBANNO                         XXXXX
       2    UTC_DATE                       YYYYMMDD
       3    UTC_TIME                       HHmm
       4    LST_DATE                       YYYYMMDD
       5    LST_TIME                       HHmm
       6    CRX_VN                         XXXXXX
       7    LONGITUDE                      Decimal_degrees
       8    LATITUDE                       Decimal_degrees
       9    T_CALC                         Celsius
       10   T_HR_AVG                       Celsius
       11   T_MAX                          Celsius
       12   T_MIN                          Celsius
       13   P_CALC                         mm
       14   SOLARAD                        W/m^2
       15   SOLARAD_FLAG                   X
       16   SOLARAD_MAX                    W/m^2
       17   SOLARAD_MAX_FLAG               X
       18   SOLARAD_MIN                    W/m^2
       19   SOLARAD_MIN_FLAG               X
       20   SUR_TEMP_TYPE                  X
       21   SUR_TEMP                       Celsius
       22   SUR_TEMP_FLAG                  X
       23   SUR_TEMP_MAX                   Celsius
       24   SUR_TEMP_MAX_FLAG              X
       25   SUR_TEMP_MIN                   Celsius
       26   SUR_TEMP_MIN_FLAG              X
       27   RH_HR_AVG                      %
       28   RH_HR_AVG_FLAG                 X
       29   SOIL_MOISTURE_5                m^3/m^3
       30   SOIL_MOISTURE_10               m^3/m^3
       31   SOIL_MOISTURE_20               m^3/m^3
       32   SOIL_MOISTURE_50               m^3/m^3
       33   SOIL_MOISTURE_100              m^3/m^3
       34   SOIL_TEMP_5                    Celsius
       35   SOIL_TEMP_10                   Celsius
       36   SOIL_TEMP_20                   Celsius
       37   SOIL_TEMP_50                   Celsius
       38   SOIL_TEMP_100                  Celsius
    """
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import object
import os
from datetime import datetime

import pandas as pd
from numpy import array


class crn(object):
    def __init__(self):

        self.username = 'anonymous'
        self.password = ''
        self.datadir = '.'
        self.cwd = os.getcwd()
        self.url = 'ftp.ncdc.noaa.gov'
        self.dates = [datetime.strptime('2016-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2016-06-06 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.filelist = []
        self.ftp = None
        self.df = None
        self.se_states = array(
            ['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN',
             'VA', 'WV'], dtype='|S14')
        self.ne_states = array(['CT', 'DE', 'DC', 'ME', 'MD', 'MA',
                                'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'],
                               dtype='|S20')
        self.nc_states = array(
            ['IL', 'IN', 'IA', 'KY', 'MI', 'MN', 'MO', 'OH', 'WI'],
            dtype='|S9')
        self.sc_states = array(['AR', 'LA', 'OK', 'TX'], dtype='|S9')
        self.r_states = array(['AZ', 'CO', 'ID', 'KS', 'MT', 'NE', 'NV', 'NM',
                               'ND', 'SD', 'UT', 'WY'], dtype='|S12')
        self.p_states = array(['CA', 'OR', 'WA'], dtype='|S10')
        self.objtype = 'CRN'
        self.monitor_file = inspect.getfile(
            self.__class__)[:-13] + '/data/stations.tsv'
        self.monitor_df = None
        self.cols = ['WBANNO', 'UTC_DATE', 'UTC_TIME', 'LST_DATE', 'LST_TIME', 'CRX_VN',
                     'LONGITUDE', 'LATITUDE', 'T_CALC', 'T_HR_AVG', 'T_MAX', 'T_MIN',
                     'P_CALC', 'SOLARAD', 'SOLARAD_FLAG', 'SOLARAD_MAX',
                     'SOLARAD_MAX_FLAG', 'SOLARAD_MIN', 'SOLARAD_MIN_FLAG',
                     'SUR_TEMP_TYPE', 'SUR_TEMP', 'SUR_TEMP_FLAG', 'SUR_TEMP_MAX',
                     'SUR_TEMP_MAX_FLAG', 'SUR_TEMP_MIN', 'SUR_TEMP_MIN_FLAG',
                     'RH_HR_AVG', 'RH_HR_AVG_FLAG', 'SOIL_MOISTURE_5',
                     'SOIL_MOISTURE_10', 'SOIL_MOISTURE_20', 'SOIL_MOISTURE_50',
                     'SOIL_MOISTURE_100', 'SOIL_TEMP_5', 'SOIL_TEMP_10', 'SOIL_TEMP_20',
                     'SOIL_TEMP_50', 'SOIL_TEMP_100']

    def retrieve_hourly_filelist(self, cwd):
        """Short summary.

        Parameters
        ----------
        cwd : type
            Description of parameter `cwd`.

        Returns
        -------
        type
            Description of returned object.

        """
        self.ftp.cwd(cwd)
        return array(self.ftp.nlst('CRN*'))

    def openftp(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        from ftplib import FTP
        self.ftp = FTP(self.url)
        self.ftp.login(self.username, self.password)

    def download_single_rawfile(self, fname):
        """Short summary.

        Parameters
        ----------
        fname : type
            Description of parameter `fname`.

        Returns
        -------
        type
            Description of returned object.

        """
        localfile = open(fname, 'wb')
        self.ftp.retrbinary('RETR ' + fname, localfile.write, 1024)
        localfile.close()

    def change_path(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        os.chdir(self.datadir)

    def change_back(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        os.chdir(self.cwd)

    def download_hourly_files(self, path='.'):
        """Short summary.

        Parameters
        ----------
        path : type
            Description of parameter `path` (the default is '.').

        Returns
        -------
        type
            Description of returned object.

        """
        self.datadir = path
        self.change_path()
        print('Connecting to FTP: ' + self.url)
        self.openftp()
        print('Retrieving Hourly file list')
        cwd = '/pub/data/uscrn/products/hourly02/' + \
            self.dates[0].strftime('%Y')
        nlst = self.retrieve_hourly_filelist(cwd)

        if nlst.shape[0] > 0:
            self.ftp.cwd(cwd)
            for i in nlst:
                if not os.path.exists(i):
                    print('Downloading Hourly CRN Data: ' + i)
                    self.download_single_rawfile(i)
                else:
                    print('File found: ', i)
        self.filelist = nlst
        self.change_back()
        self.ftp.close()

    def aggragate_files(self, output=''):
        """Short summary.

        Parameters
        ----------
        output : type
            Description of parameter `output` (the default is '').

        Returns
        -------
        type
            Description of returned object.

        """
        from numpy import sort
        from io import StringIO

        print('Aggregating files into Pandas DataFrame...')
        self.change_path()
        fnames = sort(self.filelist)
        a = ''

        for i in fnames:
            with open(i, 'rb') as f:
                a += f.read()[:-1]  # dont read the last line of each file

        # read all the files with pandas
        dft = pd.read_csv(StringIO(a), delim_whitespace=True, names=self.cols,
                          parse_dates={'datetime': [1, 2], 'datetime_local': [3, 4]}, infer_datetime_format=True)

        self.df = dft.copy()
        self.df = self.df.copy().drop_duplicates()
        print('Reshuffling DataFrame....')
        self.df = self.agg_cols()
        cols = self.df.columns.values
        cols[2] = 'WBAN'
        self.df.columns = cols
        con = (self.df.datetime >= self.dates[0]) & (
            self.df.datetime <= self.dates[-1])
        self.df = self.df[con]
        self.merge_monitor_meta_data()
        if output == '':
            output = 'CRN.hdf'
        print('Outputing dataframe to: ' + output)
        self.df.to_hdf(output, 'df', format='fixed')
        self.change_back()

    def set_daterange(self, begin='', end=''):
        """Short summary.

        Parameters
        ----------
        begin : type
            Description of parameter `begin` (the default is '').
        end : type
            Description of parameter `end` (the default is '').

        Returns
        -------
        type
            Description of returned object.

        """
        dates = pd.date_range(start=begin, end=end,
                              freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates

    def get_monitor_df(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        self.monitor_df = pd.read_csv(self.monitor_file, delimiter='\t')

    def merge_monitor_meta_data(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        self.df.WBAN = self.df.WBAN.astype('|S5')
        self.df = self.df.merge(self.monitor_df, on=[
                                'WBAN', 'LONGITUDE', 'LATITUDE'], how='left')

    def agg_cols(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        cols = array(['T_CALC', 'T_HR_AVG', 'T_MAX', 'T_MIN', 'P_CALC', 'SOLARAD', 'SOLARAD_FLAG', 'SOLARAD_MAX',
                      'SOLARAD_MAX_FLAG', 'SOLARAD_MIN', 'SOLARAD_MIN_FLAG', 'SUR_TEMP_TYPE', 'SUR_TEMP',
                      'SUR_TEMP_FLAG', 'SUR_TEMP_MAX', 'SUR_TEMP_MAX_FLAG', 'SUR_TEMP_MIN', 'SUR_TEMP_MIN_FLAG',
                      'RH_HR_AVG', 'RH_HR_AVG_FLAG', 'SOIL_MOISTURE_5', 'SOIL_MOISTURE_10', 'SOIL_MOISTURE_20',
                      'SOIL_MOISTURE_50', 'SOIL_MOISTURE_100', 'SOIL_TEMP_5', 'SOIL_TEMP_10', 'SOIL_TEMP_20',
                      'SOIL_TEMP_50', 'SOIL_TEMP_100'])
        units = array(
            ['Celsius', 'Celsius', 'Celsius', 'Celsius', 'mm', 'W/m^2', 'X', 'W/m^2', 'X', 'W/m^2', 'X', 'X', 'Celsius',
             'X', 'Celsius', 'X', 'Celsius', 'X', '%', 'X', 'm^3/m^3', 'm^3/m^3', 'm^3/m^3', 'm^3/m^3', 'm^3/m^3',
             'Celsius', 'Celsius', 'Celsius', 'Celsius', 'Celsius'])
        bcols = [u'datetime_local', u'datetime', u'WBANNO', u'CRX_VN', u'LONGITUDE',
                 u'LATITUDE']
        dfs = []
        for i, j in zip(cols, units):
            bdfadd = self.df.copy()[bcols]
            bdfadd['Obs'] = self.df[i].values
            bdfadd['Species'] = i
            bdfadd['Unit'] = j
            dfs.append(bdfadd)

        bdf = pd.concat(dfs, ignore_index=True)

        return bdf
