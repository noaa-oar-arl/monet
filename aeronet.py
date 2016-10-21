# this is written to retrive airnow data concatenate and add to pandas array for usage
import os
from datetime import datetime

import pandas as pd
import wget


class aeronet:
    def __init__(self):
        self.baseurl = 'http://aeronet.gsfc.nasa.gov/data_push/AOT_Level2_All_Points.tar.gz'
        self.datadir = '.'
        self.cwd = os.getcwd()
        self.url = 'ftp.airnowgateway.org'
        self.dates = [datetime.strptime('2016-06-06 12:00:00', '%Y-%m-%d %H:%M:%S'),
                      datetime.strptime('2016-06-10 13:00:00', '%Y-%m-%d %H:%M:%S')]
        self.datestr = []
        self.ftp = None
        self.df = None
        self.objtype = 'AERONET'
        self.station_file = '{0}data/aeronet_locations.txt'.format(
            os.path.realpath(__file__).split(os.path.basename(__file__))[0])
        self.station_df = None
        self.file = self.baseurl.split('/')[-1]

    def download_aeronet_data(self, path='.'):
        print 'Downloading the AERONET L2 AOT file from:'
        print '         ' + self.baseurl
        self.datadir = path
        self.change_path()
        self.file = wget.download(self.baseurl)

    def read_aeronet(self, f):
        # get site location name lat lon
        f.readline()
        f.readline()
        line = f.readline()
        arr = line.split(',')
        location = arr[0].split('=')[1]
        lon = float(arr[1].split('=')[1])
        lat = float(arr[2].split('=')[1])
        f.seek(0)
        df = pd.read_csv(f, skiprows=4, na_values=['N/A'])

        df['Latitude'] = lat
        df['Longitude'] = lon
        df['Site_Name'] = location

        return df

    def aggragate_files(self):
        from numpy import array
        import tarfile
        print 'Opening file: ' + self.file
        ff = tarfile.open(self.file)
        print 'Getting file members:'
        members = ff.getmembers()  # get all file names in the tar file
        names = [i.name for i in members]
        getnames = self.station_df.Site_Name.values
        n = []
        for k in getnames:
            for index, i in enumerate(names):
                if k in i:
                    n.append(index)
        dfs = []
        for f in array(members)[array(n)]:
            print f
            dfs.append(self.read_aeronet(ff.extractfile(f)))

        # dfs = [self.read_aeronet(ff.extractfile(f)) for f in array(members)[array(n)]]
        self.df = pd.concat(dfs)
        dates = [datetime.strptime(x + ' ' + y, '%d:%m:%Y %H:%M:%S') for x, y in
                 zip(self.df['Date(dd-mm-yy)'].values, self.df['Time(hh:mm:ss)'].values)]
        self.df.drop('Date(dd-mm-yy)', axis=1, inplace=True)
        self.df.drop('Time(hh:mm:ss)', axis=1, inplace=True)
        self.df['datetime'] = dates
        con = (self.df.datetime >= self.dates[0]) & (self.df.datetime <= self.dates[-1])
        self.df = self.df[con]

    def calc_550nm(self, df):
        from numpy import log, NaN, arange, exp
        from scipy.optimize import curve_fit
        df[df < 0] = NaN
        dfnew = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
        cols = dfnew.columns
        aotcols = [s for s in cols if 'AOT' in s]
        lambdas = [int(s.split('_')[1]) for s in cols if 'AOT' in s]
        lamcols = [s.split('_')[1] for s in cols if 'AOT' in s]
        df2 = log(dfnew[aotcols])
        for s in lambdas:
            df2[str(s)] = log(s)
        vals = []
        for i in arange(df2[aotcols[0]].count()):
            popt, pcov = curve_fit(self.func, df2[lamcols].iloc[i].values, df2[aotcols].iloc[i].values)
            vals.append(popt[0] * log(550) ** 2 + popt[1] * log(550) + popt[-1])

        d = {'datetime': df.datetime, 'AOT_550': exp(vals), 'Latitude': df.Latitude, 'Logitude': df.Longitude,
             "Site_Name": df.Site_Name}
        df = pd.DataFrame(d)
        return df

    @staticmethod
    def func(x, a, b, c):
        return a * x ** 2 + b * x + c

    def get_station_df(self):
        self.station_df = pd.read_csv(self.station_file, skiprows=2, names=['Site_Name', 'Longitude', 'Latitude'],
                                      usecols=[0, 1, 2])

    def get_relavent_sites(self, lat=None, lon=None):
        if lon is None:
            lon = [-58, -136]
        if lat is None:
            lat = [16, 54]
        df = self.station_df
        con = (df.Latitude > min(lat)) & (df.Latitude < max(lat)) & (df.Longitude > min(lon)) & (
            df.Longitude < max(lon))
        self.station_df = df[con]

    def change_path(self):
        os.chdir(self.datadir)

    def change_back(self):
        os.chdir(self.cwd)

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end, freq='H').values.astype('M8[s]').astype('O')
        self.dates = dates
