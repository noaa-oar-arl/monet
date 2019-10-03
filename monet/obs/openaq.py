"""Short summary.

    Attributes
    ----------
    url : type
        Description of attribute `url`.
    dates : type
        Description of attribute `dates`.
    df : type
        Description of attribute `df`.
    daily : type
        Description of attribute `daily`.
    objtype : type
        Description of attribute `objtype`.
    filelist : type
        Description of attribute `filelist`.
    monitor_file : type
        Description of attribute `monitor_file`.
    __class__ : type
        Description of attribute `__class__`.
    monitor_df : type
        Description of attribute `monitor_df`.
    savecols : type
        Description of attribute `savecols`.
    """
import json

import dask
import dask.dataframe as dd
import pandas as pd
from numpy import NaN, vectorize


def add_data(dates, n_procs=1):
    """add openaq data from the amazon s3 server.

    Parameters
    ----------
    dates : pd.DateTimeIndex or list of datatime objects
        this is a list of dates to download
    n_procs : type
        Description of parameter `n_procs`.

    Returns
    -------
    type
        Description of returned object.

    """
    a = OPENAQ()
    return a.add_data(dates, num_workers=n_procs)


class OPENAQ():
    def __init__(self):
        import s3fs
        from numpy import vectorize
        self.fs = s3fs.S3FileSystem(anon=True)
        self.s3bucket = 'openaq-fetches/realtime'

    def _get_available_days(self, dates):
        folders = self.fs.ls(self.s3bucket)
        days = [j.split('/')[2] for j in folders]
        avail_dates = pd.to_datetime(days, format='%Y-%m-%d', errors='coerce')
        dates = pd.to_datetime(dates).floor(freq='D')
        d = pd.Series(dates, name='dates').drop_duplicates()
        ad = pd.Series(avail_dates, name='dates')
        return pd.merge(d, ad, how='inner')

    def _get_files_in_day(self, date):
        files = self.fs.ls("{}/{}".format(self.s3bucket,
                                          date.strftime('%Y-%m-%d')))
        return files

    def build_urls(self, dates):
        d = self._get_available_days(dates)
        urls = pd.Series([], name='url')
        for i in d.dates:
            files = self._get_files_in_day(i)
            furls = pd.Series([
                f.replace('openaq-fetches',
                          'https://openaq-fetches.s3.amazonaws.com')
                for f in files
            ],
                name='url')
            urls = pd.merge(urls, furls, how='outer')
        return urls.url.values

    def add_data(self, dates, num_workers=1):
        import dask.dataframe as dd
        import dask

        urls = self.build_urls(dates).tolist()
        # z = dd.read_json(urls).compute()
        dfs = [dask.delayed(self.read_json)(f) for f in urls]
        dff = dd.from_delayed(dfs)
        z = dff.compute(num_workers=num_workers)
        z.coordinates.replace(to_replace=[None], value=pd.np.nan, inplace=True)
        z = z.dropna().reset_index(drop=True)
        js = json.loads(z[['coordinates', 'date']].to_json(orient='records'))
        dff = pd.io.json.json_normalize(js)
        dff.columns = dff.columns.str.split('.').str[1]
        dff.rename({
            'local': 'time_local',
            'utc': 'time'
        },
            axis=1,
            inplace=True)

        dff['time'] = pd.to_datetime(dff.time)
        dff['time_local'] = pd.to_datetime(dff.time_local)
        zzz = z.join(dff).drop(
            columns=['coordinates', 'date', 'attribution', 'averagingPeriod'])
        zp = self._pivot_table(zzz)
        zp['siteid'] = zp.country + '_' + zp.latitude.round(3).astype(
            str) + 'N_' + zp.longitude.round(3).astype(str) + 'E'
        return zp.loc[zp.time >= dates.min().tz_localize('utc')]

    def read_json(self, url):
        return pd.read_json(url, lines=True).dropna().sort_index(axis=1)

    # def read_json(self, url):
    #     df = pd.read_json(url, lines=True).dropna()
    #     df.coordinates.replace(to_replace=[None],
    #                            value=pd.np.nan,
    #                            inplace=True)
    #     df = df.dropna(subset=['coordinates'])
    #     # df = self._parse_latlon(df)
    #     # json_struct = json.loads(df.coordinates.to_json(orient='records'))
    #     # df_flat = pd.io.json.json_normalize(json_struct)
    #     # df = self._parse_datetime(df)
    #     # df = self._fix_units(df)
    #     # df = self._pivot_table(df)
    #     return df

    def _parse_latlon(self, df):
        # lat = vectorize(lambda x: x['latitude'])
        # lon = vectorize(lambda x: x['longitude'])
        def lat(x):
            return x['latitude']

        def lon(x):
            return x['longitude']

        df['latitude'] = df.coordinates.apply(lat)
        df['longitude'] = df.coordinates.apply(lon)
        return df.drop(columns='coordinates')

    def _parse_datetime(self, df):
        def utc(x):
            return pd.to_datetime(x['utc'])

        def local(x):
            return pd.to_datetime(x['local'])

        df['time'] = df.date.apply(utc)
        df['time_local'] = df.date.apply(local)
        return df.drop(columns='date')

    def _fix_units(self, df):
        df.loc[df.value <= 0] = NaN
        df.loc[(df.parameter == 'co') & (df.unit != 'ppm'), 'value'] /= 1145
        df.loc[(df.parameter == 'o3') & (df.unit != 'ppm'), 'value'] /= 2000
        df.loc[(df.parameter == 'so2') & (df.unit != 'ppm'), 'value'] /= 2620
        df.loc[(df.parameter == 'no2') & (df.unit != 'ppm'), 'value'] /= 1880
        return df

    def _pivot_table(self, df):
        w = df.pivot_table(values='value',
                           index=[
                               'time', 'latitude', 'longitude', 'sourceName',
                               'sourceType', 'city', 'country', 'time_local'
                           ],
                           columns='parameter').reset_index()
        w = w.rename(dict(co='co_ppm',
                          o3='o3_ppm',
                          no2='no2_ppm',
                          so2='so2_ppm',
                          bc='bc_umg3',
                          pm25='pm25_ugm3',
                          pm10='pm10_ugm3'),
                     axis=1)
        return w
