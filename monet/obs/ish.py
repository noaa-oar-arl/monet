"""Python module for reading NOAA ISH files"""
from __future__ import division
from __future__ import print_function

from future import standard_library

standard_library.install_aliases()
from builtins import zip
from builtins import object
from past.utils import old_div
import io
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
import os
import gzip
from dask.diagnostics import ProgressBar

ProgressBar().register()


class ish(object):
    def __init__(self):
        self.WIDTHS = [4, 11, 8, 4, 1, 6, 7, 5, 5, 5, 4, 3, 1,
                       1, 4, 1, 5, 1, 1, 1, 6, 1, 1, 1, 5, 1, 5, 1, 5, 1]
        self.DTYPES = [('varlength', 'i2'),
                       ('station_id', 'S11'),
                       ('date', 'i4'),
                       ('time', 'i2'),
                       ('source_flag', 'S1'),
                       ('latitude', 'float'),
                       ('longitude', 'float'),
                       ('code', 'S5'),
                       ('elev', 'i2'),
                       ('call_letters', 'S5'),
                       ('qc_process', 'S4'),
                       ('wdir', 'i2'),
                       ('wdir_quality', 'S1'),
                       ('wdir_type', 'S1'),
                       ('ws', 'i2'),
                       ('ws_quality', 'S1'),
                       ('ceiling', 'i4'),
                       ('ceiling_quality', 'S1'),
                       ('ceiling_code', 'S1'),
                       ('ceiling_cavok', 'S1'),
                       ('vsb', 'i4'),
                       ('vsb_quality', 'S1'),
                       ('vsb_variability', 'S1'),
                       ('vsb_variability_quality', 'S1'),
                       ('t', 'i2'),
                       ('t_quality', 'S1'),
                       ('dpt', 'i2'),
                       ('dpt_quality', 'S1'),
                       ('p', 'i4'),
                       ('p_quality', 'S1')]
        self.NAMES, _ = list(zip(*self.DTYPES))
        self.history_file = 'https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv'
        self.history = None
        self.dates = pd.to_datetime(['2017-07-09', '2017-09-10'])

    def delimit(self, file_object, delimiter=','):
        """Iterate over the lines in a file yielding comma delimited versions.

        Arguments
        ---------
        file_object : file or filename

        """

        try:
            file_object = open(file_object)
        except TypeError:
            pass

        for line in file_object:
            items = []
            index = 0
            for w in self.WIDTHS:
                items.append(line[index:index + w])
                index = index + w
            yield ','.join(items)

    def _clean_column(self, series, missing=9999, multiplier=1):
        series = series.apply(float)
        series[series == missing] = np.nan
        return old_div(series, multiplier)

    def _clean_column_by_name(self, frame, name, *args, **kwargs):
        frame[name] = self._clean_column(frame[name], *args, **kwargs)
        return frame

    def _clean(self, frame):
        """Clean up the data frame"""

        # index by time
        frame['datetime'] = [pd.Timestamp('{:08}{:04}'.format(date, time))
                             for date, time in zip(frame['date'], frame['time'])]
        frame.set_index('datetime', drop=True, inplace=True)

        frame = self._clean_column_by_name(frame, 'wdir', missing=999)
        frame = self._clean_column_by_name(frame, 'ws', multiplier=10)
        frame = self._clean_column_by_name(frame, 'ceiling', missing=99999)
        frame = self._clean_column_by_name(frame, 'vsb', missing=999999)
        frame = self._clean_column_by_name(frame, 't', multiplier=10)
        frame = self._clean_column_by_name(frame, 'dpt', multiplier=10)
        frame = self._clean_column_by_name(
            frame, 'p', multiplier=10, missing=99999)
        return frame

    def read_data_frame(self, file_object):
        """Create a data frame from an ISH file."""

        frame_as_array = np.genfromtxt(
            file_object, delimiter=self.WIDTHS, dtype=self.DTYPES)

        frame = pd.DataFrame.from_records(frame_as_array)

        df = self._clean(frame)
        df.drop(['latitude', 'longitude'], axis=1, inplace=True)
        #        df.latitude = self.history.groupby('station_id').get_group(df.station_id[0]).LAT.values[0]
        #        df.longitude = self.history.groupby('station_id').get_group(df.station_id[0]).LON.values[0]
        #        df['STATION_NAME'] = self.history.groupby('station_id').get_group(df.station_id[0])['STATION NAME'].str.strip().values[0]
        index = (df.index >= self.dates.min()) & (df.index <= self.dates.max())

        return df.loc[index, :].reset_index()

    def read_ish_history(self):
        """ read ISH history file """
        fname = self.history_file
        self.history = pd.read_csv(
            fname, parse_dates=['BEGIN', 'END'], infer_datetime_format=True)
        # & (self.history.END <= self.dates.max())
        index = (self.history.END >= self.dates.min())
        self.history = self.history.loc[index, :].dropna(subset=['LAT', 'LON'])
        self.history.loc[:, 'USAF'] = self.history.USAF.astype(
            'str').str.zfill(6)
        self.history.loc[:, 'WBAN'] = self.history.WBAN.astype(
            'str').str.zfill(5)
        self.history['station_id'] = self.history.USAF + self.history.WBAN
        self.history.rename(
            columns={'LAT': 'latitude', 'LON': 'longitude'}, inplace=True)

    def subset_sites(self, latmin=32.65, lonmin=-113.3, latmax=34.5, lonmax=-110.4):
        """ find sites within designated region"""
        latindex = (self.history.LAT >= latmin) & (self.history.LAT <= latmax)
        lonindex = (self.history.LON >= lonmin) & (self.history.LON <= lonmax)
        dfloc = self.history.loc[latindex & lonindex, :]
        return dfloc

    def read_sites(self, box=None, country=None, state=None, site=None, resample=True, window='H'):
        import urllib.request
        import urllib.error
        import urllib.parse
        from numpy import NaN
        i = self.dates[0]
        year = i.strftime('%Y')
        url = 'https://www1.ncdc.noaa.gov/pub/data/noaa/' + year + '/'
        if self.history is None:
            self.read_ish_history()
        self.history['fname'] = url + self.history.USAF + \
            '-' + self.history.WBAN + '-' + year + '.gz'
        dfloc = self.history.copy()
        if type(box) is not type(None):
            print('Retrieving Sites in: ' + box)
            dfloc = self.subset_sites(
                latmin=box[0], lonmin=box[1], latmax=box[2], lonmax=box[3])
        elif country is not None:
            print('Retrieving Country: ' + country)
            dfloc = self.history.loc[self.history.CTRY == country, :]
        elif state is not None:
            print('Retrieving State: ' + state)
            dfloc = self.history.loc[self.history.STATE == state, :]
        elif type(site) is not type(None):
            print('Retrieving Site: ' + site)
            dfloc = self.history.loc[self.history.station_id == site, :]
        print(dfloc.fname.unique())
        objs = self.get_url_file_objs(dfloc.fname.unique())
        # return objs,size,self.history.fname
        # dfs = []
        # for f in objs:
        #     try:
        #         dfs.append(self.read_data_frame(f))
        #     except:
        #         pass

        print('  Reading ISH into pandas DataFrame...')
        dfs = [dask.delayed(self.read_data_frame)(f) for f in objs]
        dff = dd.from_delayed(dfs)
        self.df = dff.compute()
        self.df.loc[self.df.vsb == 99999, 'vsb'] = NaN
        if resample:
            print('  Resampling to every ' + window)
            self.df.index = self.df.datetime
            self.df = self.df.groupby('station_id').resample(
                'H').mean().reset_index()
        self.df = self.df.merge(self.history[['station_id', 'latitude', 'longitude', 'STATION NAME']],
                                on=['station_id'], how='left')

    def get_url_file_objs(self, fname):
        from io import StringIO
        import urllib.request
        import urllib.error
        import urllib.parse
        import gzip
        objs = []
        size = []
        print('  Contstructing ISH file objects from urls...')
        for i in fname:
            #            print i
            try:
                request = urllib.request.Request(i)
                response = urllib.request.urlopen(request)
                buf = StringIO(response.read())
                if buf.len > 300:
                    objs.append(gzip.GzipFile(fileobj=buf))
            except:
                pass

                #        requests = [urllib2.Request(i) for i in fname]
                #        responses = [urllib2.urlopen(i) for i in requests]
                #        bufs = [i.read() for i in responses]
        return objs
