""" READS NAPD DATA """

from builtins import object

import pandas as pd
from numpy import NaN


def add_data(dates, network='NTN', siteid=None, weekly=True):
    n = NADP()
    df = n.add_data(dates, network=network, siteid=siteid, weekly=weekly)
    return df


class NADP(object):
    def __init__(self):
        self.weekly = True
        self.network = None
        self.df = pd.DataFrame()
        self.objtype = 'NADP'
        self.url = None

    def build_url(self, network='NTN', siteid=None):
        baseurl = 'http://nadp.slh.wisc.edu/datalib/'
        if siteid is not None:
            siteid = siteid.upper() + '-'
        else:
            siteid = ''
        if network.lower() == 'amnet':
            url = 'http://nadp.slh.wisc.edu/datalib/AMNet/AMNet-All.zip'
        elif network.lower() == 'amon':
            url = 'http://nadp.slh.wisc.edu/dataLib/AMoN/csv/all-ave.csv'
        elif network.lower() == 'airmon':
            url = 'http://nadp.slh.wisc.edu/datalib/AIRMoN/AIRMoN-ALL.csv'
        else:
            if self.weekly:
                url = baseurl + network.lower(
                ) + '/weekly/' + siteid + network.upper() + '-All-w.csv'
            else:
                url = baseurl + network.lower(
                ) + '/annual/' + siteid + network.upper() + '-All-a.csv'
        return url

    def network_names(self):
        print('Available Networks: AMNET, NTN, MDN, AIRMON, AMON')

    def read_ntn(self, url):
        print('Reading NADP-NTN Data...')
        print(url)
        # header = self.get_columns()
        df = pd.read_csv(url, infer_datetime_format=True, parse_dates=[2, 3])
        df.columns = [i.lower() for i in df.columns]
        df.rename(
            columns={
                'dateon': 'time',
                'dateoff': 'time_off'
            }, inplace=True)
        try:
            meta = pd.read_csv('https://bit.ly/2sPMvaO')
        except RuntimeError:
            meta = pd.read_csv(self.__path__ + '/../../data/ntn-sites.csv')
        meta.columns = [i.lower() for i in meta.columns]
        meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        dfn = pd.merge(df, meta, on='siteid', how='left')
        dfn.dropna(subset=['latitude', 'longitude'], inplace=True)
        dfn.loc[(dfn.flagmg == '<') | (dfn.mg < 0), 'mg'] = NaN
        dfn.loc[(dfn.flagbr == '<') | (dfn.br < 0), 'br'] = NaN
        dfn.loc[(dfn.flagso4 == '<') | (dfn.so4 < 0), 'so4'] = NaN
        dfn.loc[(dfn.flagcl == '<') | (dfn.cl < 0), 'cl'] = NaN
        dfn.loc[(dfn.flagno3 == '<') | (dfn.no3 < 0), 'no3'] = NaN
        dfn.loc[(dfn.flagnh4 == '<') | (dfn.nh4 < 0), 'nh4'] = NaN
        dfn.loc[(dfn.flagk == '<') | (dfn.k < 0), 'k'] = NaN
        dfn.loc[(dfn.flagna == '<') | (dfn.na < 0), 'na'] = NaN
        dfn.loc[(dfn.flagca == '<') | (dfn.ca < 0), 'ca'] = NaN
        return dfn

    def read_mdn(self, url):
        print('Reading NADP-MDN Data...')
        # header = self.get_columns()
        df = pd.read_csv(url, infer_datetime_format=True, parse_dates=[1, 2])
        df.columns = [i.lower() for i in df.columns]
        df.rename(
            columns={
                'dateon': 'time',
                'dateoff': 'time_off'
            }, inplace=True)
        try:
            meta = pd.read_csv('https://bit.ly/2Lq6kgq')
            meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        except RuntimeError:
            meta = pd.read_csv(self.__path__ + '/../../data/mdn-sites.csv')
            meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        meta.columns = [i.lower() for i in meta.columns]
        dfn = pd.merge(df, meta, on='siteid', how='left')
        dfn.dropna(subset=['latitude', 'longitude'], inplace=True)
        dfn.loc[dfn.qr ==
                'C', ['rgppt', 'svol', 'subppt', 'hgconc', 'hgdep']] = NaN
        return dfn

    def read_airmon(self, url):
        print('Reading NADP-AIRMoN Data...')
        # header = self.get_columns()
        df = pd.read_csv(url, infer_datetime_format=True, parse_dates=[2, 3])
        df.columns = [i.lower() for i in df.columns]
        df.rename(
            columns={
                'dateon': 'time',
                'dateoff': 'time_off'
            }, inplace=True)
        try:
            meta = pd.read_csv('https://bit.ly/2xMlgTW')
            meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        except RuntimeError:
            meta = pd.read_csv(self.__path__ + '/../../data/airmon-sites.csv')
            meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        meta.columns = [i.lower() for i in meta.columns]
        dfn = pd.merge(df, meta, on='siteid', how='left')
        dfn.dropna(subset=['latitude', 'longitude'], inplace=True)
        dfn.loc[dfn.qrcode == 'C', [
            'subppt', 'pptnws', 'pptbel', 'svol', 'ca', 'mg', 'k', 'na', 'nh4',
            'no3', 'cl', 'so4', 'po4', 'phlab', 'phfield', 'conduclab',
            'conducfield'
        ]] = NaN
        return dfn

    def read_amon(self, url):
        print('Reading NADP-AMoN Data...')
        # header = self.get_columns()
        df = pd.read_csv(url, infer_datetime_format=True, parse_dates=[2, 3])
        df.columns = [i.lower() for i in df.columns]
        df.rename(
            columns={
                'startdate': 'time',
                'enddate': 'time_off'
            }, inplace=True)
        try:
            meta = pd.read_csv('https://bit.ly/2sJmkCg')
            meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        except RuntimeError:
            meta = pd.read_csv(self.__path__ + '/../../data/amon-sites.csv')
            meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        meta.columns = [i.lower() for i in meta.columns]
        dfn = pd.merge(df, meta, on='siteid', how='left')
        dfn.dropna(subset=['latitude', 'longitude'], inplace=True)
        dfn.loc[dfn.qr == 'C', ['airvol', 'conc']] = NaN
        return dfn

    def read_amnet(self, url):
        print('Reading NADP-AMNet Data...')
        # header = self.get_columns()
        df = pd.read_csv(url, infer_datetime_format=True, parse_dates=[2, 3])
        df.columns = [i.lower() for i in df.columns]
        df.rename(
            columns={
                'startdate': 'time',
                'enddate': 'time_off'
            }, inplace=True)
        try:
            meta = pd.read_csv('https://bit.ly/2sJmkCg')
            meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        except RuntimeError:
            meta = pd.read_csv(self.__path__ + '/../../data/amnet-sites.csv')
            meta.drop(['startdate', 'stopdate'], axis=1, inplace=True)
        meta.columns = [i.lower() for i in meta.columns]
        dfn = pd.merge(df, meta, on='siteid', how='left')
        dfn.dropna(subset=['latitude', 'longitude'], inplace=True)
        dfn.loc[dfn.qr == 'C', ['airvol', 'conc']] = NaN
        return dfn

    def add_data(self, dates, network='NTN', siteid=None, weekly=True):
        url = self.build_url(network=network, siteid=siteid)
        if network.lower() == 'ntn':
            df = self.read_ntn(url)
        elif network.lower() == 'mdn':
            df = self.read_mdn(url)
        elif network.lower() == 'amon':
            df = self.read_amon(url)
        elif network.lower() == 'airmon':
            df = self.read_airmon(url)
        else:
            df = self.read_amnet(url)
        self.df = df
        self.df = self.df.loc[(self.df.time >= dates.min())
                              & (self.df.time_off <= dates.max())]

        return df

    def set_daterange(self, begin='', end=''):
        dates = pd.date_range(start=begin, end=end, freq='H')
        self.dates = dates
        return dates
