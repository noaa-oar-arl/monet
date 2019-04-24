from __future__ import print_function

import inspect
import os

# this is written to retrive airnow data concatenate and add to pandas array
# for usage
from builtins import object
from datetime import datetime

import pandas as pd


class AirNow(object):
    """Short summary.

    Attributes
    ----------
    url : type
        Description of attribute `url`.
    dates : type
        Description of attribute `dates`.
    datestr : type
        Description of attribute `datestr`.
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

    def __init__(self):
        self.datadir = "."
        self.cwd = os.getcwd()
        self.url = None
        self.dates = [
            datetime.strptime("2016-06-06 12:00:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2016-06-06 13:00:00", "%Y-%m-%d %H:%M:%S"),
        ]
        self.datestr = []
        self.df = pd.DataFrame()
        self.daily = False
        self.objtype = "AirNow"
        self.filelist = None
        self.monitor_file = (
            inspect.getfile(self.__class__)[:-13] + "data/monitoring_site_locations.dat"
        )
        self.monitor_df = None
        self.savecols = [
            "time",
            "siteid",
            "site",
            "utcoffset",
            "variable",
            "units",
            "obs",
            "time_local",
            "latitude",
            "longitude",
            "cmsa_name",
            "msa_code",
            "msa_name",
            "state_name",
            "epa_region",
        ]

    def convert_dates_tofnames(self):
        """Helper function to create file names

        Returns
        -------


        """
        self.datestr = []
        for i in self.dates:
            self.datestr.append(i.strftime("%Y%m%d%H.dat"))

    def build_urls(self):
        """Short summary.

        Returns
        -------
        helper function to build urls

        """

        furls = []
        fnames = []
        print("Building AIRNOW URLs...")
        # 2017/20170131/HourlyData_2017012408.dat
        url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/"
        for i in self.dates:
            f = url + i.strftime("%Y/%Y%m%d/HourlyData_%Y%m%d%H.dat")
            fname = i.strftime("HourlyData_%Y%m%d%H.dat")
            furls.append(f)
            fnames.append(fname)
        # https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/2017/20170108/HourlyData_2016121506.dat

        # files needed for comparison
        self.url = pd.Series(furls, index=None)
        self.fnames = pd.Series(fnames, index=None)

    def read_csv(self, fn):
        """Short summary.

        Parameters
        ----------
        fn : string
            file name to read

        Returns
        -------
        type
            Description of returned object.

        """
        try:
            dft = pd.read_csv(
                fn,
                delimiter="|",
                header=None,
                error_bad_lines=False,
                encoding="ISO-8859-1",
            )
            cols = [
                "date",
                "time",
                "siteid",
                "site",
                "utcoffset",
                "variable",
                "units",
                "obs",
                "source",
            ]
            dft.columns = cols
        except Exception:
            cols = [
                "date",
                "time",
                "siteid",
                "site",
                "utcoffset",
                "variable",
                "units",
                "obs",
                "source",
            ]
            dft = pd.DataFrame(columns=cols)
        dft["obs"] = dft.obs.astype(float)
        dft["siteid"] = dft.siteid.str.zfill(9)
        dft["utcoffset"] = dft.utcoffset.astype(int)
        return dft

    def retrieve(self, url, fname):
        """Short summary.

        Parameters
        ----------
        url : type
            Description of parameter `url`.
        fname : type
            Description of parameter `fname`.

        Returns
        -------
        type
            Description of returned object.

        """
        import requests

        if not os.path.isfile(fname):
            print("\n Retrieving: " + fname)
            print(url)
            print("\n")
            r = requests.get(url)
            open(fname, "wb").write(r.content)
        else:
            print("\n File Exists: " + fname)

    def aggregate_files(self, download=False):
        """Short summary.

        Parameters
        ----------
        download : type
            Description of parameter `download` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        """
        import dask
        import dask.dataframe as dd

        print("Aggregating AIRNOW files...")
        self.build_urls()
        if download:
            for url, fname in zip(self.url, self.fnames):
                self.retrieve(url, fname)
            dfs = [dask.delayed(self.read_csv)(f) for f in self.fnames]
        else:
            dfs = [dask.delayed(self.read_csv)(f) for f in self.url]
        dff = dd.from_delayed(dfs)
        df = dff.compute()
        df["time"] = pd.to_datetime(
            df.date + " " + df.time, format="%m/%d/%y %H:%M", exact=True, box=False
        )
        df.drop(["date"], axis=1, inplace=True)
        df["time_local"] = df.time + pd.to_timedelta(df.utcoffset, unit="H")
        self.df = df
        print("    Adding in Meta-data")
        self.get_station_locations()
        self.df = self.df[self.savecols]
        self.df.drop_duplicates(inplace=True)
        self.filter_bad_values()

    def add_data(self, dates, download=False):
        """Short summary.

        Parameters
        ----------
        dates : type
            Description of parameter `dates`.
        download : type
            Description of parameter `download` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        """
        self.dates = dates
        self.aggregate_files(download=download)
        return self.df

    def filter_bad_values(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        from numpy import NaN

        self.df.loc[(self.df.obs > 1000) | (self.df.obs < 0), "obs"] = NaN

    def set_daterange(self, **kwargs):
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
        dates = pd.date_range(**kwargs)
        self.dates = dates
        return dates

    def get_station_locations(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        from .epa_util import read_monitor_file

        self.monitor_df = read_monitor_file(airnow=True)
        self.df = pd.merge(self.df, self.monitor_df, on="siteid")  # , how='left')

    def get_station_locations_remerge(self, df):
        """Short summary.

        Parameters
        ----------
        df : type
            Description of parameter `df`.

        Returns
        -------
        type
            Description of returned object.

        """
        df = pd.merge(
            df, self.monitor_df.drop(["Latitude", "Longitude"], axis=1), on="siteid"
        )  # ,
        # how='left')
        return df
