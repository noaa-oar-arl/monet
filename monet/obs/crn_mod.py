"""
Data is taken from the Climate Reference Network.  This is to expand validation
 of the NOAA ARL model validation leveraging inhouse datasets.

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
============================================
Daily Data
============================================
    Field#  Name                           Units
---------------------------------------------
   1    WBANNO                         XXXXX
   2    LST_DATE                       YYYYMMDD
   3    CRX_VN                         XXXXXX
   4    LONGITUDE                      Decimal_degrees
   5    LATITUDE                       Decimal_degrees
   6    T_DAILY_MAX                    Celsius
   7    T_DAILY_MIN                    Celsius
   8    T_DAILY_MEAN                   Celsius
   9    T_DAILY_AVG                    Celsius
   10   P_DAILY_CALC                   mm
   11   SOLARAD_DAILY                  MJ/m^2
   12   SUR_TEMP_DAILY_TYPE            X
   13   SUR_TEMP_DAILY_MAX             Celsius
   14   SUR_TEMP_DAILY_MIN             Celsius
   15   SUR_TEMP_DAILY_AVG             Celsius
   16   RH_DAILY_MAX                   %
   17   RH_DAILY_MIN                   %
   18   RH_DAILY_AVG                   %
   19   SOIL_MOISTURE_5_DAILY          m^3/m^3
   20   SOIL_MOISTURE_10_DAILY         m^3/m^3
   21   SOIL_MOISTURE_20_DAILY         m^3/m^3
   22   SOIL_MOISTURE_50_DAILY         m^3/m^3
   23   SOIL_MOISTURE_100_DAILY        m^3/m^3
   24   SOIL_TEMP_5_DAILY              Celsius
   25   SOIL_TEMP_10_DAILY             Celsius
   26   SOIL_TEMP_20_DAILY             Celsius
   27   SOIL_TEMP_50_DAILY             Celsius
   28   SOIL_TEMP_100_DAILY            Celsius

===============================================
 SUB HOURLY
 ==============================================
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
   9    AIR_TEMPERATURE                Celsius
   10   PRECIPITATION                  mm
   11   SOLAR_RADIATION                W/m^2
   12   SR_FLAG                        X
   13   SURFACE_TEMPERATURE            Celsius
   14   ST_TYPE                        X
   15   ST_FLAG                        X
   16   RELATIVE_HUMIDITY              %
   17   RH_FLAG                        X
   18   SOIL_MOISTURE_5                m^3/m^3
   19   SOIL_TEMPERATURE_5             Celsius
   20   WETNESS                        Ohms
   21   WET_FLAG                       X
   22   WIND_1_5                       m/s
   23   WIND_FLAG                      X

   """
from __future__ import print_function

import inspect
import os
from builtins import object, zip

import pandas as pd
from future import standard_library
from numpy import array

standard_library.install_aliases()


class crn(object):
    def __init__(self):
        self.dates = None
        self.daily = False
        self.ftp = None
        self.df = pd.DataFrame()
        self.se_states = array(
            ["AL", "FL", "GA", "MS", "NC", "SC", "TN", "VA", "WV"],
            dtype="|S14")
        self.ne_states = array(
            [
                "CT", "DE", "DC", "ME", "MD", "MA", "NH", "NJ", "NY", "PA",
                "RI", "VT"
            ],
            dtype="|S20",
        )
        self.nc_states = array(
            ["IL", "IN", "IA", "KY", "MI", "MN", "MO", "OH", "WI"],
            dtype="|S9")
        self.sc_states = array(["AR", "LA", "OK", "TX"], dtype="|S9")
        self.r_states = array(
            [
                "AZ", "CO", "ID", "KS", "MT", "NE", "NV", "NM", "ND", "SD",
                "UT", "WY"
            ],
            dtype="|S12",
        )
        self.p_states = array(["CA", "OR", "WA"], dtype="|S10")
        self.objtype = "CRN"
        self.monitor_file = inspect.getfile(
            self.__class__)[:-18] + "data/stations.tsv"
        self.monitor_df = None
        self.baseurl = "https://www1.ncdc.noaa.gov/pub/data/uscrn/products/"
        self.hcols = [
            "WBANNO",
            "UTC_DATE",
            "UTC_TIME",
            "LST_DATE",
            "LST_TIME",
            "CRX_VN",
            "LONGITUDE",
            "LATITUDE",
            "T_CALC",
            "T_AVG",
            "T_MAX",
            "T_MIN",
            "P_CALC",
            "SOLARAD",
            "SOLARAD_FLAG",
            "SOLARAD_MAX",
            "SOLARAD_MAX_FLAG",
            "SOLARAD_MIN",
            "SOLARAD_MIN_FLAG",
            "SUR_TEMP_TYPE",
            "SUR_TEMP",
            "SUR_TEMP_FLAG",
            "SUR_TEMP_MAX",
            "SUR_TEMP_MAX_FLAG",
            "SUR_TEMP_MIN",
            "SUR_TEMP_MIN_FLAG",
            "RH_AVG",
            "RH_AVG_FLAG",
            "SOIL_MOISTURE_5",
            "SOIL_MOISTURE_10",
            "SOIL_MOISTURE_20",
            "SOIL_MOISTURE_50",
            "SOIL_MOISTURE_100",
            "SOIL_TEMP_5",
            "SOIL_TEMP_10",
            "SOIL_TEMP_20",
            "SOIL_TEMP_50",
            "SOIL_TEMP_100",
        ]
        self.dcols = [
            "WBANNO",
            "LST_DATE",
            "CRX_VN",
            "LONGITUDE",
            "LATITUDE",
            "T_MAX",
            "T_MIN",
            "T_MEAN",
            "T_AVG",
            "P_CALC",
            "SOLARAD",
            "SUR_TEMP_TYPE",
            "SUR_TEMP_MAX",
            "SUR_TEMP_MAX",
            "SUR_TEMP_MIN",
            "SUR_TEMP_AVG",
            "RH_MAX",
            "RH_MIN",
            "RH_AVG",
            "SOIL_MOISTURE_5",
            "SOIL_MOISTURE_10",
            "SOIL_MOISTURE_20",
            "SOIL_MOISTURE_50",
            "SOIL_MOISTURE_100",
            "SOIL_TEMP_5",
            "SOIL_TEMP_10",
            "SOIL_TEMP_20",
            "SOIL_TEMP_50",
            "SOIL_TEMP_100",
        ]
        self.shcols = [
            "WBANNO",
            "UTC_DATE",
            "UTC_TIME",
            "LST_DATE",
            "LST_TIME",
            "CRX_VN",
            "LONGITUDE",
            "LATITUDE",
            "T_MEAN",
            "P_CALC",
            "SOLARAD",
            "SOLARAD_FLAG",
            "SUR_TEMP_AVG",
            "SUR_TEMP_TYPE",
            "SUR_TEMP_FLAG",
            "RH_AVG",
            "RH_FLAG",
            "SOIL_MOISTURE_5",
            "SOIL_TEMP_5",
            "WETNESS",
            "WET_FLAG",
            "WIND",
            "WIND_FLAG",
        ]
        self.citiation = "Diamond, H. J., T. R. Karl, M. A. Palecki, C. B. Baker, J. E. Bell, R. D. Leeper, D. R. Easterling, J. H. "
        " Lawrimore, T. P. Meyers, M. R. Helfert, G. Goodge, and P. W. Thorne,"
        " 2013: U.S. Climate Reference Network after one decade of operations:"
        " status and assessment. Bull. Amer. Meteor. Soc., 94, 489-498. "
        "doi: 10.1175/BAMS-D-12-00170.1"
        self.citation2 = "Bell, J. E., M. A. Palecki, C. B. Baker, W. G. "
        "Collins, J. H. Lawrimore, R. D. Leeper, M. E. Hall, J. Kochendorfer, "
        "T. P. Meyers, T. Wilson, and H. J. Diamond. 2013: U.S. Climate "
        "Reference Network soil moisture and temperature observations. J. "
        "Hydrometeorol., 14, 977-988. doi: 10.1175/JHM-D-12-0146.1"

    def load_file(self, url):
        nanvals = [-99999, -9999.0]
        if "CRND0103" in url:
            cols = self.dcols
            df = pd.read_csv(
                url,
                delim_whitespace=True,
                names=cols,
                parse_dates={"time_local": [1]},
                infer_datetime_format=True,
                na_values=nanvals,
            )
            self.daily = True
        elif "CRNS0101" in url:
            cols = self.shcols
            df = pd.read_csv(
                url,
                delim_whitespace=True,
                names=cols,
                parse_dates={
                    "time": ["UTC_DATE", "UTC_TIME"],
                    "time_local": ["LST_DATE", "LST_TIME"],
                },
                infer_datetime_format=True,
                na_values=nanvals,
            )
        else:
            cols = self.hcols
            df = pd.read_csv(
                url,
                delim_whitespace=True,
                names=cols,
                parse_dates={
                    "time": ["UTC_DATE", "UTC_TIME"],
                    "time_local": ["LST_DATE", "LST_TIME"],
                },
                infer_datetime_format=True,
                na_values=nanvals,
            )
        return df

    def build_url(self,
                  year,
                  state,
                  site,
                  vector,
                  daily=False,
                  sub_hourly=False):
        if daily:
            beginning = self.baseurl + "daily01/" + year + "/"
            fname = "CRND0103-"
        elif sub_hourly:
            beginning = self.baseurl + "subhourly01/" + year + "/"
            fname = "CRNS0101-05-"
        else:
            beginning = self.baseurl + "hourly02/" + year + "/"
            fname = "CRNH0203-"
        rest = year + "-" + state + "_" + site + "_" + vector + ".txt"
        url = beginning + fname + rest
        fname = fname + rest
        return url, fname

    @staticmethod
    def check_url(url):
        """Short summary.

        Parameters
        ----------
        url : type
            Description of parameter `url`.

        Returns
        -------
        type
            Description of returned object.

        """
        import requests

        if requests.head(url).status_code < 400:
            return True
        else:
            return False

    def build_urls(self, monitors, dates, daily=False, sub_hourly=False):
        """Short summary.

        Parameters
        ----------
        monitors : type
            Description of parameter `monitors`.
        dates : type
            Description of parameter `dates`.
        daily : type
            Description of parameter `daily` (the default is False).
        sub_hourly : type
            Description of parameter `sub_hourly` (the default is False).

        Returns
        -------
        type
            Description of returned object.

        """
        print("Building and checking urls...")
        years = pd.DatetimeIndex(dates).year.unique().astype(str)
        urls = []
        fnames = []
        for i in monitors.index:
            for y in years:
                state = monitors.iloc[i].STATE
                site = monitors.iloc[i].LOCATION.replace(" ", "_")
                vector = monitors.iloc[i].VECTOR.replace(" ", "_")
                url, fname = self.build_url(
                    y, state, site, vector, daily=daily, sub_hourly=sub_hourly)
                if self.check_url(url):
                    urls.append(url)
                    fnames.append(fname)
                    print(url)
        return urls, fnames

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
        """rdate - datetime object. Uses year and month. Day and hour are not used.
           state - state abbreviation to retrieve data for
           Files are by year month and state.
        """
        import wget

        if not os.path.isfile(fname):
            print("Retrieving: " + fname)
            print(url)
            print("\n")
            wget.download(url)
        else:
            print("File Exists: " + fname)

    def add_data(self,
                 dates,
                 daily=False,
                 sub_hourly=False,
                 download=False,
                 latlonbox=None):
        """Short summary.

        Parameters
        ----------
        dates : type
            Description of parameter `dates`.
        daily : type
            Description of parameter `daily` (the default is False).
        sub_hourly : type
            Description of parameter `sub_hourly` (the default is False).
        download : type
            Description of parameter `download` (the default is False).
        latlonbox : type
            Description of parameter `latlonbox` (the default is None).

        Returns
        -------
        type
            Description of returned object.

        """
        import dask
        import dask.dataframe as dd

        if self.monitor_df is None:
            self.get_monitor_df()
        if latlonbox is not None:  # get them all[latmin,lonmin,latmax,lonmax]
            mdf = self.monitor_df
            con = ((mdf.LATITUDE >= latlonbox[0])
                   & (mdf.LATITUDE <= latlonbox[2])
                   & (mdf.LONGITUDE >= latlonbox[1])
                   & (mdf.LONGITUDE <= latlonbox[3]))
            monitors = mdf.loc[con].copy()
        else:
            monitors = self.monitor_df.copy()
        urls, fnames = self.build_urls(
            monitors, dates, daily=daily, sub_hourly=sub_hourly)
        if download:
            for url, fname in zip(urls, fnames):
                self.retrieve(url, fname)
            dfs = [dask.delayed(self.load_file)(i) for i in fnames]
        else:
            dfs = [dask.delayed(self.load_file)(i) for i in urls]
        dff = dd.from_delayed(dfs)
        self.df = dff.compute()
        self.df = pd.merge(
            self.df,
            monitors,
            how="left",
            on=["WBANNO", "LATITUDE", "LONGITUDE"])
        if ~self.df.columns.isin(["time"]).max():
            self.df["time"] = self.df.time_local + pd.to_timedelta(
                self.df.GMT_OFFSET, unit="H")
        id_vars = self.monitor_df.columns.append(
            pd.Index(["time", "time_local"]))
        keys = self.df.columns[self.df.columns.isin(id_vars)]
        self.df = pd.melt(
            self.df, id_vars=keys, var_name="variable",
            value_name="obs")  # this stacks columns to be inline with MONET
        self.df.rename(columns={"WBANNO": "siteid"}, inplace=True)
        self.change_units()
        self.df.columns = [i.lower() for i in self.df.columns]

    def change_units(self):
        """Short summary.

        Parameters
        ----------
        df : type
            Description of parameter `df`.
        param : type
            Description of parameter `param` (the default is 'O3').
        aqs_param : type
            Description of parameter `aqs_param` (the default is 'OZONE').

        Returns
        -------
        type
            Description of returned object.

        """
        self.df["units"] = ""
        for i in self.df.variable.unique():
            if self.daily and i is "SOLARAD":
                self.df.loc[self.df.variable == i, "units"] = "MJ/m^2"
            elif "T_" in i:
                self.df.loc[self.df.variable == i, "units"] = "K"
                self.df.loc[self.df.variable == i, "obs"] += 273.15
            elif "FLAG" in i or "TYPE" in i:
                pass
            elif "TEMP" in i:
                self.df.loc[self.df.variable == i, "units"] = "K"
                self.df.loc[self.df.variable == i, "obs"] += 273.15
            elif "MOISTURE" in i:
                self.df.loc[self.df.variable == i, "units"] = "m^3/m^3"
            elif "RH" in i:
                self.df.loc[self.df.variable == i, "units"] = "%"
            elif "P_CALC" is i:
                self.df.loc[self.df.variable == i, "units"] = "mm"

    def set_daterange(self, begin="", end=""):
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
        dates = (pd.date_range(start=begin, end=end,
                               freq="H").values.astype("M8[s]").astype("O"))
        self.dates = dates

    def get_monitor_df(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        self.monitor_df = pd.read_csv(self.monitor_file, delimiter="\t")
