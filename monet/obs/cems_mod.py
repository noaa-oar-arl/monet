from __future__ import print_function
import os
import datetime
import pandas as pd
import numpy as np

"""
NAME: cems_mod.py
PGRMMER: Alice Crawford   ORG: ARL
This code written at the NOAA air resources laboratory
Python 3
#################################################################
"""


def getdegrees(degrees, minutes, seconds):
    return degrees + minutes / 60.0 + seconds / 3600.00


def addmonth(dt):
    month = dt.month + 1
    year = dt.year
    day = dt.day
    hour = dt.hour
    if month > 12:
        year = dt.year + 1
        month = month - 12
        if day == 31 and month in [4, 6, 9, 11]:
            day = 30
        if month == 2 and day in [29, 30, 31]:
            if year % 4 == 0:
                day = 29
            else:
                day = 28
    return datetime.datetime(year, month, day, hour)


def get_date_fmt(date, verbose=False):
    """Determines what format of the date is in.
    In some files year is first and others it is last.
    Parameters
    ----------
    date: str
          with format either YYYY-mm-DD or mm-DD-YYYY
    verbose: boolean
          if TRUE print extra information
    Rerturns
    --------
    fmt: str
        string which can be used with datetime object to give format of date
        string.
    """
    if verbose:
        print("Determining date format")
    if verbose:
        print(date)
    temp = date.split("-")
    if len(temp[0]) == 4:
        fmt = "%Y-%m-%d %H"
    else:
        fmt = "%m-%d-%Y %H"
    return fmt


class CEMS(object):
    """
    Class for data from continuous emission monitoring systems (CEMS).
    Data from power plants can be downloaded from
    ftp://newftp.epa.gov/DMDNLoad/emissions/

   Attributes
    ----------
    efile : type string
        Description of attribute `efile`.
    url : type string
        Description of attribute `url`.
    info : type string
        Information about data.
    df : pandas DataFrame
        dataframe containing emissions data.
   Methods
    ----------
    __init__(self)
    add_data(self, rdate, states=['md'], download=False, verbose=True):
    load(self, efile, verbose=True):
    retrieve(self, rdate, state, download=True):

    match_column(self, varname):
    get_var(self, varname, loc=None, daterange=None, unitid=-99, verbose=True):
    retrieve(self, rdate, state, download=True):
    create_location_dictionary(self):
    rename(self, ccc, newname, rcolumn, verbose):
    """

    def __init__(self):
        self.efile = None
        self.url = "ftp://newftp.epa.gov/DmDnLoad/emissions/"
        self.lb2kg = 0.453592  # number of kilograms per pound.
        self.info = "Data from continuous emission monitoring systems (CEMS)\n"
        self.info += self.url + "\n"
        self.df = pd.DataFrame()
        self.namehash = {}  # if columns are renamed keeps track of original names.
        # Each facility may have more than one unit which is specified by the
        # unit id.

    def __str__(self):
        return self.info

    def add_data(self, rdate, states=["md"], download=False, verbose=True):
        """
           gets the ftp url from the retrieve method and then
           loads the data from the ftp site using the load method.

        Parameters
        ----------
        rdate : single datetime object of list of datetime objects
               The first datetime object indicates the month and year of the
               first file to retrieve.
               The second datetime object indicates the month and year of the
               last file to retrieve.
        states : list of strings
             list of two letter state identifications.
        download : boolean
               if download=True then retrieve will download the files and load
               will read the downloaded files.
               if download=False then retrieve will return the url and load
               will read directly from ftp site.
        verbose : boolean
               if TRUE prints out additional information.
        Returns
        -------
        boolean True

        """
        if isinstance(states, str):
            states = [states]
        if isinstance(rdate, list):
            r1 = rdate[0]
            r2 = rdate[1]
            rdatelist = [r1]
            done = False
            iii = 0
            while not done:
                r3 = addmonth(rdatelist[-1])
                if r3 <= r2:
                    rdatelist.append(r3)
                else:
                    done = True
                if iii > 100:
                    done = True
                iii += 1
        else:
            rdatelist = [rdate]
        for rd in rdatelist:
            print("getting data")
            print(rd)
            for st in states:
                url = self.retrieve(rd, st, download=download, verbose=verbose)
                self.load(url, verbose=verbose)
        return self.df

    def match_column(self, varname):
        """varname is list of strings.
           returns column name which contains all the strings.
        """
        columns = list(self.df.columns.values)
        cmatch = None
        for ccc in columns:
            # print('-----'  + ccc + '------')
            # print( temp[ccc].unique())
            match = 0
            for vstr in varname:
                if vstr.lower() in ccc.lower():
                    match += 1
            if match == len(varname):
                cmatch = ccc
        return cmatch

    def cemspivot(self, varname, daterange=None, unitid=False, verbose=True):
        """
        Parameters
        ----------
        varname: string
            name of column in the cems dataframe
        daterange: list of two datetime objects
            define a date range
        unitid: boolean.
                 If True and unit id columns exist then these will be kept as
                 separate columns in the pivot table.
        verbose: boolean
                 if true print out extra information.
        Returns: pandas DataFrame object
            returns dataframe with rows time. Columns are (orispl_code,
            unit_id).
            If no unit_id in the file then columns are just orispl_code.
            if unitid flag set to False then sums over unit_id's that belong to
             an orispl_code. Values are from the column specified by the
             varname input.
        """

        from .obs_util import timefilter

        temp = self.df.copy()
        if daterange:
            temp = timefilter(temp, daterange)
        if "unit_id" in temp.columns.values and unitid:
            if temp["unit_id"].unique():
                if verbose:
                    print("UNIT IDs ", temp["unit_id"].unique())
            # create pandas frame with index datetime and columns for value for
            # each unit_id,orispl
            pivot = pd.pivot_table(
                temp,
                values=varname,
                index=["time"],
                columns=["orispl_code", "unit_id"],
                aggfunc=np.sum,
            )
        else:
            if verbose:
                print("NO UNIT ID")
            # returns data frame where rows are date and columns are the values
            # of cmatch for orispl
            pivot = pd.pivot_table(
                temp,
                values=varname,
                index=["time"],
                columns=["orispl_code"],
                aggfunc=np.sum,
            )
        return pivot

    def get_var(self, varname, orisp=None, daterange=None, unitid=-99, verbose=True):
        """
           returns time series with variable indicated by varname.
           returns data frame where rows are date and columns are the
           values of cmatch for each fac_id.

           routine looks for column which contains all strings in varname.
           Currently not case sensitive.

           loc and ORISPL CODES.
           unitid is a unit_id

           if a particular unitid is specified then will return values for that
            unit.


        Parameters
        ----------
        varname : string or iteratable of strings
            varname may be string or list of strings.
        loc : type
            Description of parameter `loc`.
        daterange : type
            Description of parameter `daterange`.

        Returns
        -------
        type
            Description of returned object.
        """
        if unitid == -99:
            ui = False
        temp = self.cemspivot(varname, daterange, unitid=ui)
        if not ui:
            return temp[orisp]
        else:
            return temp[orisp, unitid]

    def retrieve(self, rdate, state, download=True, verbose=False):
        """Short summary.

        Parameters
        ----------
        rdate : datetime object
             Uses year and month. Day and hour are not used.
        state : string
            state abbreviation to retrieve data for
        download : boolean
            set to True to download
            if download FALSE then returns string with url of ftp
            if download TRUE then returns name of downloaded file

        Returns
        -------
        efile string
            if download FALSE then returns string with url of ftp
            if download TRUE then returns name of downloaded file
        """
        # import requests
        # TO DO: requests does not support ftp sites.
        efile = "empty"
        ftpsite = self.url
        ftpsite += "hourly/"
        ftpsite += "monthly/"
        ftpsite += rdate.strftime("%Y") + "/"
        print(ftpsite)
        print(rdate)
        print(state)
        fname = rdate.strftime("%Y") + state + rdate.strftime("%m") + ".zip"
        if not download:
            efile = ftpsite + fname
        if not os.path.isfile(fname):
            # print('retrieving ' + ftpsite + fname)
            # r = requests.get(ftpsite + fname)
            # open(efile, 'wb').write(r.content)
            # print('retrieved ' + ftpsite + fname)
            efile = ftpsite + fname
            print("WARNING: Downloading file not supported at this time")
            print("you may download manually using the following address")
            print(efile)
        else:
            print("file exists " + fname)
            efile = fname
        self.info += "File retrieved :" + efile + "\n"
        return efile

    def create_location_dictionary(self, verbose=False):
        """
        returns dictionary withe key orispl_code and value  (latitude,
        longitude) tuple
        """
        if "latitude" in list(self.df.columns.values):
            dftemp = self.df.copy()
            pairs = zip(
                dftemp["orispl_code"], zip(dftemp["latitude"], dftemp["longitude"])
            )
            pairs = list(set(pairs))
            lhash = dict(pairs)  # key is facility id and value is name.
            if verbose:
                print(lhash)
            return lhash
        else:
            return False

    def create_name_dictionary(self, verbose=False):
        """
        returns dictionary withe key orispl_code and value facility name
        """
        if "latitude" in list(self.df.columns.values):
            dftemp = self.df.copy()
            pairs = zip(dftemp["orispl_code"], dftemp["facility_name"])
            pairs = list(set(pairs))
            lhash = dict(pairs)  # key is facility id and value is name.
            if verbose:
                print(lhash)
            return lhash
        else:
            return False

    def columns_rename(self, columns, verbose=False):
        """
        Maps columns with one name to a standard name
        Parameters:
        ----------
        columns: list of strings

        Returns:
        --------
        rcolumn: list of strings
        """
        rcolumn = []
        for ccc in columns:
            if "facility" in ccc.lower() and "name" in ccc.lower():
                rcolumn = self.rename(ccc, "facility_name", rcolumn, verbose)
            elif "orispl" in ccc.lower():
                rcolumn = self.rename(ccc, "orispl_code", rcolumn, verbose)
            elif "facility" in ccc.lower() and "id" in ccc.lower():
                rcolumn = self.rename(ccc, "fac_id", rcolumn, verbose)
            elif (
                "so2" in ccc.lower()
                and ("lbs" in ccc.lower() or "pounds" in ccc.lower())
                and ("rate" not in ccc.lower())
            ):
                rcolumn = self.rename(ccc, "so2_lbs", rcolumn, verbose)
            elif (
                "nox" in ccc.lower()
                and ("lbs" in ccc.lower() or "pounds" in ccc.lower())
                and ("rate" not in ccc.lower())
            ):
                rcolumn = self.rename(ccc, "nox_lbs", rcolumn, verbose)
            elif "co2" in ccc.lower() and (
                "short" in ccc.lower() and "tons" in ccc.lower()
            ):
                rcolumn = self.rename(ccc, "co2_short_tons", rcolumn, verbose)
            elif "date" in ccc.lower():
                rcolumn = self.rename(ccc, "date", rcolumn, verbose)
            elif "hour" in ccc.lower():
                rcolumn = self.rename(ccc, "hour", rcolumn, verbose)
            elif "lat" in ccc.lower():
                rcolumn = self.rename(ccc, "latitude", rcolumn, verbose)
            elif "lon" in ccc.lower():
                rcolumn = self.rename(ccc, "longitude", rcolumn, verbose)
            elif "state" in ccc.lower():
                rcolumn = self.rename(ccc, "state_name", rcolumn, verbose)
            else:
                rcolumn.append(ccc.strip().lower())
        return rcolumn

    def rename(self, ccc, newname, rcolumn, verbose):
        """
        keeps track of original and new column names in the namehash attribute
        Parameters:
        ----------
        ccc: str
        newname: str
        rcolumn: list of str
        verbose: boolean
        Returns
        ------
        rcolumn: list of str
        """
        # dictionary with key as the newname and value as the original name
        self.namehash[newname] = ccc
        rcolumn.append(newname)
        if verbose:
            print(ccc + " to " + newname)
        return rcolumn

    def add_info(self, dftemp):
        """
        -------------Load supplmental data-----------------------
        Add location (latitude longitude) and time UTC information to dataframe
         dftemp.
        cemsinfo.csv contains info on facility id, lat, lon, time offset from
         UTC.
        allows transformation from local time to UTC.
        If not all power stations are found in the cemsinfo.csv file,
        then Nan will be written in lat, lon and 'time' column.

        Parameters
        ----------
        dftemp: pandas dataframe

        Returns
        ----------
        dftemp: pandas dataframe
        """
        basedir = os.path.abspath(os.path.dirname(__file__))[:-3]
        iname = os.path.join(basedir, "data", "cemsinfo.csv")
        # iname = os.path.join(basedir, 'data', 'cem_facility_loc.csv')
        method = 1
        # TO DO: Having trouble with pytest throwing an error when using the
        # apply on the dataframe.
        # runs ok, but pytest fails. Tried several differnt methods.
        if os.path.isfile(iname):
            sinfo = pd.read_csv(iname, sep=",", header=0)
            try:
                dftemp.drop(["latitude", "longitude"], axis=1, inplace=True)
            except Exception:
                pass
            dfnew = pd.merge(
                dftemp,
                sinfo,
                how="left",
                left_on=["orispl_code"],
                right_on=["orispl_code"],
            )
            # print('---------z-----------')
            # print(dfnew.columns.values)
            # remove stations which do not have a time offset.
            dfnew.dropna(axis=0, subset=["time_offset"], inplace=True)
            if method == 1:
                # this runs ok but fails pytest
                def i2o(x):
                    return datetime.timedelta(hours=x["time_offset"])

                dfnew["time_offset"] = dfnew.apply(i2o, axis=1)
                dfnew["time"] = dfnew["time local"] + dfnew["time_offset"]
            elif method == 2:
                # this runs ok but fails pytest
                def utc(x):
                    return pd.Timestamp(x["time local"]) + datetime.timedelta(
                        hours=x["time_offset"]
                    )

                dfnew["time"] = dfnew.apply(utc, axis=1)
            elif method == 3:
                # this runs ok but fails pytest
                def utc(x, y):
                    return x + datetime.timedelta(hours=y)

                dfnew["time"] = dfnew.apply(
                    lambda row: utc(row["time local"], row["time_offset"]), axis=1
                )
            # remove the time_offset column.
            dfnew.drop(["time_offset"], axis=1, inplace=True)
            mlist = dftemp.columns.values.tolist()
            # merge the dataframes back together to include rows with no info
            # in the cemsinfo.csv
            dftemp = pd.merge(dftemp, dfnew, how="left", left_on=mlist, right_on=mlist)
        return dftemp
        # return dfnew

    def load(self, efile, verbose=True):
        """
        loads information found in efile into a pandas dataframe.
        Parameters
        ----------
        efile: string
             name of csv file to open or url of csv file.
        verbose: boolean
             if TRUE prints out information
        """

        # pandas read_csv can read either from a file or url.
        dftemp = pd.read_csv(efile, sep=",", index_col=False, header=0)
        columns = list(dftemp.columns.values)
        columns = self.columns_rename(columns, verbose)
        dftemp.columns = columns
        if verbose:
            print(columns)
        dfmt = get_date_fmt(dftemp["date"][0], verbose=verbose)

        # create column with datetime information
        # from column with month-day-year and column with hour.
        dftime = dftemp.apply(
            lambda x: pd.datetime.strptime(
                "{0} {1}".format(x["date"], x["hour"]), dfmt
            ),
            axis=1,
        )
        dftemp = pd.concat([dftime, dftemp], axis=1)
        dftemp.rename(columns={0: "time local"}, inplace=True)
        dftemp.drop(["date", "hour"], axis=1, inplace=True)

        # -------------Load supplmental data-----------------------
        # contains info on facility id, lat, lon, time offset from UTC.
        # allows transformation from local time to UTC.
        dftemp = self.add_info(dftemp)

        if ["year"] in columns:
            dftemp.drop(["year"], axis=1, inplace=True)
        if self.df.empty:
            self.df = dftemp
            if verbose:
                print("Initializing pandas dataframe. Loading " + efile)
        else:
            self.df = self.df.append(dftemp)
            if verbose:
                print("Appending to pandas dataframe. Loading " + efile)
        # if verbose: print(dftemp[0:10])
        return dftemp
