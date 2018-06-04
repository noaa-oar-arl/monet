from __future__ import print_function
import os
import pandas as pd
import numpy as np
import datetime


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


def lbs2kg(lbs):
    kg = 0.453592 * lbs
    return kg


class CEMSEmissions(object):
    """
    Class for data from continuous emission monitoring systems (CEMS).
     Data from power plants can be downloaded from ftp://newftp.epa.gov/DMDNLoad/emissions/

    Attributes
    ----------
    efile : type
        Description of attribute `efile`.
    url : type
        Description of attribute `url`.

    """

    def __init__(self):
        self.efile = None
        self.url = "ftp://newftp.epa.gov/DmDnLoad/emissions/"
        self.lb2kg = 0.453592  #number of kilograms per pound.
        self.info = "Data from continuous emission monitoring systems (CEMS) \n"
        self.info += self.url + '\n'
        self.df = None
        ##Each facility may have more than one unit which is specified by the unit id.

    def __str__(self):
        return self.info

    def add_data(self, rdate, states=['md'], download=False):
        """gets the ftp url from the retrieve method and then
           loads the data from the ftp site using the load method.

           rdate should either be a single datetime object or a list of two datetime objects.
           The first datetime object indicates the month and year of the first file to retrieve.
           The second datetime object indicates the month and year of the last file to retrieve.

           If download=True then retrieve will download the files and load
           will read the downloaded files.
           If download=False then retrieve will return the url and load will read directly from ftp site.
           TO DO add loop for adding multiple months.

        Parameters
        ----------
        rdate : type
            Description of parameter `rdate`.
        states : type
            Description of parameter `states`.
        download : type
            Description of parameter `download`.

        Returns
        -------
        type
            Description of returned object.

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
                if iii > 100: done = True
                iii += 1
        else:
            rdatelist = [rdates]
        for rd in rdatelist:
            print('getting data')
            print(rd)
            for st in states:
                url = self.retrieve(rd, st, download=download)
                self.load(url)

    def get_var(self, varname, loc=None, daterange=None):
        """returns time series with variable indicated by varname.

           routine looks for column which contains all strings in varname.
           Currently not case sensitive.

           TO DO filter for specified dates.

           loc must be list of FAC_ID's.

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
        if isinstance(varname, str):
            varname = (varname)
        columns = list(self.df.columns.values)
        #temp = self.df['OP_DATE', 'OP_HOUR', 'OP_TIME']
        #print(temp[0:10])
        if loc:
            temp = self.df[self.df['FAC_ID'].isin(loc)]
        if daterange:
            temp = temp[temp['datetime'] >= daterange[0]]
            temp = temp[temp['datetime'] <= daterange[1]]
        for ccc in columns:
            #print('-----'  + ccc + '------')
            #print( temp[ccc].unique())
            match = 0
            for vstr in varname:
                if vstr.lower() in ccc.lower():
                    match += 1
            if match == len(varname):
                cmatch = ccc
        temp.set_index(['datetime'], inplace=True)
        #print(temp)
        return temp[cmatch]

    def retrieve(self, rdate, state, download=True):
        """Short summary.

        Parameters
        ----------
        rdate : datetime object
             Uses year and month. Day and hour are not used.
        state : string
            state abbreviation to retrieve data for
        download : boolean
            set to True to download

        Returns
        -------
        type
            Description of returned object.

        """

        import requests
        efile = 'empty'
        ftpsite = self.url
        ftpsite += 'hourly/'
        ftpsite += 'monthly/'
        ftpsite += rdate.strftime("%Y") + '/'
        fname = rdate.strftime("%Y") + state + rdate.strftime("%m") + '.zip'
        if not download:
            return ftpsite + fname
        if not os.path.isfile(fname):
            print('retrieving ' + ftpsite + fname)
            r = requests.get(ftpsite + fname)
            open(efile, 'wb').write(r.content)
            print('retrieved ' + ftpsite + fname)
        else:
            print('file exists ' + fname)
            efile = fname
        self.info += 'File retrieved :' + efile + '\n'
        return efile

    def get_location(self, facid):
        """Need to create a comprehensive dictionary for all locations in the US."""
        lhash = {}
        lhash[314] = (39.1785, -76.5269)  #Herbert A Wagner
        lhash[110] = (getdegrees(39, 10, 53), getdegrees(-76, 32,
                                                         16))  #brandon shores
        lhash[312] = (getdegrees(39, 19, 25), getdegrees(-76, 21,
                                                         59))  #CP Crane
        lhash[322] = (getdegrees(38, 32, 37), getdegrees(-76, 41,
                                                         19))  #chalk point
        lhash[323] = (getdegrees(39, 12, 36), getdegrees(-77, 27,
                                                         54))  #dickerson
        lhash[324] = (getdegrees(38, 21, 33), getdegrees(-76, 58,
                                                         36))  #morgantown
        lhash[1116] = (getdegrees(39, 35, 46), getdegrees(-78, 44,
                                                          46))  #warrier run
        lhash[1229] = (38.670, -76.865)  #brandywine
        lhash[316] = (39.238, -76.5119)  #perryman
        self.lhash = lhash
        return lhash[facid]

    def load(self, efile, verbose=True):
        """loads information found in efile into a pandas dataframe.
        """
        dftemp = pd.read_csv(efile)
        columns = list(dftemp.columns.values)
        ckeep = []
        for ccc in columns:
            #print('-------------')
            #print(ccc)
            #print( dftemp[ccc].unique())
            if 'so2' in ccc.lower():
                ckeep.append(ccc)
            elif 'time' in ccc.lower():
                ckeep.append(ccc)
            elif 'date' in ccc.lower():
                ckeep.append(ccc)
            elif 'hour' in ccc.lower():
                ckeep.append(ccc)
            elif 'name' in ccc.lower():
                ckeep.append(ccc)
            elif 'id' in ccc.lower():
                ckeep.append(ccc)
            elif 'btu' in ccc.lower():
                ckeep.append(ccc)
        dftemp = dftemp[ckeep]
        cnan = ['SO2_MASS (lbs)']
        ##drop rows with NaN in the cnan column.
        dftemp.dropna(axis=0, inplace=True, subset=cnan)
        #print(dftemp['FACILITY_NAME'].unique())
        #print(dftemp['FAC_ID'].unique())
        pairs = zip(dftemp['FAC_ID'], dftemp['FACILITY_NAME'])
        pairs = list(set(pairs))
        #print(pairs)
        self.namehash = dict(pairs)  #key is facility id and value is name.

        ##create column with datetime information from column with month-day-year and column with hour.
        dftime = dftemp.apply(lambda x:pd.datetime.strptime("{0} {1}".format(x['OP_DATE'], x['OP_HOUR'])  , "%m-%d-%Y %H"), axis=1)
        dftemp = pd.concat([dftime, dftemp], axis=1)
        dftemp.rename(columns={0: 'datetime'}, inplace=True)
        dftemp.drop(['OP_DATE', 'OP_HOUR'], axis=1, inplace=True)

        if self.df is None:
            self.df = dftemp
            if verbose:
                print('Initializing pandas dataframe. Loading ' + efile)
        else:
            self.df = self.df.append(dftemp)
            if verbose:
                print('Appending to pandas dataframe. Loading ' + efile)
        return dftemp
