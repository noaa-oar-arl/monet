from __future__ import print_function
import os
import pandas as pd
import numpy as np
import datetime
import sys

###NAME: cems.py
###PGRMMER: Alice Crawford   ORG: ARL
###This code written at the NOAA air resources laboratory
###Python 3

##Record of changes

#####################################################################


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


class CEMSEmissions(object):
    """
    Class for data from continuous emission monitoring systems (CEMS).
    Data from power plants can be downloaded from ftp://newftp.epa.gov/DMDNLoad/emissions/
 
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
    columns_rename(self, columns, verbose=False):
    get_date_fmt(self, date):
    """

    def __init__(self):
      self.efile = None   
      self.url = "ftp://newftp.epa.gov/DmDnLoad/emissions/"
      self.lb2kg = 0.453592  #number of kilograms per pound.
      self.info = "Data from continuous emission monitoring systems (CEMS) \n"
      self.info += self.url + '\n'
      self.df = pd.DataFrame()
      self.namehash = {}  #if columns are renamed keeps track of original names.
      ##Each facility may have more than one unit which is specified by the unit id.


    def __str__(self):
        return self.info

    def add_data(self, rdate, states=['md'], download=False, verbose=True):
        """
           gets the ftp url from the retrieve method and then
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
        rdate : list of datetime objects
            Description of parameter `rdate`.
        states : list of strings
             list of two letter state identifications.
        download : boolean
            Description of parameter `download`.
        verbose : boolean
            if TRUE prints out additional information.
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
            rdatelist = [rdate]
        for rd in rdatelist:
            print('getting data')
            print(rd)
            for st in states:
                url = self.retrieve(rd, st, download=download, verbose=verbose)
                self.load(url, verbose=verbose)

    def match_column(self, varname):
        """varname is list of strings.
           returns column name which contains all the strings.
        """
        columns =  list(self.df.columns.values)
        cmatch = None
        for ccc in columns:
            #print('-----'  + ccc + '------')
            #print( temp[ccc].unique())
            match=0
            for vstr in varname:
                if vstr.lower() in ccc.lower():
                   match+=1
            if match == len(varname):
               cmatch = ccc
        return cmatch 
 
    def get_var(self, varname, loc=None, daterange=None, unitid=-99, verbose=True):
        """returns time series with variable indicated by varname.
           returns data frame where rows are date and columns are the values of cmatch for each fac_id.

           routine looks for column which contains all strings in varname.
           Currently not case sensitive.
     
           loc must be list of ORISPL CODES.

           TO DO - each FAC_ID may have several UNIT_ID, each of which
           corresponds to a different unit at the facility. Need to handle this.
           Either return separately or add together?

           Each facility may have more than one unit. If unitid=-99 then this
           method returns sum from all units.

           if a particular unitid is specified then will return values for that unit.

           TO DO if unitid -999 then will return a dictionary where key is the unit and value
           is a panda time series for that unit.

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
        from obs_util import timefilter
        if isinstance(varname, str):
            varname = (varname)
        columns = list(self.df.columns.values)
       
        if loc:
            temp = self.df[self.df['orispl_code'].isin(loc)]
        else:
            temp = self.df.copy()
        temp = timefilter(temp, daterange)
        cmatch = self.match_column(varname)
        if 'unit_id' in columns:
            ##create pandas frame with index datetime and columns for value for each unit_id
            pivot = pd.pivot_table(temp, values=cmatch, index=['time'], columns=['unit_id'])
            pivot = pivot.fillna(value=0)
            #print('------------------pivot')
            #print(pivot[0:10])
            if unitid == -99:
                pivot['all'] = pivot.sum(axis=1)
                #print(pivot[0:10])
                return pivot['all']       
            else:
                return pivot['unitid']  
        else:
            #temp.set_index('time', inplace=True)
            ##returns data frame where rows are date and columns are the values of cmatch for each fac_id.
            pivot = pd.pivot_table(temp, values=cmatch, index=['time'], columns = ['orispl_code'])
            ldict = self.create_location_dictionary()
            if ldict:
                cnew = []
                columns =  list(pivot.columns.values)
                for ccc in columns:
                    cnew.append(ldict[ccc])
            
            pivot.columns = cnew
            return pivot 
            #return temp[cmatch]

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

        #import requests
        #TO DO: requests does not support ftp sites.
        efile = 'empty'
        ftpsite = self.url
        ftpsite += 'hourly/'
        ftpsite += 'monthly/'
        ftpsite += rdate.strftime("%Y") + '/'
        print(ftpsite)
        print(rdate)
        print(state)
        fname = rdate.strftime("%Y") + state + rdate.strftime("%m") + '.zip'
        if not download:
            efile = ftpsite + fname
        if not os.path.isfile(fname):
            #print('retrieving ' + ftpsite + fname)
            #r = requests.get(ftpsite + fname)
            #open(efile, 'wb').write(r.content)
            #print('retrieved ' + ftpsite + fname)
            efile = ftpsite + fname
            print('WARNING: Downloading file not supported at this time')
            print('you may download manually using the following address')
            print(efile)
        else:
            print('file exists ' + fname)
            efile = fname
        self.info += 'File retrieved :' + efile + '\n'
        return efile

    def create_location_dictionary(self):
        if 'latitude' in list(self.df.columns.values):
            dftemp = self.df.copy()
            pairs = zip(dftemp['orispl_code'], zip( dftemp['latitude'], dftemp['longitude']))
            pairs = list(set(pairs))
            lhash = dict(pairs)  #key is facility id and value is name.
            print(lhash)
            return  lhash
        else:
            return false 

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
        self.namehash[newname] = ccc  #dictionary with key as the newname and value as the original name
        rcolumn.append(newname)
        if verbose: print(ccc + ' to ' + newname)
        return rcolumn

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
            if 'facility' in ccc.lower() and 'name' in ccc.lower():
                rcolumn = self.rename(ccc, 'facility_name', rcolumn, verbose)
            elif 'orispl' in ccc.lower():
                rcolumn = self.rename(ccc, 'orispl_code', rcolumn, verbose)
            elif 'facility' in ccc.lower() and 'id' in ccc.lower():
                rcolumn = self.rename(ccc, 'fac_id', rcolumn, verbose)
            elif 'so2' in ccc.lower() and ('lbs' in ccc.lower() or 'pounds' in ccc.lower()) and ('rate' not in ccc.lower()):
                rcolumn = self.rename(ccc, 'so2_lbs', rcolumn, verbose)
            elif 'nox' in ccc.lower() and ('lbs' in ccc.lower() or 'pounds' in ccc.lower()) and ('rate' not in ccc.lower()):
                rcolumn = self.rename(ccc, 'nox_lbs', rcolumn, verbose)
            elif 'co2' in ccc.lower() and ('short' in ccc.lower() and 'tons' in ccc.lower()):
                rcolumn = self.rename(ccc, 'co2_short_tons', rcolumn, verbose)
            elif 'date' in ccc.lower():
                rcolumn = self.rename(ccc, 'date', rcolumn, verbose)
            elif 'hour' in ccc.lower():
                rcolumn = self.rename(ccc, 'hour', rcolumn, verbose)
            elif 'lat' in ccc.lower():
                rcolumn = self.rename(ccc, 'latitude', rcolumn, verbose)
            elif 'lon' in ccc.lower():
                rcolumn = self.rename(ccc, 'longitude', rcolumn, verbose)
            elif 'state' in ccc.lower():
                rcolumn = self.rename(ccc, 'state_name', rcolumn, verbose)
            else:
                rcolumn.append(ccc.strip().lower())
        return rcolumn

    def get_date_fmt(self, date, verbose=False):
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
            string which can be used with datetime object to give format of date string.
        """
        if verbose: print('Determining date format')
        if verbose: print(date)
        temp = date.split('-')
        if len(temp[0]) == 4:
           fmt = "%Y-%m-%d %H"
        else:
           fmt = "%m-%d-%Y %H"
        return fmt

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

        ##pandas read_csv can read either from a file or url.
        dftemp = pd.read_csv(efile, sep=',', index_col=False, header=0)
        columns = list(dftemp.columns.values)
        columns = self.columns_rename(columns, verbose)
        dftemp.columns = columns
        if verbose: print(columns)

        dfmt = self.get_date_fmt(dftemp['date'][0], verbose=verbose)

        ##create column with datetime information from column with month-day-year and column with hour.
        dftime = dftemp.apply(lambda x:pd.datetime.strptime("{0} {1}".format(x['date'], x['hour'])  , dfmt), axis=1) 
        dftemp = pd.concat([dftime, dftemp], axis=1) 
        dftemp.rename(columns={0:'time local'}, inplace=True)
        dftemp.drop(['date', 'hour'], axis=1, inplace=True)

        #-------------Load supplmental data-----------------------
        #contains info on facility id, lat, lon, time offset from UTC.
        #allows transformation from local time to UTC.
        #If not all power stations are found in the cemsinfo.csv file, then Nan will be written in lat, lon and 'time' column.
        basedir = os.path.abspath(os.path.dirname(__file__))[:-10]
        iname = os.path.join(basedir, 'data', 'cemsinfo.csv')
        if os.path.isfile(iname):
           sinfo = pd.read_csv(iname, sep=',',  header=0)
           dfnew = pd.merge(dftemp, sinfo, how='left'  , left_on=['orispl_code'], right_on=['orispl_code'])
           #remove stations which do not have a time offset.
           dfnew.dropna(axis=0, subset=['time_offset'], inplace=True)
           #Create new 'time' column with UTC time
           def utc(x): return  pd.Timestamp(x['time local']) + datetime.timedelta(hours=x['time_offset'])
           dfnew['time'] = dfnew.apply(utc, axis=1)
           #Remove time_offset column and merge back.
           dfnew.drop(['time_offset'], axis=1, inplace=True)
           mlist = dftemp.columns.values.tolist()
           dlist = dftemp.columns.values.tolist()
           dftemp = pd.merge(dftemp, dfnew, how='left', left_on=mlist, right_on=mlist)
        #---------------------------------------------------------

        ##This is a kluge because these dates are when the time change occurs and routine can't handle
        ## non-existent or ambiguous local times.
        #dftemp= dftemp.ix[dftemp['time local'] != pd.Timestamp(2016,3,13,2)] 
        #dftemp= dftemp.ix[dftemp['time local'] != pd.Timestamp(2016,11,6,1)] 

        #tz =  TimezoneFinder()
        #def timezone(x): return tz.timezone_at(lat=x['latitude'], lng=x['longitude'])
        #dftemp['timezone'] = dftemp.apply(timezone, axis=1)
     
        ##this creates a column with time stamps which are offset-aware.        
        #def utc(x): return  pd.Timestamp(x['time local'], tz= x['timezone']).tz_convert('UTC')
        #dftemp['time'] = dftemp.apply(utc, axis=1)
        #offset = datetime.timedelta(hours=6) 
        #def utc(x): return  pd.Timestamp(x['time local']) + offset
        #dftemp['time'] = dftemp.apply(utc, axis=1)
 
        if ['year'] in columns: dftemp.drop(['year'], axis=1, inplace=True)
        if self.df is None:
            self.df = dftemp
            if verbose:
                print('Initializing pandas dataframe. Loading ' + efile)
        else:
            self.df = self.df.append(dftemp)        
            if verbose: print('Appending to pandas dataframe. Loading ' + efile)
        if verbose: print(dftemp[0:10])
        return dftemp
