from __future__ import print_function
import os
import pandas as pd
import numpy as np
import datetime
import pytz
from timezonefinder import TimezoneFinder
import sys


#def get_utcoffset((lat,lon), date):
#    timezone = tzwhere.tzwhere()
#    timezone_str = tzwhere.tzNameAt(lat, lon)
#    timezone = pytz.timezone(timezone_str)
#    return timezone.utcoffset(date)

def getdegrees(degrees, minutes, seconds):
        return degrees+ minutes/60.0 + seconds/3600.00


def addmonth(dt):
    month = dt.month +1
    year = dt.year
    day = dt.day
    hour = dt.hour
    if month > 12:
       year = dt.year +1
       month = month -12
       if day==31 and month in [4,6,9,11]:
          day=30
       if month==2 and day in [29,30,31]:
          if year%4 ==0:
             day=29
          else:
             day=28
    return datetime.datetime(year, month, day, hour)

def lbs2kg(lbs):
    kg = 0.453592 * lbs
    return kg

class CEMSEmissions(object):
    """Class for data from continuous emission monitoring systems (CEMS).
       Data from power plants can be downloaded from ftp://newftp.epa.gov/DMDNLoad/emissions/"""

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
        """gets the ftp url from the retrieve method and then 
           loads the data from the ftp site using the load method.

           rdate should either be a single datetime object or a list of two datetime objects.
           The first datetime object indicates the month and year of the first file to retrieve.
           The second datetime object indicates the month and year of the last file to retrieve.

           If download=True then retrieve will download the files and load
           will read the downloaded files.
           If download=False then retrieve will return the url and load will read directly from ftp site.
           TO DO add loop for adding multiple months.
        """
        if not rdate:
           return -1 
        if isinstance(states, str):
           states = [states] 
        if isinstance(rdate, list):
           r1 = rdate[0]
           r2 = rdate[1]
           rdatelist = [r1]
           done = False
           iii=0

           while not done:
              r3 = addmonth(rdatelist[-1])
              if r3 <= r2:
                 rdatelist.append(r3)
              else:
                 done=True     
              if iii > 100: done=True
              iii+=1
        else:
            rdatelist = [rdates]
        for rd in rdatelist: 
            for st in states:
                url = self.retrieve(rd, st, download=download, verbose=verbose)
                self.load(url, verbose=verbose)


    def filter_by_time(self, df, daterange):
          """ returns dataframe with values between (and including) [date1, date2]"""
          if not daterange: return df.copy()
          temp = df.copy()
          print('filter by time')
          print(daterange[0])
          print(daterange[1])
          temp = temp[temp['time']>=daterange[0]] 
          temp = temp[temp['time']<=daterange[1]] 
          #sys.exit()
          return temp


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
           varname may be string or list of strings.
           routine looks for column which contains all strings in varname.
           Currently not case sensitive.
     
           loc must be list of FAC_ID's.

           TO DO - each FAC_ID may have several UNIT_ID, each of which
           corresponds to a different unit at the facility. Need to handle this.
           Either return separately or add together?

           loc must be list of FAC_ID's.

           Each facility may have more than one unit. If unitid=-99 then this
           method returns sum from all units.

           if a particular unitid is specified then will return values for that unit.

           TO DO if unitid -999 then will return a dictionary where key is the unit and value
           is a panda time series for that unit.
        """
        if isinstance(varname, str):
           varname = (varname)
        columns =  list(self.df.columns.values)
        #temp = self.df['OP_DATE', 'OP_HOUR', 'OP_TIME']
        #print(temp[0:10])
       
        if loc:
            temp = self.df[self.df['fac_id'].isin(loc)]
        else:
            temp = self.df.copy()
        temp = self.filter_by_time(temp, daterange)
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
            pivot = pd.pivot_table(temp, values=cmatch, index=['time'], columns = ['fac_id'])
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
        """rdate - datetime object. Uses year and month. Day and hour are not used.
           state - state abbreviation to retrieve data for
           Files are by year month and state.
        """
        import wget
        efile = 'empty'
        ftpsite = self.url
        ftpsite += 'hourly/'
        ftpsite += 'monthly/'
        ftpsite +=  rdate.strftime("%Y") + '/'
        fname = rdate.strftime("%Y") + state + rdate.strftime("%m") + '.zip'
        if not download:
           return ftpsite + fname
        if not os.path.isfile(fname):
            print('retrieving ' + ftpsite + fname) 
            efile = wget.download(ftpsite + fname)
            print('retrieved ' + ftpsite + fname) 
        else:
            print('file exists ' + fname)
            efile = fname
        self.info += 'File retrieved :' +  efile + '\n'
        return efile

    def create_location_dictionary(self):
        if 'latitude' in list(self.df.columns.values):
            dftemp = self.df.copy()
            pairs = zip(dftemp['fac_id'], zip( dftemp['latitude'], dftemp['longitude']))
            pairs = list(set(pairs))
            lhash = dict(pairs)  #key is facility id and value is name.
            print(lhash)
            return  lhash
        else:
            return false 

    def get_location(self, facid):
        """Need to create a comprehensive dictionary for all locations in the US."""
        lhash = {}
        lhash[314] = (39.1785, -76.5269) #Herbert A Wagner
        lhash[110]   = (getdegrees(39,10,53), getdegrees(-76,32,16)) #brandon shores
        lhash[312]   = (getdegrees(39,19,25), getdegrees(-76,21,59)) #CP Crane
        lhash[322]   = (getdegrees(38,32,37), getdegrees(-76,41,19)) #chalk point
        lhash[323]   = (getdegrees(39,12,36), getdegrees(-77,27,54)) #dickerson
        lhash[324]   = (getdegrees(38,21,33), getdegrees(-76,58,36)) #morgantown
        lhash[1116]   = (getdegrees(39,35,46), getdegrees(-78,44,46)) #warrier run
        lhash[1229]   = (38.670, -76.865) #brandywine
        lhash[316]   = (39.238,-76.5119) #perryman
        ##TO DO. This should be moved elsewhere.
        if 'latitude' in list(self.df.columns.values):
            dftemp = self.df.copy()
            pairs = zip(dftemp['fac_id'], zip( dftemp['latitude'], dftemp['longitude']))
            pairs = list(set(pairs))
            lhash = dict(pairs)  #key is facility id and value is name.
            print(lhash)
        self.lhash= lhash
        return lhash[facid]

    def rename(self, ccc, newname, rcolumn, verbose):
         self.namehash[newname] = ccc  #dictionary with key as the newname and value as the original name
         rcolumn.append(newname)
         if verbose: print(ccc + ' to ' + newname)
         return rcolumn

    def columns_rename(self, columns, verbose=False):
        rcolumn = []
        for ccc in columns:
            if 'facility' in ccc.lower() and 'name' in ccc.lower():
                rcolumn = self.rename(ccc, 'facility_name', rcolumn, verbose)
            elif 'facility' in ccc.lower() and 'id' in ccc.lower():
                rcolumn = self.rename(ccc, 'fac_id', rcolumn, verbose)
            elif 'so2' in ccc.lower() and ('lbs' in ccc.lower() or 'pounds' in ccc.lower()):
                rcolumn = self.rename(ccc, 'so2_lbs', rcolumn, verbose)
            elif 'nox' in ccc.lower() and ('lbs' in ccc.lower() or 'pounds' in ccc.lower()):
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

    def get_date_fmt(self, date):
        print(date)
        temp = date.split('-')
        if len(temp[0]) == 4:
           fmt = "%Y-%m-%d %H"
        else:
           fmt = "%m-%d-%Y %H"
        return fmt

    def latlonfilter(self, llcrnr, urcrnr):
        lat1 = llcrnr[0]
        lat2 = urcrnr[0]
        lon1 = llcrnr[1]
        lon2 = urcrnr[1]
        self.df =  self.df[self.df['latitude'] < lat2]
        self.df =  self.df[self.df['latitude'] > lat1]
        self.df =  self.df[self.df['longitude'] > lon1]
        self.df =  self.df[self.df['longitude'] < lon2]

    def load(self, efile, verbose=True):
        """loads information found in efile into a pandas dataframe.
        """
        dftemp = pd.read_csv(efile, sep=',', index_col=False, header=0)
        columns = list(dftemp.columns.values)
        columns = self.columns_rename(columns, verbose)
        dftemp.columns = columns
        ckeep=[]
        for ccc in columns:
            if verbose:
                print('-------------')
                print(ccc)
                print(dftemp[ccc][0])
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
        #dftemp = dftemp[ckeep]
        #cnan = ['SO2_MASS (lbs)']
        ##drop rows with NaN in the cnan column.
        #dftemp.dropna(axis=0,  inplace=True, subset=cnan)
        #print(dftemp['FACILITY_NAME'].unique())
        #print(dftemp['FAC_ID'].unique())
        #pairs = zip(dftemp['fac_id'], dftemp['facility_name'])
        #pairs = list(set(pairs))
        #print(pairs)
        #self.namehash = dict(pairs)  #key is facility id and value is name.
        dfmt = self.get_date_fmt(dftemp['date'][0])
        ##create column with datetime information from column with month-day-year and column with hour.
        dftime = dftemp.apply(lambda x:pd.datetime.strptime("{0} {1}".format(x['date'], x['hour'])  , dfmt), axis=1) 
        dftemp = pd.concat([dftime, dftemp], axis=1) 
        dftemp.rename(columns={0:'time local'}, inplace=True)
        dftemp.drop(['date', 'hour'], axis=1, inplace=True)

        ##TO DO This is a kluge because these dates are when the time change occurs and routine can't handle
        ## non-existent or ambiguous local times.
        dftemp= dftemp.ix[dftemp['time local'] != pd.Timestamp(2016,3,13,2)] 
        dftemp= dftemp.ix[dftemp['time local'] != pd.Timestamp(2016,11,6,1)] 

        tz =  TimezoneFinder()
        def timezone(x): return tz.timezone_at(lat=x['latitude'], lng=x['longitude'])
        dftemp['timezone'] = dftemp.apply(timezone, axis=1)
     
        ##this creates a column with time stamps which are offset-aware.        
        def utc(x): return  pd.Timestamp(x['time local'], tz= x['timezone']).tz_convert('UTC')
        dftemp['time'] = dftemp.apply(utc, axis=1)
 
 
        if ['year'] in columns: dftemp.drop(['year'], axis=1, inplace=True)
        if self.df is None:
            self.df = dftemp
            if verbose: print('Initializing pandas dataframe. Loading ' + efile)
        else:
            self.df = self.df.append(dftemp)        
            if verbose: print('Appending to pandas dataframe. Loading ' + efile)
        if verbose: print(dftemp[0:10])
        return dftemp
        
