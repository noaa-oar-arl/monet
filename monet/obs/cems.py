from __future__ import print_function
import os
import pandas as pd
import numpy as np
import datetime

def getdegrees(degrees, minutes, seconds):
        return degrees+ minutes/60.0 + seconds/3600.00


class CEMSEmissions(object):
    """Class for data from continuous emission monitoring systems (CEMS).
       Data from power plants can be downloaded from ftp://newftp.epa.gov/DMDNLoad/emissions/"""

    def __init__(self):
      self.efile = None   
      self.url = "ftp://newftp.epa.gov/DmDnLoad/emissions/"
      self.lb2kg = 0.453592  #number of kilograms per pound.
      self.info = "Data from continuous emission monitoring systems (CEMS) \n"
      self.info += self.url + '\n'

    def __str__(self):
        return self.info

    def retrieve(self, rdate, state, download=True):
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

    def get_location(self, name):
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
        lhash[316]   = (29.238,-76.5119) #perryman

    def load(self, efile):
        """loads information found in efile into a pandas dataframe.
           Currently cannot load from ftp site directly.
        """
        dftemp = pd.read_csv(efile)
        columns = list(dftemp.columns.values)
        ckeep=[]
        for ccc in columns:
            #print('Column name: ' + ccc)
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
        dftemp.dropna(axis=0,  inplace=True, subset=cnan)
        print(dftemp['FACILITY_NAME'].unique())
        print(dftemp['FAC_ID'].unique())
        pairs = zip(dftemp['FAC_ID'], dftemp['FACILITY_NAME'])
        pairs = list(set(pairs))
        #print(pairs)
        self.namehash = dict(pairs)  #key is facility id and value is name.
        self.df = dftemp
        return dftemp
        
