from __future__ import print_function
import os
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import sys

def ppb2ugm3(ppb, mw, pressure=None, temperature=None):
    """ppb is value in volume fraction parts per billion.
       mw is molecular weight in g/ mol
       if pressure and temperature are not given then standard pressure and temperature are used"""
    if not temperature:
       temperature = 293.0 ##Kelvin.
    if not pressure:
        standard_press = 101.325 #(kPa)
    R = 8.3144 #J/ (mol K)  or (L kPa / (mol K)) 
    return ppb * mw / R / temperature * pressure


def lbs2kg(lbs):
    kg = 0.453592 * lbs
    return kg

class StationData(object):
    """ Class for manipulating data in MONET observation dataframe.
        write_datem:  method for writing text file in datem format to be used with c2datem (HYSPLIT distribution)
        find_near: finds sites within a certain distance of  an input lat lon location.
        convert_units: currently converts SO2 ppb to ug/m3.
        dropna: removes columns which have all nans.
    """
    def __init__(self, df=None):
      """Short summary.
      Parameters
      ----------
      df : pandas dataframe  
      """

      #self.efile = None   
      #self.url = None
      self.lb2kg = 0.453592  #number of kilograms per pound.
      self.info = None
      self.df = df

    def find_near(self, latlon, distance=100, sid='site_num', drange=None):
        """find all values in the df dataframe column sid which are within distance (km) of lat lon point.
           output dictionary with key as value in column sid and value tuple (latitude, longitude)

         Parameters
         ----------
         latlon : tuple or list 
                  (longitude, latitude) 
         distance : float 
                   kilometers
         sid: string
              name of column
         drange: tuple or list with two datetimes
              consider rows with dates between these two dates.
         
         Returns
         --------
         lhash: dictionary
             key is the value in column sid and value is (latitude, longitude) position.
        """
        degree2km = 111
        if drange:
           df = self.df.copy()
           self.timefilter(drange)
        lhash = self.get_lhash(sid)
        for key in lhash.keys:
            xd = (lhash[key][1] - latlon[1]) * degree2km *np.cos(latlon[1] * np.pi/ 180.0)
            yd = (lhash[key][0] - latlon[0]) * degree2km
            dd = (xd**2 + yd**2) **0.5
            if dd > distance:
               lhash.pop(key, None) 
        return lhash

    def convert_units(self, obscolumn='obs', unit='UG/M3'):
        """
        converts ppb to ug/m3 for SO2
        SO2 see 40 CFR Part 50.5, Appendix A-1 to part 50, appendix A=2 to Part 50.
        to convert from ppb to ug/m3 multiply by 2.6178.

        """ 
        factor = 2.6178
        self.df = self.df[self.df['units']=='ppb']
        self.df['units'] = 'UG/M3'
        self.df[obscolumn] = self.df[obscolumn] * 2.6178
        #self.df[obscolumn] = self.df.apply(lambda x: ppb2ugm3(mw, x[obscolumn]))
        #self.df['units'] = 'ugm3'

    def write_datem(self, obscolumn='obs', dname = 'datemfile.txt', sitename='1', info=None, drange=None):
        """returns string in datem format (See NOAA ARL).
         datem format has the following columns:
         Year, Month, Day, Hour, Duration, lat, lon, Concentration (units), site id, height
  
         Parameters
         ----------
         obscolumn : string
                name of column with values to write in the Concentration column.
         dname : string 
                 name of the output file.
         sitename : string. 
                 If it is the name of a column in the dataframe then 
                 that column will be used to generate the site name column in the datem file.
                 If is not the name of a column, then the string will be used as the site name.
         info : string 
               will be written to the second line of the header.
         drange : list of two time stamp objects.
         Returns
         --------
         runstring: string
           string in datem format.
        """
        if drange:
           df = self.df.copy()
           self.timefilter(drange)
          
        units=self.df['units'].tolist()
        units = list(set(units))
        sdate = datetime.datetime(2010,1,1,0)
        if len(units) > 1: print('WARNING, more than one type of unit ', units)
        ustr=''
        for uuu in units:
            ustr += uuu + ' '
        runstring =  "Beginning date " + sdate.strftime("%Y %m %d %H:%M") + " UTC ---"
        runstring += 'Information '  
        if info:
           runstring += info + "\n"
        else:
           runstring +=  "\n"
        runstring += "Year, Month, Day, Hour:Minute (UTC), Dur(hhmm) ,  LAT, LON, Concentration (" + ustr +  "), sid, height\n"
        lat = self.df['latitude']
        lon = self.df['longitude']
        cval = self.df[obscolumn]
        #print t2
        t1 = self.df['time']
        duration = ' 0100 '
        height = '20'
        if sitename in self.df.columns.values:
           sval = self.df[sitename]
        else:
           sval = [sitename] * len(cval)
        for val in zip(t1, lat, lon, cval, sval):
            runstring += val[0].strftime('%Y  %m  %d  %H%M') +  duration
            print(runstring)
            try:
                runstring += str(val[1]) + ' ' + str(val[2]) + ' ' 
            except:
                print('WARNING1', val[1])
                print(val[2])
                print(type(val[1]))
                print(type(val[2]))
                sys.exit()
            if isinstance(val[4],str):
                runstring +=  "{:.3f}".format(val[3]) + val[4] + height + "\n"
            else:
                runstring +=  "{:.3f}".format(val[3]) + ' ' + "{0:d}".format(val[4]) + ' ' + height + "\n"

        with open(dname, 'w') as fid:
             fid.write(runstring)
        if drange: self.df = df  #restore self.df to full dataframe.
        return runstring


    def dropna(self):
        """remove columns which have all Nans"""
        self.df = self.df.dropna(axis=1, inplace=True)


    def get_lhash(self, idn ):
        """returns a dictionary with the key as the input column value and the value a tuple of (lat, lon)
           Useful for getting lat lon locations of different sites in a dataframe.
        """
        if 'latitude' in list(self.df.columns.values):
            dftemp = self.df.copy()
            pairs = zip(dftemp[idn], zip( dftemp['latitude'], dftemp['longitude']))
            pairs = list(set(pairs))
            lhash = dict(pairs)  #key is facility id and value is name.
            print(lhash)
        self.lhash= lhash
        return lhash

    def __str__(self):
        return self.info

    def summarize(self, verbose=False):
        """prints list of columns. if verbose prints list of unique values in each column"""
        columns = list(self.df.columns.values)
        if verbose: 
            for ccc in columns:
                print(ccc)
                print(self.df[ccc].unique())
        print('-------------------------------')
        for ccc in columns:
            print(ccc)
             
    def latlonfilter(self, llcrnr, urcrnr):
        """
         removes rows from self.df with latitude longitude outside of the box
         described by llcrnr (lower left corner) and urcrnr (upper right corner)
         Parameters
         ----------
           llcrnr : tuple 
                    lower left corner. (latitude, longitude)
           urcrnr : tuple 
                    upper right corner (latittude, longitude)
           removes rows with latitude longitude outside of the box
           described by llcrnr (lower left corner) and urcrnr (upper right corner)
        """
        lat1 = llcrnr[0]
        lat2 = urcrnr[0] 
        lon1 = llcrnr[1]
        lon2 = urcrnr[1] 
        self.df = self.df[self.df['latitude'] < lat2]
        self.df = self.df[self.df['latitude'] > lat1]
        self.df = self.df[self.df['longitude'] > lon1]
        self.df = self.df[self.df['longitude'] < lon2]

    def timefilter(self, daterange):
        """removes rows with dates outside of the daterange from self.df
         Parameters
         ----------
         daterange:  tuple 
                   (datetime, datetime)
        """
        self.df = self.df[self.df['time'] > daterange[0]]
        self.df = self.df[self.df['time'] < daterange[1]]


    def unique(self, cname='Site_Num'): 
        sites= list(self.df[cname].unique())
        return sites
       
    def plotloc(self, latlon, var='obs'):
        dftemp = self.df[self.df['latlon'] == latlon]
        dftemp.set_index('datetime', inplace=True)
        series = dftemp[var]
        plt.plot(series, '-b.')
        plt.title('Site ' + str(latlon[2]) + ' location ' + str(latlon[0]) + ' : ' + str(latlon[1]) )
        unitlist = dftemp['Units'].unique()
        #for unit in unitlist:
        #    unitstr += unit 
        plt.show()
    
       
