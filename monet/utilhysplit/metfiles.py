# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
#import numpy as np
import datetime
#import time
#import os
from os import path
#from subprocess import call
#import pandas as pd

"""
NAME: svhy.py
PRGMMR: Alice Crawford  ORG: ARL
This code written at the NOAA  Air Resources Laboratory
ABSTRACT: choosing met files for HYSPLIT control file


"""


def getmetfiles(strfmt, sdate, runtime):
    """
    Inputs
    strfmt : str
            string with path to meteorlogical input files.
            datetime formatting should be used for dates.
            Example
            /TopLevelDirectory/gdas1/gdas1.%b%y.week
            /TopLevelDirectory/wrf27km/%Y/wrfout_d01_%Y%m%d.ARL
            use the word week to indicate when files are by week.
            week will be replaced by w1, w2... as appropriate.
    sdate : datetime object
            begininng date of HYSPLIT run
    runtime : integer
            duration of run
    Returns:
            list of tuples (directory, filename) of meteorological files.
    """
    mfe = MetFiles(strfmt)
    return mfe.get_files(sdate, runtime)


class MetFiles:
    def __init__(self, strfmt, verbose=False):
        self.verbose = verbose
        self.strfmt = strfmt
        self.mdt = None  # defined later.
        # if not hours:
        #    self.mdt = self.find_mdt()
        # else:
        #    self.mdt = datetime.timedelta(hours=hours)

    def get_files(self, sdate, runtime):
        """
        sdate : datetime object
        runtime : integer. hours of runtime.
        """
        nlist = self.make_file_list(sdate, runtime)
        return self.process(nlist)

    @staticmethod
    def handle_hour_a(sdate):
        """
        Inputs
        Return
        """
        # returns date with 0 hour.
        year = sdate.year
        month = sdate.month
        day = sdate.day
        #hour = sdate.hour
        testdate = datetime.datetime(year, month, day, 0)
        return testdate

    def handle_hour_b(self, sdate):
        """
        Inputs
        Return
        """
        testdate = self.handle_hour_a(sdate)
        done = False
        imax = 100
        iii = 0
        while not done:
            newdate = testdate + self.mdt
            if newdate < sdate:
                testdate = newdate
            else:
                done = True
            iii += 1
            if iii > imax:
                done = True
        return testdate

    def find_mdt(self, testdate):
        """
        # finds time spacing between met files by
        # seeing which spacing produces a new file name.
        # testdate = datetime.datetime(2010,1,1)
        Inputs
        Return
        """
        if "%H" in self.strfmt:
            mdtlist = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
            testdate = self.handle_hour_a(testdate)
        else:
            mdtlist = [1, 24, 24 * 7, 24 * 31, 24 * 356]

        file1 = testdate.strftime(self.strfmt)
        done = False
        iii = 0
        while not done:
            ddt = datetime.timedelta(hours=mdtlist[iii])
            dd2 = testdate + ddt
            file2 = dd2.strftime(self.strfmt)
            if file2 != file1 and path.isfile(file2):
                done = True
            iii += 1
            if iii >= len(mdtlist):
                done = True
        return ddt

    def parse_week(self, edate):
        """
        Inputs
        Return
        """
        # used if week is in the strfmt (mostly for gdas1)
        temp = edate.strftime(self.strfmt)
        day = int(edate.strftime("%d"))
        if day < 7:
            temp = temp.replace("week", "w1")
        elif day < 14:
            temp = temp.replace("week", "w2")
        elif day < 21:
            temp = temp.replace("week", "w3")
        elif day < 28:
            temp = temp.replace("week", "w4")
        else:
            temp = temp.replace("week", "w5")
        return temp

    def make_file_list(self, sdate, runtime):
        """
        Inputs
        Return
        """
        nlist = []
        sdate = sdate.replace(tzinfo=None)
        self.mdt = self.find_mdt(sdate)
        # handle backwards runs. by switching sdate and edate
        if runtime < 0:
            runtime = abs(runtime)
            end_date = sdate
            sdate = end_date - datetime.timedelta(hours=runtime)
        else:
            end_date = sdate + datetime.timedelta(hours=runtime)
        done = False
        # self.verbose=True
        if "%H" in self.strfmt:
            sdate = self.handle_hour_b(sdate)
        edate = sdate
        if self.verbose:
            print("GETMET", sdate, edate, end_date, runtime, self.mdt)
        zzz = 0
        while not done:
            if "week" in self.strfmt:
                temp = self.parse_week(edate)
            else:
                temp = edate.strftime(self.strfmt)
            edate = edate + self.mdt
            if not path.isfile(temp):
                temp = temp.lower()
            if not path.isfile(temp):
                print("WARNING", temp, " meteorological file does not exist")
            else:
                if temp not in nlist:
                    nlist.append(temp)
            # print(edate, '--' , end_date, '--' , self.mdt)
            if edate > end_date:
                done = True
            if zzz > 50:
                done = True
            zzz += 1
        return nlist

    @staticmethod
    def process(nlist):
        """
        Inputs
        Return
        # convert list of filenames with full path to
        # list of directories and list of filenames
        # and then zips the lists to return list of tuples.
        """
        mfiles = []
        mdirlist = []
        for temp in nlist:
            sii = [x for x, char in enumerate(temp) if char == "/"]
            sii = sii[-1]
            fname = temp[sii + 1:]
            mdir = temp[0: sii + 1]
            mfiles.append(fname)
            mdirlist.append(mdir)
        return list(zip(mdirlist, mfiles))
