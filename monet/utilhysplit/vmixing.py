#!/n-home/alicec/anaconda/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import datetime
import string
import subprocess
from os import path

import numpy as np
import pandas as pd
from monet.utilhysplit.hcontrol import HycsControl


"""
PRGMMR: Alice Crawford  ORG: ARL
PYTHON 2.7
This code written at the NOAA  Air Resources Laboratory
UID: r102
CTYPE: source code
ABSTRACT: manages the xtrct_stn program and outputs.

CLASSES


"""


class VmixingRun:
    def __init__(self,
                 fname,
                 cname='CONTROL',
                 cdir='./',
                 pid=None,
                 kbls=1,
                 kblt=2,
                 cameo=2,
                 tkemin=None,
                 verbose=True):
        self.control = HycsControl(fname=cname, rtype='vmixing')
        self.pid = self.control.replace('CONTROL.', '')
        self.kbls = kbls  # 1 fluxes  #2 wind/temp profile
        self.kblt = kblt  # 1 BJ #2 KC #3 TKE.
        self.cameo = cameo  # 0 no #1 yes #2 yes + wdir
        self.tkemin = tkemin
        self.woption = woption  # output extra file
        self.cdir = cdir

    def readcontrol(self):
        self.control.read()

    def writecontrol(self, cname=None, cdir=None):
        if not cdir:
            cdir = self.cdir
        if cname:
            self.control.rename(cname, working_directory=cdir)
        self.control.write()

    def assign_pid(self, pid):
        self.pid = pid
        self.control.rename('CONTROL.' + str(pid))
        return -1

    def make_runstr(self, hdir):
        rstr = hdir
        if rstr[-1] != '/':
            rstr.append('/')
        rstr += 'vmixing '
        if self.pid:
            rstr += '-p' + str(self.pid)
            rstr += '-s' + str(self.kbls)
            rstr += '-t' + str(self.kblt)
            rstr += '-a' + str(self.cameo)
            if tkemin:
                rstr += '-m' + str(self.tkemin)
            rstr += '-w' + str(self.woption)
        return rstr


class VmixingData:
    """
        add_data
        make_dummies (NOT FUNCTIONAL)
        readfile
    """

    def __init__(self, century=2000, verbose=True):
        """fname : name of file output by xtrct_stn
           valra : list of values that are in fname
           century : fname lists year by last two digits only. century is needed to process date.
           """
        self.units = None
        self.df = pd.DataFrame()

    def add_data(self, fname, vdir='./', century=2000, verbose=False,
                 sid=None):
        df = self.readfile(fname, vdir, century, verbose)
        if sid:
            df['sid'] = sid
        if self.df.empty:
            self.df = df
        else:
            self.df = pd.concat([self.df, df], axis=0)
            # print(self.df)
            #import sys
            # sys.exit()
        return self.df

    def make_dummies(self, data_ra=[-999]):
        """instead of running,  write a dummy file like the one vmixing would write.
           Used for testing.
        """
        #sdate = datetime.datetime()
        #dt = datetime.timedelta(hour=1)
        # iii=1
        # with open(self.fname) as fid:
        #     fid.write(str(iii) + sdate.strftime(" %y %m %d %h"))
        #     iii+=1
        return -1

    def get_location(self, head1):
        # vmixing doesn't always print a space between lat and lon
        head1 = head1.replace('-', ' -')
        temp1 = head1.split()
        lat = float(temp1[0])
        lon = float(temp1[1])
        met = temp1[2]
        return lat, lon, met

    def parse_header(self, head2, head3):
        temp2 = head2.split()
        temp3 = head3.split()
        cols = ['date']
        units = ['utc']
        cols.extend(temp2[6:])
        if 'Total' in cols and 'Cld' in cols:
            cols.remove('Total')
        units.extend(temp3)
        return cols, units

    def readfile(self, fname, vdir='./', century=2000, verbose=False):
        """Reads file and returns True if the file exists.
           returns False if file is not found"""
        df = pd.DataFrame()
        if path.isfile(vdir + fname):
            if verbose:
                print('Adding', vdir + fname)
            data = []
            with open(vdir + fname, "r") as fid:
                head1 = fid.readline()
                head2 = fid.readline()
                head3 = fid.readline()
                try:
                    lat, lon, met = self.get_location(head1)
                except:
                    print('problem with vmixing file ', fname, vdir)
                    print('header ', head1)
                    return df
                    # sys.exit()
                cols, units = self.parse_header(head2, head3)
                for line in fid.readlines():
                    # get the date for the line
                    temp = line.split()
                    try:
                        vals = [self.line2date(line, century)]
                    except:
                        return False
                    temp2 = []
                    for val in temp[6:]:
                        try:
                            temp2.append(float(val))
                        except:
                            temp2.append(val)
                    vals.extend(temp2)
                    data.append(vals)
            df = pd.DataFrame.from_records(data)
            df.columns = cols
            df['latitude'] = lat
            df['longitude'] = lon
            df['met'] = met
            self.units = zip(cols, units)
        else:
            if verbose:
                print('Cannot Find ', vdir + fname)
        return df

    def line2date(self, line, century):
        """get date from a line in the xtrct_stn output and return datetime object"""
        temp = line.strip().split()
        year = int(temp[1]) + century
        month = int(temp[2])
        day = int(temp[3])
        hour = int(temp[4])
        minute = int(temp[5])
        vdate = datetime.datetime(year, month, day, hour, minute)
        return vdate
