#!/opt/Tools/anaconda3/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
#from math import *
from optparse import OptionParser
import numpy as np
import matplotlib.pyplot as plt
#import datetime
#import pandas as pd
#from pylab import matrix
#import io


"""class for Hysplit MESSAGE file.
optionparser input which will plot time steps"""


class HysplitMessageFile(object):
    """Class to read the Hysplit Message File.
    Currently looks at how time step evolves over the run"""


    def __init__(self, fname):
        self.fname=fname
        self.read()


    def read(self):
        self.flags = []
        self.warning = []
        phour = 0
        nnn = 0

        self.mhash={}
        self.mhash['hour'] = []
        self.mhash['pnum'] = []
        self.mhash['mass'] = []
        
        self.emrise = [] # list of tuple of hour number, mixd, rise

        thash={}   #key is hour number, value is number of times the hour is printed out
        phash={}   #key is hour number, value is large number of particles in that hour

        iii=0
        # Message files usually have one line with some binary code.
        # put the errors=ignore in.
        with open(self.fname,'r', errors="ignore") as fid:
             print('opening file' , self.fname)
             for temp in fid:
                 if 'str' in temp:
                     break
                 if 'WARNING' in temp:
                    self.warning.append(temp)
                 elif ('NOTICE' in temp) and ('main' in temp):
                    if 'number meteo grids and times' in temp:
                        meteogrids = temp
                    elif 'flags' in temp:
                        self.flags.append(temp)
                    elif 'time step' in temp:
                        init_time_step = temp
                    else:
                        temp2 = temp.split()
                        #print temp2
                        hour = int(temp2[2])
                        self.mhash['hour'].append(hour)
                        self.mhash['pnum'].append(int(temp2[4]))
                        self.mhash['mass'].append(float(temp2[5]))
                        if hour != phour:
                           nnn = 1
                        else:
                           nnn +=1
                        phash[hour] = int(temp2[4]) 
                        #print hour , int(temp2[4])
                        thash[hour] = nnn
                        phour = hour
                 elif ('NOTICE' in temp) and ('emrise' in temp):
                    temp2 = temp.split()
                    self.emrise.append((hour, float(temp2[4]), float(temp2[5])))
                    print(temp2)
                  
        self.timestep = []
        self.pnumber = []
        ##calculate time step for each simulation hour.
        for key in thash:
            tstep = 60.0 / thash[key]
            self.timestep.append((key, tstep))
            self.pnumber.append(np.log10(phash[key]))
            #print key, phash[key] , np.log10(phash[key])


    def print_warnings(self):
        """prints all lines with WARNING in them"""
        for warn in self.warning:
            print(warn)

    def plot_time_step(self):
        """plots the time step as a function of simulation hour"""
        timestep = self.timestep
        fig = plt.figure(1)
        ax = plt.subplot(1,1,1) 
        ax.plot(zip(*timestep)[0],zip(*timestep)[1],  '-b.')
        ax.set_xlabel('Simulation Hour')
        ax.set_ylabel('Average time step in hour (minutes)')
        plt.show()                    

    def plot_emrise(self):
        sep = list(zip(*self.emrise))
        print(sep)
        fig = plt.figure(1)
        ax=fig.add_subplot(1,1,1)
        ax.set_xlabel('Simulation Hour')
        ax.set_ylabel('Height')
        ax.plot(sep[0], sep[2], '-b.')
        ax.plot(sep[0], sep[1], '-g.')
        plt.show()  
        


parser = OptionParser()

parser.add_option("-f", type='string', dest='fname', default='MESSAGE',
                 help="Name of HYSPLIT output MESSAGE file. (MESSAGE)")

(options, args) = parser.parse_args()

mfile = HysplitMessageFile(options.fname)     
#mfile.print_warnings()   
#mfile.plot_time_step()
mfile.plot_emrise()
