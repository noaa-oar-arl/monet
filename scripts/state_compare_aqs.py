#! /naqfc/noscrub/Barry.Baker/anaconda2/bin/python
###for aitken replace the first line with /data/aqf/barryb/anaconda2/bin/python for the line above
#   To use this function you exectute as follows
#
# ./state_compare_aqs.py PATH/TO/ACONC PATH/TO/GRIDCRO LABEL STATE EXTENSION
#
# note the state is the abbreviated state (TX for Texas or MD for Maryland)

import sys
from glob import glob

import matplotlib.pyplot as plt
import monet as verify

print "Name of Script: ", sys.argv[0]
print "ACONC FILE: ", sys.argv[1]
print "GRIDCRO2D FILE: ", sys.argv[2]
print "LABEL: ", sys.argv[3]
print "State Name: ", sys.argv[4]
print 'OUTPUT FILE EXTENSION: ', sys.argv[5]

files = glob(sys.argv[1])
grid = sys.argv[2]

va = verify.vaqs(concpath=files, gridcro=grid, datapath='.', combine=True, neighbors=9)
params = va.df.groupby('State_Name').get_group(sys.argv[4]).Species.unique()
for j in params:
    va.compare_param(param=j, timeseries=True, label=sys.argv[3], state=sys.argv[5], footer=False)
    plt.savefig(j + '_' + sys.argv[4] + sys.argv[5], dpi=100)
    plt.close()
