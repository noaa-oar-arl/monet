#! /naqfc/noscrub/Barry.Baker/anaconda2/bin/python
###for aitken replace the first line with /data/aqf/barryb/anaconda2/bin/python for the line above
#   To use this function you exectute as follows
#
# ./domain_compare_aqs.py PATH/TO/ACONC PATH/TO/GRIDCRO LABEL EXTENSION


import sys
from glob import glob

import matplotlib.pyplot as plt
import verify

print "Name of Script: ", sys.argv[0]
print "ACONC FILE: ", sys.argv[1]
print "GRIDCRO2D FILE: ", sys.argv[2]
print "LABEL: ", sys.argv[3]
print "OUTPUT FILENAME: ", sys.argv[4]
files = glob(sys.argv[1])
grid = sys.argv[2]
va = verify.verify_aqs(concpath=files, gridcro=grid, datapath='.', combine=True, neighbors=9)
params = va.df.Species.unique()
for i in params:
    va.compare_param(param=i, timeseries=True, label=sys.argv[3])
    plt.savefig(i.replace('.', '') + '_' + sys.argv[4], dpi=100)
    plt.close()
