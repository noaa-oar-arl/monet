#! /naqfc/noscrub/Barry.Baker/anaconda2/bin/python
###for aitken replace the first line with /data/aqf/barryb/anaconda2/bin/python for the line above
#   To use this function you exectute as follows
#
# ./region_compare.py PATH/TO/ACONC PATH/TO/GRIDCRO LABEL STATE EXTENSION

import sys
from glob import glob

import matplotlib.pyplot as plt
import monet as verify

print "Name of Script: ", sys.argv[0]
print "ACONC FILE: ", sys.argv[1]
print "GRIDCRO2D FILE: ", sys.argv[2]
print "LABEL: ", sys.argv[3]
print "OUTPUT FILENAME: ", sys.argv[4]

files = glob(sys.argv[1])
grid = sys.argv[2]

va = verify.vaqs(concpath=files, gridcro=grid, datapath='.', user=sys.argv[4], passw=sys.argv[5], combine=True,
                       neighbors=9)

for i in va.df.Region.dropna().unique():
    params = va.df.groupby('Region').get_group(i).Species.unique()
    for j in params:
        va.compare_param(param=j, timeseries=True, label=sys.argv[3], region=i, footer=False)
        plt.savefig(j + '_' + i + sys.argv[4], dpi=100)
        plt.close()
