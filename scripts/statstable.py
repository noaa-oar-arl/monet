#! /naqfc/noscrub/Barry.Baker/anaconda2/bin/python
###for aitken replace the first line with /data/aqf/barryb/anaconda2/bin/python for the line above
#   To use this function you exectute as follows
#
# ./5x_domain_compare.py PATH/TO/ACONC PATH/TO/GRIDCRO LABEL USERNAME PASSWORD

import verify
from glob import glob
import matplotlib.pyplot as plt
import sys
from pandas import Series,DataFrame

print "Name of Script: ", sys.argv[0]
print "ACONC FILE: ", sys.argv[1]
print "GRIDCRO2D FILE: ", sys.argv[2]
print "LABEL: ", sys.argv[3]
print "OUTPUT FILENAME: ", sys.argv[4]


files = glob(sys.argv[1])
grid = sys.argv[2]

va = verify.verify_aqs(concpath=files,gridcro=grid,datapath='.',combine=True,neighbors=9)

regions = arange(7).astype('|S15')
regions[:] = 'Domain'
stats = ['Obs Mean','Model Mean','Mean Bias','Fractional Bias','RMSE','d1','E1']
c1 = regions.tolist()
c2 = stats.tolist()
#species list
spec = va.df.Species.unique()
c3 = array(stats.shape[0],spec.shape[0])
for i,j in enumerate(spec):
    c3[:,i] = get_stats(va.groupby('Species').get_group(j))
    
index = [c1,c2]
df = pd.DataFrame(c3.T,columns=index,index=spec).T
df.to_html('test.html')

    
def get_stats(df):
    import mystats as ms
    obs = df.Obs.values
    model = df.CMAQ.values
    mb = ms.MB(obs,model)
    omean = obs.mean()
    mmean = model.mean()
    fb = ms.FB(obs,model)
    rmse = ms.RMSE(obs,model)
    d1 = ms.d1(obs,model)
    e1 = ms.E1(obs,mod)
    return omean,mmean,mb,fb,rmse,d1,e1
    
    
    
    
    
    
