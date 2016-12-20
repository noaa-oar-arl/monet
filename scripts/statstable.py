#!/data/aqf/barryb/anaconda2/bin/python
###for aitken replace the first line with /data/aqf/barryb/anaconda2/bin/python for the line above
#   To use this function you exectute as follows
#
# ./5x_domain_compare.py PATH/TO/ACONC PATH/TO/GRIDCRO LABEL USERNAME PASSWORD

import verify
from glob import glob
import matplotlib.pyplot as plt
import sys
from numpy import arange,array,empty,concatenate,append,NaN
from pandas import Series,DataFrame, MultiIndex
def main():
    print "Name of Script: ", sys.argv[0]
    print "ACONC FILE: ", sys.argv[1]
    print "GRIDCRO2D FILE: ", sys.argv[2]

    files = glob(sys.argv[1])
    grid = sys.argv[2]

    va = verify.verify_airnow(concpath=files,gridcro=grid,datapath='AIRNOW.hdf',combine=True,neighbors=9,user='Barry.Baker',passw='p00pST!ck123')

    regions = arange(7).astype('|S15')
    regions[:] = 'Domain'
    stats = ['Obs Mean','Model Mean','Mean Bias','Fractional Bias','RMSE','d1','E1']
    c1 = regions
    c2 = array(stats)
    #species list
    spec = va.df.Species.unique()
    c3 = empty((array(stats).shape[0],spec.shape[0]))
    print c3.shape
    print c1.shape,c2.shape
    for i,j in enumerate(spec):
        c3[:,i] = get_stats(va.df.groupby('Species').get_group(j))
        
    for k,region in enumerate(va.df['Region'].dropna().unique()):
        temp = arange(7).astype('|S15')
        temp[:] = region
        c1 = append(c1,temp)
        c2 = append(c2,array(stats))
        dftemp = va.df.groupby('Region').get_group(region)
        c = empty((array(stats).shape[0],spec.shape[0]))
        c[:] = 0.
        for i,j in enumerate(spec):
            if j not in dftemp.Species.unique():
                c[:,i] = NaN
            else:
                c[:,i] = get_stats(dftemp.groupby('Species').get_group(j))
        c3 = concatenate([c3,c],axis=0)
            
    tuples = list(zip(*[c1.tolist(),c2.tolist()]))
    print tuples
    index =  MultiIndex.from_tuples(tuples,names=['Area','Statistic'])
    print c1.shape,c2.shape
    print c3.shape
    print c3
    df =DataFrame(c3.T,columns=index,index=spec).round(decimals=3).T


    df.to_html('test.html')


    
    cssstyle = '<style>\n.GenericTable\n{\nfont-size:12px;\ncolor:white;\nborder-width: 1px;\nborder-color: rgb(160,160,160);/* This is dark*/\nborder-collapse: collapse;\n}\n.GenericTable th\n{\nfont-size:16px;\ncolor:white;\nbackground-color:rgb(100,100,100);/* This is dark*/\nborder-width: 1px;\npadding: 4px;\nborder-style: solid;\nborder-color: rgb(192, 192, 192);/* This is light*/\ntext-align:left;\n}\n.GenericTable tr\n{\ncolor:black;\nbackground-color:rgb(224, 224, 224);/* This is light*/\n}\n.GenericTable td\n{\nfont-size:14px;\nborder-width: 1px;\nborder-style: solid;\nborder-color: rgb(255, 255, 255);/* This is dark*/\n}\n.hoverTable{\nwidth:100%; \nborder-collapse:collapse; \n}\n.hoverTable td{ \npadding:7px; border:#E0E0E0 1px solid;\n}\n/* Define the default color for all the table rows */\n.hoverTable tr{\nbackground: #C0C0C0;\n}\n/* Define the hover highlight color for the table row */\n    .hoverTable tr:hover {\n          background-color: #ffff99;\n    }\n</style>'

    lines = cssstyle.split('\n')
    with open('test.html','r') as f:
        for line in f.readlines():
            lines.append(line.replace('class="dataframe"','class="GenericTable hoverTable"'))
    f.close()
    with open('test.html', 'w') as f:
        for line in lines:
            f.write(line)
    f.close()


def get_stats(df):
    import mystats as ms
    obs = df.Obs.dropna().values
    model = df.CMAQ.dropna().values
    mb = ms.MB(obs,model)
    omean = obs.mean()
    mmean = model.mean()
    fb = ms.FB(obs,model)
    rmse = ms.RMSE(obs,model)
    d1 = ms.d1(obs,model)
    e1 = ms.E1(obs,model)
    return omean,mmean,mb,fb,rmse,d1,e1


if __name__ == "__main__":
    main()



