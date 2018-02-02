#!/data/aqf/barryb/anaconda2/bin/python

###for AITKEN
#### /data/aqf/barryb/anaconda2/bin/python

###for WCOSS
### /naqfc/noscrub/Barry.Baker/anaconda2/bin/python
import f90nml
from numpy import sort
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from numpy import unique,sort
from datetime import datetime, timedelta
import pandas as pd
from glob import glob
from verify_airnow import verify_airnow

#Read the Namelist
nml = f90nml.read('comparesim_testbed.namelist')
base = nml['files']['basename']
gridcro = nml['files']['gridcro']
datapath = nml['files']['airnow_data_dir']
interp = nml['interp']['method']
neighbors = nml['interp']['neighbors']
radius = nml['interp']['radius_of_influence']

#airnow user and pass
usr = 'Barry.Baker'
p = 'p00pST!ck123'
date2days = datetime.now() - timedelta(days=2)
dateconc = date2days.strftime('%Y%m%d/aqm.t12z.aconc.ncf')
datemet = date2days.strftime('%Y%m%d/aqm.t12z.metcro2d.ncf')
#INTERP SIMULATIONS TO OBSERVATIONS
if nml['files']['sim1'].lower() != 'none':
    print 'Pairing Sim1...'
    print ' '
    if nml['files']['sim1'].lower()[-4:] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim1']
        sim1 = verify_airnow()
        sim1.df = pd.read_hdf(nml['files']['sim1'])
    else:
        import monet as m
        print nml['files']['sim1'] + dateconc
        files = sort(glob(nml['files']['sim1'] + dateconc))
        metfiles = nml['files']['sim1'] + datemet
        print metfiles
        print files
        sim1 = m.vairnow(concpath=files,gridcro=gridcro,met2dpath=metfiles,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius,user=usr,passw=p)
        print sim1.df.keys()

if nml['files']['sim2'].lower()!= 'none':
    print ' '
    print 'Pairing Sim2...'
    if nml['files']['sim2'].lower()[-4:] =='.hdf':
        print ' '
        print '  Loading Paired Data: ', nml['files']['sim2']
        sim2 = verify_airnow()
        sim2.df = pd.read_hdf(nml['files']['sim2'])
    else:
        files = sort(glob(nml['files']['sim2']))
        import monet as mm
        print files 
        sim2 = mm.vairnow(concpath=files,gridcro=gridcro,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius,user=usr,passw=p)
else:
    sim2=False

if nml['files']['sim3'].lower()!= 'none':
    print ' '
    print 'Pairing Sim3...'
    print ' '
    if nml['files']['sim3'].lower()[-4:] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim3']
        sim3 = verify_airnow()
        sim3.df = pd.read_hdf(nml['files']['sim3'])
    else:
        import monet as mmm
        files = sort(glob(nml['files']['sim3']))
        sim3 = mmm.vairnow(concpath=files,gridcro=gridcro,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius,user=usr,passw=p)
else:
    sim3 = False

if nml['files']['sim4'].lower()!= 'none':
    print 'Pairing Sim4...'
    print ' '
    if nml['files']['sim4'].lower()[:-4] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim4']
        import verify_airnow as vairnow
        sim4 = vairnow()
        sim4.df = pd.read_hdf(nml['files']['sim4'])
    else:
        import monet
        files = sort(glob(nml['files']['sim4']))
        sim4 = monet.vairnow(concpath=files,gridcro=gridcro,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius,user=usr,passw=p)
else:
    sim4 = False

date = sim1.cmaq.dates[0]
ymd= date.strftime('%Y%m%d')

#DOMAIN PLOTTING

if nml['domain']['params'].lower() != 'none':
    if nml['domain']['params'] == 'all':
        params = sort(sim1.df.Species.unique())
    else:
        params = nml['domain']['params'].split(',')
    for i in params:
        print i,'domain'
        if nml['domain']['tseries']:
            try:
                sim1.compare_param(param=i,timeseries=True,label=nml['files']['sim1label'])
                if sim2 is not False:
                    sim2.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 is not False:
                    sim3.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 is not False:
                    sim4.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                code = '00000'
                savename = ymd + '.5X.'+i.replace('.','P') +'.ts.'+code+'.png'
                plt.savefig(savename,dpi=75)
                print 'Saving: ' + savename
                plt.close('all')
            except:
                plt.close('all')
                pass
        if nml['domain']['tseriesrmse']:
            try:
                sim1.compare_param(param=i,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 is not False:
                    sim2.compare_param(param=i,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 is not False:
                    sim3.compare_param(param=i,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 is not False:
                    sim4.compare_param(param=i,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'timeseries_rmse.jpg',dpi=75)
                print 'Saving: ' + base +'_'+i.replace('.','')+'_'+'timeseries_rmse.jpg'
                plt.close('all')
            except:
                plt.close('all')
                pass
        if nml['domain']['tseriesbias']:
            try:
                sim1.compare_param(param=i,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 is not False:
                    sim2.compare_param(param=i,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 is not False:
                    sim3.compare_param(param=i,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 is not False:
                    sim4.compare_param(param=i,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'timeseries_mb.jpg',dpi=75)
                print 'Saving: ' + base +'_'+i.replace('.','')+'_'+'timeseries_mb.jpg'
                plt.close('all')
            except:
                plt.close('all')
                pass
        if nml['domain']['scatter']:
            try:
                sim1.compare_param(param=i,scatter=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 is not False:
                    sim2.compare_param(param=i,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 is not False:
                    sim3.compare_param(param=i,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 is not False:
                    sim4.compare_param(param=i,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'scatter.jpg',dpi=75)
                plt.close('all')
            except:
                plt.close('all')
                pass
        if nml['domain']['diffscatter']:
            try:
                sim1.compare_param(param=i,diffscatter=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 is not False:
                    sim2.compare_param(param=i,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 is not False:
                    sim3.compare_param(param=i,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 is not False:
                    sim4.compare_param(param=i,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'diffscatter.jpg',dpi=75)
                print 'Saving: ' + base +'_'+i.replace('.','')+'_'+'diffscatter.jpg'
                plt.close('all')
            except:
                plt.close('all')
                pass
        if nml['domain']['pdfs']:
            try:
                sim1.compare_param(param=i,pdfs=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 is not False:
                    sim2.compare_param(param=i,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 is not False:
                    sim3.compare_param(param=i,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 is not False:
                    sim4.compare_param(param=i,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'pdfs.jpg',dpi=75)
                print 'Saving: ' + base +'_'+i.replace('.','')+'_'+'pdfs.jpg'
                plt.close('all')
            except:
                plt.close('all')
                pass
        if nml['domain']['diffpdfs']:
            try:
                sim1.compare_param(param=i,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 is not False:
                    sim2.compare_param(param=i,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 is not False:
                    sim3.compare_param(param=i,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 is not False:
                    sim4.compare_param(param=i,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'diffpdfs.jpg',dpi=75)
                print 'Saving: ' + base +'_'+i.replace('.','')+'_'+'diffpdfs.jpg'
                plt.close('all')
            except:
                plt.close('all')
                pass
        if nml['domain']['taylordiagram']:
            try:
                dia = sim1.compare_param(param=i,taylordiagram=True,label=nml['files']['sim1label'])
                if sim2 is not False:
                    sim2.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia)
                if sim3 is not False:
                    sim3.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia)
                if sim4 is not False:
                    sim4.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia)
                plt.savefig(base +'_'+i.replace('.','')+'_'+'taylor.jpg',dpi=75)
                print 'Saving: ' + base +'_'+i.replace('.','')+'_'+'taylor.jpg'
                plt.close('all')
            except:
                plt.close('all')
                pass

#EPA Regions
if (nml['epa_region']['params'].lower() != 'none') & (nml['epa_region']['epa_region'].lower() !='none'):
    if nml['epa_region']['params'] == 'all':
        params = sort(sim1.df.Species.unique())
    else:
        params = nml['epa_region']['params'].split(',')
    if nml['epa_region']['epa_region'] =='all':
        regions = sim1.df.EPA_region.dropna().unique()
    else:
        regions = nml['epa_region']['epa_region'].split(',')
    for j in regions:
        for i in params:
            print i,j
            if nml['epa_region']['tseries']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    print 'here'
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    r = j.strip('R')
                    code = r.zfill(5)
                    savename = ymd + '.5X.'+ i.replace('.','P') +'.ts.'+code+'.png'
                    print savename
                    plt.savefig(savename,dpi=75)
                    print 'Saving: ', savename
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['tseriesrmse']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg',dpi=75)
                    print 'Saving: ' + base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg'
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['tseriesbias']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_mb.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['scatter']:
                try:
                    sim1.compare_param(param=i,region=j,scatter=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'scatter.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['diffscatter']:
                try:
                    sim1.compare_param(param=i,region=j,diffscatter=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffscatter.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['pdfs']:
                try:
                    sim1.compare_param(param=i,region=j,pdfs=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'pdfs.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['diffpdfs']:
                try:
                    sim1.compare_param(param=i,region=j,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffpdfs.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['taylordiagram']:
                try:
                    dia = sim1.compare_param(param=i,region=j,taylordiagram=True,label=nml['files']['sim1label'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia)
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia)
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia)
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass


#States
if (nml['state']['params'].lower() != 'none') & (nml['state']['state'].lower() !='none'):
    plt.close('all')
    if nml['state']['params'] == 'all':
        params = sim1.df.Species.unique()
    else:
        params = nml['state']['params'].split(',')
    if nml['state']['state'] =='all':
        states = sim1.df.State_Name.unique()
    else:
        states = nml['state']['state'].split(',')
    for j in states:
        for i in params:
            print i,j
            if nml['state']['tseries']:
                try:
                    sim1.compare_param(param=i,state=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,state=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['state']['tseriesrmse']:
                try:
                    sim1.compare_param(param=i,state=j,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,state=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['state']['tseriesbias']:
                try:
                    sim1.compare_param(param=i,state=j,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,state=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_mb.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['state']['scatter']:
                try:
                    sim1.compare_param(param=i,state=j,scatter=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,state=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'scatter.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['state']['diffscatter']:
                try:
                    sim1.compare_param(param=i,state=j,diffscatter=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,state=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffscatter.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['state']['pdfs']:
                try:
                    sim1.compare_param(param=i,state=j,pdfs=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,state=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'pdfs.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['state']['diffpdfs']:
                try:
                    sim1.compare_param(param=i,state=j,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,state=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffpdfs.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['state']['taylordiagram']:
                try:
                    dia = sim1.compare_param(param=i,state=j,taylordiagram=True,label=nml['files']['sim1label'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia)
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia)
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia)
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass

#CITY
if (nml['city']['params'].lower() != 'none') & (nml['city']['city'].lower() !='none'):
    if nml['city']['params'] == 'all':
        params = sim1.df.Species.unique()
    else:
        params = nml['city']['params'].split(',')
    if nml['city']['city'] =='all':
        citys = sim1.df.MSA_Name.unique()
    else:
        citys = nml['city']['city'].split(',')
    for j in citys:
        for i in params:
            print i,j
            names = sim1.df.MSA_Name.dropna().values
            codes = sim1.df.MSA_Code.dropna().values
            names,index = unique(names,return_index=True)
            codes = codes[index].astype('|S5')
            for k,p in zip(names,codes):
                if j.lower() in k.lower():
                    name = k
                    code = p
            if nml['city']['tseries']:
                try:
                    sim1.compare_param(param=i,city=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    savename = ymd + '.5X.'+ i.replace('.','P') +'.ts.'+code+'.png'
                    plt.savefig(savename,dpi=75)
                    print 'Saving: ', savename
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['city']['tseriesrmse']:
                try:
                    sim1.compare_param(param=i,city=j,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['city']['tseriesbias']:
                try:
                    sim1.compare_param(param=i,city=j,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_mb.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['city']['scatter']:
                try:
                    sim1.compare_param(param=i,city=j,scatter=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'scatter.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['city']['diffscatter']:
                try:
                    sim1.compare_param(param=i,city=j,diffscatter=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffscatter.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['city']['pdfs']:
                try:
                    sim1.compare_param(param=i,city=j,pdfs=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'pdfs.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['city']['diffpdfs']:
                try:
                    sim1.compare_param(param=i,city=j,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffpdfs.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['city']['taylordiagram']:
                try:
                    dia = sim1compare_param(param=i,city=j,taylordiagram=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia)
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia)
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia)
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass


def make_tseries_name(sim,var,city='',region=''):
    date = sim.cmaq.dates[0]
    print date
    ymd= date.strftime('%Y.%m.%d')
    print var,city,region
    if city != '':
        names = sim.df.MSA_Name.dropna().values
        codes = sim.df.MSA_Code.dropna().values
        names,index = unique(names,return_index=True)
        codes = codes[index].astype('|S5')
        for i,j in zip(names,codes):
            if city.upper() in i.upper():
                name = i
                code = j
    elif region != '':
        r = region.strip('R')
        code = r.zfill(5)
    elif (region == '') & (city == ''):
        code = '00000'
    savename = ymd + '.5X.'+var +'.ts.'+code+'.png'
    return savename

