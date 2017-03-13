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
from datetime import datetime
import pandas as pd
from glob import glob
from verify_aqs import verify_aqs
#Read the Namelist
nml = f90nml.read('comparesim.namelist')

base = nml['files']['basename']
gridcro = nml['files']['gridcro']
datapath = nml['files']['aqs_data_dir']
interp = nml['interp']['method']
neighbors = nml['interp']['neighbors']
radius = nml['interp']['radius_of_influence']

#INTERP SIMULATIONS TO OBSERVATIONS
if nml['files']['sim1'].lower() != 'none':
    print 'Pairing Sim1...'
    print ' '
    if nml['files']['sim1'].lower()[-4:] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim1']
        sim1 = verify_aqs()
        sim1.df = pd.read_hdf(nml['files']['sim1'])
    else:
        import monet as m
        files = sort(glob(nml['files']['sim1']))
        print files
        sim1 = m.vaqs(concpath=files,gridcro=gridcro,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius)

if nml['files']['sim2'].lower()!= 'none':
    print ' '
    print 'Pairing Sim2...'
    if nml['files']['sim2'].lower()[-4:] =='.hdf':
        print ' '
        print '  Loading Paired Data: ', nml['files']['sim2']
        sim2 = verify_aqs()
        sim2.df = pd.read_hdf(nml['files']['sim2'])
    else:
        files = sort(glob(nml['files']['sim2']))
        import monet as mm
        print files 
        sim2 = mm.vaqs(concpath=files,gridcro=gridcro,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius)
else:
    sim2=False

if nml['files']['sim3'].lower()!= 'none':
    print ' '
    print 'Pairing Sim3...'
    print ' '
    if nml['files']['sim3'].lower()[-4:] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim3']
        sim3 = verify_aqs()
        sim3.df = pd.read_hdf(nml['files']['sim3'])
    else:
        import monet as mmm
        files = sort(glob(nml['files']['sim3']))
        sim3 = mmm.vaqs(concpath=files,gridcro=gridcro,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius)
else:
    sim3 = False

if nml['files']['sim4'].lower()!= 'none':
    print 'Pairing Sim4...'
    print ' '
    if nml['files']['sim4'].lower()[:-4] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim4']
        import verify_aqs as vaqs
        sim4 = vaqs()
        sim4.df = pd.read_hdf(nml['files']['sim4'])
    else:
        import monet
        files = sort(glob(nml['files']['sim4']))
        sim4 = monet.vaqs(concpath=files,gridcro=gridcro,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius)
else:
    sim4 = False

if nml['files']['save']:
    sim1.df.to_hdf(nml['files']['sim1_save_name'],'df',format='fixed')
    if nml['files']['sim2'].lower() != 'none':
        sim2.df.to_hdf(nml['files']['sim2_save_name'],'df',format='fixed')
    if nml['files']['sim3'].lower() != 'none':
        sim3.df.to_hdf(nml['files']['sim3_save_name'],'df',format='fixed')
    if nml['files']['sim4'].lower() != 'none':
        sim4.df.to_hdf(nml['files']['sim4_save_name'],'df',format='fixed')

if (nml['files']['start_date'] != 'none') & (nml['files']['end_date'] != 'none'):
    if datetime.strptime(nml['files']['start_date'],'%Y-%m-%d %H') > datetime.strptime(nml['files']['end_date'],'%Y-%m-%d %H'):
        print 'end_date must be larger than than start_date'
        exit
    con = (sim1.df.datetime >= datetime.strptime(nml['files']['start_date'],'%Y-%m-%d %H'))  & (sim1.df.datetime <= datetime.strptime(nml['files']['end_date'],'%Y-%m-%d %H'))
    sim1.df = sim1.df.copy()[con]
    if sim2 is not False:
        sim2.df = sim2.df[sim2.df['datetime'] >= datetime.strptime(nml['files']['start_date'],'%Y-%m-%d %H')]
        sim2.df = sim2.df[sim2.df['datetime'] <= datetime.strptime(nml['files']['end_date'],'%Y-%m-%d %H')]
    if sim3 is not False:
        con = (sim3.df.datetime >= datetime.strptime(nml['files']['start_date'],'%Y-%m-%d %H'))  & (sim3.df.datetime <= datetime.strptime(nml['files']['end_date'],'%Y-%m-%d %H'))
        sim3.df=sim3.df.copy()[con]
    if sim4 is not False:
        con = (sim4.df.datetime >= datetime.strptime(nml['files']['start_date'],'%Y-%m-%d %H'))  & (sim4.df.datetime <= datetime.strptime(nml['files']['end_date'],'%Y-%m-%d %H'))
        sim4.df=sim4.df.copy()[con]
#DOMAIN PLOTTING

if nml['domain']['params'].lower() != 'none':
    if nml['domain']['params'] == 'all':
        params = sim1.df.Species.unique()
    else:
        params = nml['domain']['params'].split(',')
    for i in params:
        print i,'domain'
        if nml['domain']['tseries']:
            try:
                sim1.compare_param(param=i,timeseries=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 is not False:
                    sim2.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 is not False:
                    sim3.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 is not False:
                    sim4.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'timeseries.jpg',dpi=75)
                print 'Saving: ' + base +'_'+i.replace('.','')+'_'+'timeseries.jpg'
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
#Regions
if (nml['region']['params'].lower() != 'none') & (nml['region']['region'].lower() !='none'):
    if nml['region']['params'] == 'all':
        params = sim1.df.Species.unique()
    else:
        params = nml['region']['params'].split(',')
    if nml['region']['region'] =='all':
        regions = sim1.df.Region.unique()
    else:
        regions = nml['region']['region'].split(',')
    for j in regions:
        for i in params:
            print i,j
            if nml['region']['tseries']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg',dpi=75)
                    print 'Saving: ' + base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg'
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['region']['tseriesrmse']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
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
            if nml['region']['tseriesbias']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
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
            if nml['region']['scatter']:
                try:
                    sim1.compare_param(param=i,region=j,scatter=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
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
            if nml['region']['diffscatter']:
                try:
                    sim1.compare_param(param=i,region=j,diffscatter=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
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
            if nml['region']['pdfs']:
                try:
                    sim1.compare_param(param=i,region=j,pdfs=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
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
            if nml['region']['diffpdfs']:
                try:
                    sim1.compare_param(param=i,region=j,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
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
            if nml['region']['taylordiagram']:
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

#EPA Regions
if (nml['epa_region']['params'].lower() != 'none') & (nml['epa_region']['epa_region'].lower() !='none'):
    if nml['epa_region']['params'] == 'all':
        params = sim1.df.Species.unique()
    else:
        params = nml['epa_region']['params'].split(',')
    if nml['epa_region']['epa_region'] =='all':
        regions = sim1.df.EPA_region.dropna().unique()
        print regions
    else:
        regions = nml['epa_region']['epa_region'].split(',')
    for j in regions:
        for i in params:
            print i,j
            if nml['epa_region']['tseries']:
                try:
                    sim1.compare_param(param=i,epa_region=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    print 'here'
                    if sim2 is not False:
                        sim2.compare_param(param=i,epa_region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg',dpi=75)
                    print 'Saving: ' + base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg'
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['tseriesrmse']:
                try:
                    sim1.compare_param(param=i,epa_region=j,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,epa_region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg',dpi=75)
                    print 'Saving: ' + base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg'
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['tseriesbias']:
                try:
                    sim1.compare_param(param=i,epa_region=j,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,epa_region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_mb.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['scatter']:
                try:
                    sim1.compare_param(param=i,epa_region=j,scatter=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,epa_region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'scatter.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['diffscatter']:
                try:
                    sim1.compare_param(param=i,epa_region=j,diffscatter=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,epa_region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffscatter.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['pdfs']:
                try:
                    sim1.compare_param(param=i,epa_region=j,pdfs=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,epa_region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'pdfs.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['diffpdfs']:
                try:
                    sim1.compare_param(param=i,epa_region=j,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['epa_region']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,epa_region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffpdfs.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['taylordiagram']:
                try:
                    dia = sim1.compare_param(param=i,epa_region=j,taylordiagram=True,label=nml['files']['sim1label'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,epa_region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia)
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia)
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia)
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
            if nml['city']['tseries']:
                try:
                    sim1.compare_param(param=i,city=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg',dpi=75)
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


#  LocalWords:  nml