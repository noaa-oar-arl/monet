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
import sys
import utils
#Read the Namelist
nml = f90nml.read('comparesim.namelist')
base = nml['files']['basename']
gridcro = nml['files']['gridcro']
datapath = nml['files']['data_dir']
interp = nml['interp']['method']
neighbors = nml['interp']['neighbors']
radius = nml['interp']['radius_of_influence']

#airnow user and pass
usr = 'Barry.Baker'
p = 'p00pST!ck123'

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
        files = sort(glob(nml['files']['sim1']))
        print 'sim1met', type(nml['files']['sim1met'])
        if nml['files']['sim1met'].lower() == 'none':
            metfiles = ''
        else:
            metfiles = sort(glob(nml['files']['sim1met']))
        print metfiles
        if nml['files']['obs_network'].upper() == 'AQS':
            print 'here'
            sim1 = m.vaqs(concpath=files,gridcro=gridcro,met2dpath=metfiles,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius)
        else:
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
        metfiles = nml['files']['sim2met']
        if nml['files']['sim2met'].lower() == 'none':
            metfiles = ''
        else:
            metfiles = sort(glob(nml['files']['sim2met']))
        import monet as mm
        if nml['files']['obs_network'].upper() == 'AQS':
            sim2 = mm.vaqs(concpath=files,gridcro=gridcro,met2dpath=metfiles,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius)
        else:
            sim2 = mm.vairnow(concpath=files,gridcro=gridcro,met2dpath=metfiles,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius,user=usr,passw=p)
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
        if nml['files']['sim3met'].lower() == 'none':
            metfiles = ''
        else:
            metfiles = sort(glob(nml['files']['sim3met']))

        if nml['files']['obs_network'].upper() == 'AQS':
            sim3 = mmm.vaqs(concpath=files,gridcro=gridcro,met2dpath=metfiles,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius)
        else:
            sim3 = mmm.vairnow(concpath=files,gridcro=gridcro,met2dpath=metfiles,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius,user=usr,passw=p)
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
        if nml['files']['sim4met'].lower() == 'none':
            metfiles = ''
        else:
            metfiles = sort(glob(nml['files']['sim4met']))
        if nml['files']['obs_network'].upper() == 'AQS':
            sim4 = monet.vaqs(concpath=files,gridcro=gridcro,met2dpath=metfiles,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius)
        else:
            sim4 = monet.vairnow(concpath=files,gridcro=gridcro,met2dpath=metfiles,datapath=datapath,interp=interp,neighbors=neighbors,radius=radius,user=usr,passw=p)
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

sim1.df.dropna(subset=['Obs','CMAQ'],inplace=True)
if sim2 is not False:
    sim2.df.dropna(subset=['Obs','CMAQ'],inplace=True)
if sim3 is not False:
    sim3.df.dropna(subset=['Obs','CMAQ'],inplace=True)
if sim4 is not False:
    sim4.df.dropna(subset=['Obs','CMAQ'],inplace=True)

#date = sim1.cmaq.dates[0]
#ymd= date.strftime('%Y%m%d')

#DOMAIN PLOTTING

if nml['domain']['params'].lower() != 'none':
    if nml['domain']['params'] == 'all':
        params = sort(sim1.df.Species.unique())
    else:
        params = nml['domain']['params'].split(',')
    first = True
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
                code = '00000'
                savename =  base +'_'+i.replace('.','')+'_'+'timeseries.jpg'
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
                    sim2.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,marker='<')
                if sim3 is not False:
                    sim3.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,marker='*')
                if sim4 is not False:
                    sim4.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,maker='D')
                plt.savefig(base +'_'+i.replace('.','')+'_'+'taylor.jpg',dpi=75)
                print 'Saving: ' + base +'_'+i.replace('.','')+'_'+'taylor.jpg'
                plt.close('all')
            except:
                plt.close('all')
                pass
        if nml['domain']['stats']:
            try:
                if first:
                    df,title = utils.get_epa_location_df(sim1.df,i)
                    utils.write_table(df=df,fname='DOMAIN',label=nml['files']['sim1label'],location='Domain')
                    first=False
                else:
                    df,title = utils.get_epa_location_df(sim1.df,i)
                    utils.write_table(df=df,fname='DOMAIN',label=nml['files']['sim1label'],location='Domain',append=True)
                if sim2 is not False:
                    df,title = utils.get_epa_location_df(sim2.df,i)
                    utils.write_table(df=df,fname='DOMAIN',label=nml['files']['sim2label'],append=True,location='Domain')
                if sim3 is not False:
                    df,title = utils.get_epa_location_df(sim3.df,i)
                    utils.write_table(df=df,fname='DOMAIN',label=nml['files']['sim3label'],append=True,location='Domain')
                if sim4 is not False:
                    df,title = utils.get_epa_location_df(sim4.df,i)
                    utils.write_table(df=df,fname='DOMAIN',label=nml['files']['sim4label'],append=True,location='Domain')
            except:
                pass

#Regions
if (nml['region']['params'].lower() != 'none') & (nml['region']['region'].lower() !='none'):
    sim1.df = utils.get_region(sim1.df)
    if sim2 is not False:
        sim2.df = utils.get_region(sim2.df)
    if sim3 is not False:
        sim3.df = utils.get_region(sim3.df)
    if sim4 is not False:
        sim4.df = utils.get_region(sim4.df)
    if nml['region']['params'] == 'all':
        params = sort(sim1.df.Species.unique())
    else:
        params = nml['region']['params'].split(',')
    if nml['region']['region'] =='all':
        regions = sim1.df.Region.dropna().unique()
    else:
        regions = nml['region']['region'].split(',')
    first=True
    for j in regions:
        for i in params:
            print i,j
            if nml['region']['tseries']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
                    print 'here'
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
                #try:
                    dia = sim1.compare_param(param=i,region=j,taylordiagram=True,label=nml['files']['sim1label'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,marker='<')
                    if sim3 is not False:
                        sim3.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,maker='*')
                    if sim4 is not False:
                        sim4.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,marker='D')
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close('all')
                #except:
                #    plt.close('all')
                #    pass
            if nml['region']['stats']:
                try:
                    if first:
                        df,title = utils.get_epa_location_df(sim1.df,i,region=j)
                        utils.write_table(df=df,fname='REGION',label=nml['files']['sim1label'],location=j)
                        first=False
                    else:
                        df,title = utils.get_epa_location_df(sim1.df,i,region=j)
                        utils.write_table(df=df,fname='REGION',label=nml['files']['sim1label'],location=j,append=True)
                    if sim2 is not False:
                        df,title = utils.get_epa_location_df(sim2.df,i,region=j)
                        utils.write_table(df=df,fname='REGION',label=nml['files']['sim2label'],append=True,location=j)
                    if sim3 is not False:
                        df,title = utils.get_epa_location_df(sim3.df,i,region=j)
                        utils.write_table(df=df,fname='REGION',label=nml['files']['sim3label'],append=True,location=j)
                    if sim4 is not False:
                        df,title = utils.get_epa_location_df(sim4.df,i,region=j)
                        utils.write_table(df=df,fname='REGION',label=nml['files']['sim4label'],append=True,location=j)
                except:
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
    first = True
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

                    print 'Saving: ', savename
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
                        sim2.compare_param(param=i,epa_region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,marker='<')
                    if sim3 is not False:
                        sim3.compare_param(param=i,epa_region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,maker='*')
                    if sim4 is not False:
                        sim4.compare_param(param=i,epa_region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,marker='D')
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['epa_region']['stats']:
                try:
                    if first:
                        df,title = utils.get_epa_location_df(sim1.df,i,epa_region=j)
                        utils.write_table(df=df,fname='EPA_REGION',label=nml['files']['sim1label'],location=j)
                        first=False
                    else:
                        df,title = utils.get_epa_location_df(sim1.df,i,epa_region=j)
                        utils.write_table(df=df,fname='EPA_REGION',label=nml['files']['sim1label'],location=j,append=True)
                    if sim2 is not False:
                        df,title = utils.get_epa_location_df(sim2.df,i,epa_region=j)
                        utils.write_table(df=df,fname='EPA_REGION',label=nml['files']['sim2label'],append=True,location=j)
                    if sim3 is not False:
                        df,title = utils.get_epa_location_df(sim3.df,i,epa_region=j)
                        utils.write_table(df=df,fname='EPA_REGION',label=nml['files']['sim3label'],append=True,location=j)
                    if sim4 is not False:
                        df,title = utils.get_epa_location_df(sim4.df,i,epa_region=j)
                        utils.write_table(df=df,fname='EPA_REGION',label=nml['files']['sim4label'],append=True,location=j)
                except:
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
    first = True
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
                        sim2.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,marker='<')
                    if sim3 is not False:
                        sim3.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,maker='*')
                    if sim4 is not False:
                        sim4.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,marker='D')
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['state']['stats']:
                try:
                    if first:
                        df,title = utils.get_epa_location_df(sim1.df,i,state=j)
                        utils.write_table(df=df,fname='STATE',label=nml['files']['sim1label'],location=j)
                        first=False
                    else:
                        df,title = utils.get_epa_location_df(sim1.df,i,state=j)
                        utils.write_table(df=df,fname='STATE',label=nml['files']['sim1label'],location=j,append=True)
                    if sim2 is not False:
                        df,title = utils.get_epa_location_df(sim2.df,i,state=j)
                        utils.write_table(df=df,fname='STATE',label=nml['files']['sim2label'],append=True,location=j)
                    if sim3 is not False:
                        df,title = utils.get_epa_location_df(sim3.df,i,state=j)
                        utils.write_table(df=df,fname='STATE',label=nml['files']['sim3label'],append=True,location=j)
                    if sim4 is not False:
                        df,title = utils.get_epa_location_df(sim4.df,i,state=j)
                        utils.write_table(df=df,fname='STATE',label=nml['files']['sim4label'],append=True,location=j)
                except:
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
    first = True
    for j in citys:
        for i in params:
            print i,j
#            names = sim1.df.MSA_Name.dropna().values
#            codes = sim1.df.MSA_Code.dropna().values
#            names,index = unique(names,return_index=True)
 #           codes = codes[index].astype('|S5')
#            for k,p in zip(names,codes):
#                if j.lower() in k.lower():
#                    name = k
#                    code = p
            if nml['city']['tseries']:
                try:
                    sim1.compare_param(param=i,city=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 is not False:
                        sim2.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    savename = base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg'
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
                        sim2.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,marker='<')
                    if sim3 is not False:
                        sim3.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,maker='*')
                    if sim4 is not False:
                        sim4.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,marker='D')
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close('all')
                except:
                    plt.close('all')
                    pass
            if nml['city']['stats']:
                try:
                    if first:
                        df,title = utils.get_epa_location_df(sim1.df,i,city=j)
                        utils.write_table(df=df,fname='CITY',label=nml['files']['sim1label'],location=j)
                        first=False
                    else:
                        df,title = utils.get_epa_location_df(sim1.df,i,city=j)
                        utils.write_table(df=df,fname='CITY',label=nml['files']['sim1label'],location=j,append=True)
                    if sim2 is not False:
                        df,title = utils.get_epa_location_df(sim2.df,i,city=j)
                        utils.write_table(df=df,fname='CITY',label=nml['files']['sim2label'],append=True,location=j)
                    if sim3 is not False:
                        df,title = utils.get_epa_location_df(sim3.df,i,city=j)
                        utils.write_table(df=df,fname='CITY',label=nml['files']['sim3label'],append=True,location=j)
                    if sim4 is not False:
                        df,title = utils.get_epa_location_df(sim4.df,i,city=j)
                        utils.write_table(df=df,fname='CITY',label=nml['files']['sim4label'],append=True,location=j)
                except:
                    pass


if nml['spatial']['params'].lower() != 'none':
    import plots
    import subprocess
    from numpy import array
    print '==============================================='
    print '    LOADING BASEMAP'
    print '==============================================='
    sim1.cmaq.load_conus_basemap('.')
    m = sim1.cmaq.map
    if nml['spatial']['params'] == 'all':
        params = sort(sim1.df.Species.unique())
    else:
        params = nml['spatial']['params'].split(',')
    if nml['spatial']['bias']:
        for i in sort(params):
            df = sim1.df.groupby('Species').get_group(i)
            for j,dates in enumerate(sim1.cmaq.dates[sim1.cmaq.indexdates]):
                f,ax,c = plots.spatial_bias_scatter(df,sim1.cmaq.map,dates,ncolors=21,cmap='RdBu_r')
                m.drawstates();m.drawcoastlines();m.drawcountries()
                ax = plt.gca()
                plt.title(dates.strftime('%d/%m/%Y %H   Model - Obs'), fontsize=13)
                c.set_label(i + ' (' + df.Units.unique()[0] + ')',fontsize=13)
                plt.tight_layout()
                dd = sim1.cmaq.dates[sim1.cmaq.indexdates][0].strftime('%Y%m%d.5X.')
                savename = dd + i.replace('.','P') +'.spbias.' + str(j).zfill(2) + '.jpg'
                print savename
                plt.savefig(savename)
                plt.close('all')
            print 'Generating GIF Animation'
            subprocess.call(['convert','-delay','50',dd+i.replace('.','P')+'.spbias.*.jpg',dd+i.replace('.','P')+'.spbias.ani.gif'])
            alldates = df.datetime_local.values.astype('M8[s]').astype('O')
            hours = array([int(p.strftime('%H')) for p in alldates])
            day = (hours > 10) & (hours <= 18)
            night = (hours > 21) | (hours <= 4)
            for k,j in zip([day,night],['dt','nt']):
                dfnew = df[k].groupby('SCS').mean().reset_index(level=0)
                dfnew['datetime'] = sim1.cmaq.dates.min()
                f,ax,c = plots.spatial_bias_scatter(dfnew,sim1.cmaq.map,sim1.cmaq.dates.min(),ncolors=21,cmap='RdBu_r')
                m.drawstates();m.drawcoastlines();m.drawcountries()
                ax = plt.gca()
                c.set_label(i + ' (' + df.Units.unique()[0] + ')',fontsize=13)
                plt.tight_layout()
                dd = sim1.cmaq.dates[sim1.cmaq.indexdates][0].strftime('%Y%m%d.5X.')
                savename = dd + i.replace('.','P') +'.spbias.' + j+ '.jpg'
                plt.savefig(savename)
                print savename
                plt.close('all')
    if nml['spatial']['monitors']:
        for i in sort(params):
            df = sim1.df.groupby('Species').get_group(i)
            lats,index = unique(df.Latitude.values,return_index=True)
            lons = df.Longitude.values[index]
            m.plot(lons,lats,'.',latlon=True,color='dodgerblue')
            m.drawstates();m.drawcoastlines();m.drawcountries()
            plt.title(i + ' Monitor Locations' + str(lats.shape[0]))
            plt.tight_layout()
            plt.savefig('monitor_location_' + i.replace('.','P') + '.jpg')
            plt.close('all')
            
