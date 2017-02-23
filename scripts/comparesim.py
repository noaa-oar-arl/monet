#!/naqfc/noscrub/Barry.Baker/anaconda2/bin/python

###for AITKEN
#### /data/aqf/barryb/anaconda2/bin/python

###for WCOSS
### /naqfc/noscrub/Barry.Baker/anaconda2/bin/python
import monet
import f90nml
from numpy import sort

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
from verify_aqs import verify_aqs

#Read the Namelist
nml = f90nml.read('comparesim.namelist')

base = nml['files']['basename']

from glob import glob
#!files = sort(glob(nml['files']['sim1']))

#INTERP SIMULATIONS TO OBSERVATIONS
if nml['files']['sim1'].lower() != 'none':
    print 'Pairing Sim1...'
    print ' '
    print nml['files']['sim1'].lower()[:-4]
    if nml['files']['sim1'].lower()[-4:] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim1']
        sim1 = verify_aqs()
        sim1.df = pd.read_hdf(nml['files']['sim1'])
    else:
        files = sort(glob(nml['files']['sim1']))
        print files
        sim1 = monet.verify_aqs(concpath=files,gridcro=nml['files']['gridcro'],datapath=nml['files']['aqs_data_dir'],interp=nml['interp']['method'],neighbors=nml['interp']['neighbors'],radius=nml['interp']['radius_of_influence'])

if nml['files']['sim2'].lower()!= 'none':
    print 'Pairing Sim2...'
    print ' '
    if nml['files']['sim2'].lower()[-4:] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim2']
        sim2 = verify_aqs()
        sim2.df = pd.read_hdf(nml['files']['sim2'])
    else:
        files = sort(glob(nml['files']['sim2']))
        sim2 = monet.verify_aqs(concpath=files,gridcro=nml['files']['gridcro'],datapath=nml['files']['aqs_data_dir'],interp=nml['interp']['method'],neighbors=nml['interp']['neighbors'],radius=nml['interp']['radius_of_influence'])
else:
    sim2='none'

if nml['files']['sim3'].lower()!= 'none':
    print 'Pairing Sim3...'
    print ' '
    if nml['files']['sim3'].lower()[-4:] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim3']
        sim3 = verify_aqs()
        sim3.df = pd.read_hdf(nml['files']['sim3'])
    else:
        files = sort(glob(nml['files']['sim3']))
        sim3 = monet.verify_aqs(concpath=files,gridcro=nml['files']['gridcro'],datapath=nml['files']['aqs_data_dir'],interp=nml['interp']['method'],neighbors=nml['interp']['neighbors'],radius=nml['interp']['radius_of_influence'])
else:
    sim3 = 'none'

if nml['files']['sim4'].lower()!= 'none':
    print 'Pairing Sim4...'
    print ' '
    if nml['files']['sim4'].lower()[:-4] =='.hdf':
        print '  Loading Paired Data: ', nml['files']['sim4']
        sim4 = verify_aqs()
        sim4.df = pd.read_hdf(nml['files']['sim4'])
    else:
        files = sort(glob(nml['files']['sim4']))
        sim4 = monet.verify_aqs(concpath=files,gridcro=nml['files']['gridcro'],datapath=nml['files']['aqs_data_dir'],interp=nml['interp']['method'],neighbors=nml['interp']['neighbors'],radius=nml['interp']['radius_of_influence'])
else:
    sim4 = 'none'

if nml['files']['save']:
    sim1.df.to_hdf(nml['files']['sim1_save_name'],'df',format='fixed')
    if nml['files']['sim2'].lower()!= 'none':
        sim2.df.to_hdf(nml['files']['sim2_save_name'],'df',format='fixed')
    if nml['files']['sim3'].lower()!= 'none':
        sim3.df.to_hdf(nml['files']['sim3_save_name'],'df',format='fixed')
    if nml['files']['sim4'].lower()!= 'none':
        sim4.df.to_hdf(nml['files']['sim4_save_name'],'df',format='fixed')

if (nml['files']['start_date'] != 'none') & (nml['files']['end_date'] != 'none'):
    con = (sim1.df.datetime >= datetime.strptime(nml['files']['start_date'],'%Y-%m-%d %H'))  & (sim1.df.datetime <= datetime.strptime(nml['files']['end_date'],'%Y-%m-%d %H'))
    sim1.df = sim1.df.copy()[con]
    if nml['files']['sim2'].lower()!= 'none':
        con = (sim2.df.datetime >= datetime.strptime(nml['files']['start_date'],'%Y-%m-%d %H'))  & (sim2.df.datetime <= datetime.strptime(nml['files']['end_date'],'%Y-%m-%d %H'))
        sim2.df =sim2.df.copy()[con]
    if nml['files']['sim3'].lower()!= 'none':
        con = (sim3.df.datetime >= datetime.strptime(nml['files']['start_date'],'%Y-%m-%d %H'))  & (sim3.df.datetime <= datetime.strptime(nml['files']['end_date'],'%Y-%m-%d %H'))
        sim3.df=sim3.df.copy()[con]
    if nml['files']['sim4'].lower()!= 'none':
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
                if sim2 != 'none':
                    sim2.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 != 'none':
                    sim3.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 != 'none':
                    sim4.compare_param(param=i,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'timeseries.jpg',dpi=75)
                plt.close()
            except:
                pass
        if nml['domain']['tseriesrmse']:
            try:
                sim1.compare_param(param=i,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 != 'none':
                    sim2.compare_param(param=i,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 != 'none':
                    sim3.compare_param(param=i,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 != 'none':
                    sim4.compare_param(param=i,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'timeseries_rmse.jpg',dpi=75)
                plt.close()
            except:
                pass
        if nml['domain']['tseriesbias']:
            try:
                sim1.compare_param(param=i,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 != 'none':
                    sim2.compare_param(param=i,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 != 'none':
                    sim3.compare_param(param=i,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 != 'none':
                    sim4.compare_param(param=i,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'timeseries_mb.jpg',dpi=75)
                plt.close()
            except:
                pass
        if nml['domain']['scatter']:
            try:
                sim1.compare_param(param=i,scatter=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 != 'none':
                    sim2.compare_param(param=i,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 != 'none':
                    sim3.compare_param(param=i,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 != 'none':
                    sim4.compare_param(param=i,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'scatter.jpg',dpi=75)
                plt.close()
            except:
                pass
        if nml['domain']['diffscatter']:
            try:
                sim1.compare_param(param=i,diffscatter=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 != 'none':
                    sim2.compare_param(param=i,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 != 'none':
                    sim3.compare_param(param=i,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 != 'none':
                    sim4.compare_param(param=i,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'diffscatter.jpg',dpi=75)
                plt.close()
            except:
                pass
        if nml['domain']['pdfs']:
            try:
                sim1.compare_param(param=i,pdfs=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 != 'none':
                    sim2.compare_param(param=i,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 != 'none':
                    sim3.compare_param(param=i,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 != 'none':
                    sim4.compare_param(param=i,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'pdfs.jpg',dpi=75)
                plt.close()
            except:
                pass
        if nml['domain']['diffpdfs']:
            try:
                sim1.compare_param(param=i,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['domain']['footers'])
                if sim2 != 'none':
                    sim2.compare_param(param=i,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                if sim3 != 'none':
                    sim3.compare_param(param=i,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                if sim4 != 'none':
                    sim4.compare_param(param=i,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                plt.savefig(base +'_'+i.replace('.','')+'_'+'diffpdfs.jpg',dpi=75)
                plt.close()
            except:
                pass
        if nml['domain']['taylordiagram']:
            try:
                dia = sim1.compare_param(param=i,taylordiagram=True,label=nml['files']['sim1label'])
                if sim2 != 'none':
                    sim2.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,addon=True)
                if sim3 != 'none':
                    sim3.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,addon=True)
                if sim4 != 'none':
                    sim4.compare_param(param=i,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,addon=True)
                plt.savefig(base +'_'+i.replace('.','')+'_'+'taylor.jpg',dpi=75)
                plt.close()
            except:
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
                    if sim2 != 'none':
                        sim2.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,region=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['region']['tseriesrmse']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,region=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['region']['tseriesbias']:
                try:
                    sim1.compare_param(param=i,region=j,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,region=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_mb.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['region']['scatter']:
                try:
                    sim1.compare_param(param=i,region=j,scatter=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,region=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'scatter.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['region']['diffscatter']:
                try:
                    sim1.compare_param(param=i,region=j,diffscatter=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,region=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffscatter.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['region']['pdfs']:
                try:
                    sim1.compare_param(param=i,region=j,pdfs=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,region=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'pdfs.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['region']['diffpdfs']:
                try:
                    sim1.compare_param(param=i,region=j,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['region']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,region=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffpdfs.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['region']['taylordiagram']:
                try:
                    dia = sim1.compare_param(param=i,region=j,taylordiagram=True,label=nml['files']['sim1label'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,addon=True)
                    if sim3 != 'none':
                        sim3.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,addon=True)
                    if sim4 != 'none':
                        sim4.compare_param(param=i,region=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,addon=True)
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close()
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
    for j in states:
        for i in params:
            print i,j
            if nml['state']['tseries']:
                try:
                    sim1.compare_param(param=i,state=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,state=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,state=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,state=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['state']['tseriesrmse']:
                try:
                    sim1.compare_param(param=i,state=j,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,state=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,state=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,state=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['state']['tseriesbias']:
                try:
                    sim1.compare_param(param=i,state=j,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,state=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,state=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,state=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_mb.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['state']['scatter']:
                try:
                    sim1.compare_param(param=i,state=j,scatter=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,state=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,state=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,state=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'scatter.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['state']['diffscatter']:
                try:
                    sim1.compare_param(param=i,state=j,diffscatter=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,state=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,state=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,state=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffscatter.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['state']['pdfs']:
                try:
                    sim1.compare_param(param=i,state=j,pdfs=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,state=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,state=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,state=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'pdfs.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['state']['diffpdfs']:
                try:
                    sim1.compare_param(param=i,state=j,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['state']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,state=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,state=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,state=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffpdfs.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['state']['taylordiagram']:
                try:
                    dia = sim1.compare_param(param=i,state=j,taylordiagram=True,label=nml['files']['sim1label'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,addon=True)
                    if sim3 != 'none':
                        sim3.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,addon=True)
                    if sim4 != 'none':
                        sim4.compare_param(param=i,state=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,addon=True)
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close()
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
    for j in citys:
        for i in params:
            print i,j
            if nml['city']['tseries']:
                try:
                    sim1.compare_param(param=i,city=j,timeseries=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,city=j,timeseries=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['city']['tseriesrmse']:
                try:
                    sim1.compare_param(param=i,city=j,timeseries_rmse=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,city=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,city=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,city=j,timeseries_rmse=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_rmse.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['city']['tseriesbias']:
                try:
                    sim1.compare_param(param=i,city=j,timeseries_mb=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,city=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,city=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,city=j,timeseries_mb=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'timeseries_mb.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['city']['scatter']:
                try:
                    sim1.compare_param(param=i,city=j,scatter=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,city=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,city=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,city=j,scatter=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'scatter.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['city']['diffscatter']:
                try:
                    sim1.compare_param(param=i,city=j,diffscatter=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,city=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,city=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,city=j,diffscatter=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffscatter.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['city']['pdfs']:
                try:
                    sim1.compare_param(param=i,city=j,pdfs=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,city=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,city=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,city=j,pdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'pdfs.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['city']['diffpdfs']:
                try:
                    sim1.compare_param(param=i,city=j,diffpdfs=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,city=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim2label'],footer=nml['city']['footers'])
                    if sim3 != 'none':
                        sim3.compare_param(param=i,city=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim3label'],footer=nml['city']['footers'])
                    if sim4 != 'none':
                        sim4.compare_param(param=i,city=j,diffpdfs=True,fig=plt.figure(1),label=nml['files']['sim4label'],footer=nml['city']['footers'])
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'diffpdfs.jpg',dpi=75)
                    plt.close()
                except:
                    pass
            if nml['city']['taylordiagram']:
                try:
                    dia = sim1compare_param(param=i,city=j,taylordiagram=True,label=nml['files']['sim1label'],footer=nml['city']['footers'])
                    if sim2 != 'none':
                        sim2.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim2label'],dia=dia,addon=True)
                    if sim3 != 'none':
                        sim3.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim3label'],dia=dia,addon=True)
                    if sim4 != 'none':
                        sim4.compare_param(param=i,city=j,taylordiagram=True,fig=plt.figure(1),label=nml['files']['sim4label'],dia=dia,addon=True)
                    plt.savefig(base +'_'+i.replace('.','')+'_'+j.replace(' ','')+'_'+'taylor.jpg',dpi=75)
                    plt.close()
                except:
                    pass

