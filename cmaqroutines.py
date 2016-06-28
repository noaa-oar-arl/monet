from numpy import array
import pandas as pd

def get_dust_pm25(ncfobj,time_index,layer):
    from numpy import zeros,array
    pm25vars = array(['ASO4J','ANO3J','ACLJ','ANH4J','ANAJ','ACAJ','AMGJ','AKJ','APOCJ','APNCOMJ','AFEJ','AALJ','ASIJ','ATIJ','AMNJ','AH2OJ','AOTHRJ'])
    pm25 = zeros(ncfobj.variables['ASO4J'][:][0,0,:,:].shape)
    for i in pm25vars:
        pm25 += ncfobj.variables[i][time_index,layer,:,:].squeeze()
    return pm25

def get_dust_total(ncfobj,time_index,layer):
    from numpy import zeros,array
    pmvars = array([ 'ASO4J', 'ASO4K', 'ANO3J', 'ANO3K', 'ACLJ', 'ACLK', 'ANH4J', 'ANAJ', 'ACAJ', 'AMGJ', 'AKJ', 'APOCJ', 'APNCOMJ', 'AECJ','AFEJ', 'AALJ', 'ASIJ', 'ATIJ', 'AMNJ', 'AH2OJ', 'AOTHRJ', 'ASOIL'])
    pm = zeros(ncfobj.variables['ASO4J'][:][0,0,:,:].shape)
    for i in pmvars:
        pm += ncfobj.variables[i][time_index,layer,:,:].squeeze()
    return pm

def calc_cmaq_aod(aerovis,metcro3d):
    from numpy import zeros,arange,diff,sum
    mie_ext = aerovis.variables['EXT_Mie'][:]-0.01
    s = mie_ext.shape
    zf = metcro3d.variables['ZF'][0:s[0],:,:,:]/1000.
    print zf.max(),mie_ext.max(),zf.min(),mie_ext.min()
    AOD = diff(zf,axis=1) * mie_ext[:,1:,:,:]
    print diff(zf,axis=1).shape,mie_ext[:,1:,:,:].shape,sum(AOD,axis=1).min()



#     AOD= zf[:,0,:,:].squeeze()/1000. * (mie_ext[:,0,:,:] - 0.01)
#     tempval = zeros(AOD.shape)
#     for i in arange(1,s[1]):
#         tempval = (zf[:,i,:,:] - zf[:,i-1,:,:])/1000. * (mie_ext[:,i,:,:] - 0.01)
#         print tempval.max(),tempval.min(),i
#         AOD = AOD + tempval
# #
#        tempval = dzf.squeeze()/1000. * (mie_ext[:,1:,:,:] - 0.01)

#    return AOD.sum(axis=1)


def aeronet_cmaq_aod(aeronet):
    #this assumes a pandas dataframe with the data

    #based on Roy et al. 2007; http://onlinelibrary.wiley.com/doi/10.1029/2006JD008085/full
    #ln(AOD) = a0 + a1 ln(gamma) + a2 (ln(gamma)) **2

    #this will curve fit for a given gamma 1 and gamma 2 and then output the result
    from scipy.optimize import curve_fit
    from numpy import array,log,ones,isfinite,append,shape,isnan
    keys = [u'AOT_1640', u'AOT_1020', u'AOT_870',
       u'AOT_675', u'AOT_667', u'AOT_555', u'AOT_551', u'AOT_532', u'AOT_531',
       u'AOT_500', u'AOT_490', u'AOT_443', u'AOT_440', u'AOT_412', u'AOT_380',
       u'AOT_340']

    first = True
    for i in keys:
        if 'AOT_' in i:
            if first:
                real = pd.notnull(aeronet[i])
                print i,log(aeronet[i].values[real.values]).max()
                if isnan(log(aeronet[i].values[real.values]).max()):
                    print 'in the loop break'
                    continue
                y = log(aeronet[i].values[real.values])
                x = ones(aeronet[i].values[real.values].shape[0]) * float(i[4:])*1.E-9
                print 'FIRST VALUE SET', i
                first=False
            else:
                real = pd.notnull(aeronet[i])
                if real.values.max() == False:
                    continue
                print i,log(aeronet[i].values[real.values]).max()
                append(y,log(aeronet[i].values[real.values]))
                append(x,ones(aeronet[i].values[real.values].shape[0]) * float(i[4:]))*1.E-9
    y = array(y).flatten()
    x = array(x).flatten()
    print x.shape,x.max()
    print y.shape,y.max()
    popt,pcov = curve_fit(func,x,y)
    return popt

def func(x,a,b,c):
    from numpy import log
    return a + b * log(x) + c * log(x)**2

def make_improve_site_plots(improve,cmaqobj):
    import matplotlib.pyplot as plt
    plt.style.use('seaborn-deep')
    #first make the AL plot
    f = plt.figure(figsize(12,6))
    tindex = get_overlap_index(cmaqobj)


def get_overlap_index(cmaqobj):
    from numpy import array,unique
    vals = array(cmaqobj.variables['TFLAG'][:,0,0],dtype='float')*1000 + cmaqobj.variables['TFLAG'][:,0,1]
    a,b = unique(vals,return_index=True)
    return b.sort()


def add_improve_latlon_to_pandas_sites(data,latlons):
    from numpy import where
    data['Lon'],data['Lat'] = 0.,0.
    for i in latlons.SITEID.values:
        condition = (data.site_code.values == i)
        data.loc[condition,'Lon'] = latlons.Lon.values[where(latlons.SITEID.values == i)[0]]
        data.loc[condition,'Lat'] = latlons.Lat.values[where(latlons.SITEID.values == i)[0]]
    return data

def make_spatial_plot(cmaqobj,gridobj,date,m,vmin=None,vmax=None,dpi=None,savename=''):
    import matplotlib.pyplot as plt
    from matplotlib.pylab import cm
    from mpl_toolkits.basemap import Basemap
    from netCDF4 import Dataset as ncf
    from numpy.ma import masked_less
    from numpy import linspace,where
    from datetime import datetime
    from six.moves import cPickle as pickle

    fig = plt.figure(figsize=(12,6),frameon=False)
    #get grid parameters and lat lon
    lat = gridobj.variables['LAT'][:].squeeze()
    lon = gridobj.variables['LON'][:].squeeze()

    #define map and draw boundries
    m.drawstates();m.drawcoastlines();m.drawcountries()
    x,y = m(lon,lat)
    plt.axis('off')

    m.pcolormesh(x,y,cmaqobj,vmin=vmin,vmax=vmax,cmap='viridis')
    titstring = date.strftime('%B %d %Y %H')
    plt.title(titstring)

    c = plt.colorbar()


    plt.tight_layout()
    if savename!='':
        plt.savefig(savename+date.strftime('%Y%m%d_%H.jpg'),dpi=dpi)
        plt.close()
    return m,c

# def add_improve_to_spatial(m,improve):

#     x,y = m(improve('lat'

def aqs_pm_spatial_scatter(aqs,m,date,savename='',vmin=1,vmax=10):
    import matplotlib.pyplot as plt
    new = aqs[aqs.datetime == date]
    x,y = m(new.Longitude.values,new.Latitude.values)
    plt.scatter(x,y,c=new['Sample Measurement'].values,vmin=vmin,vmax=vmax,cmap='viridis')
    if savename != '':
        plt.savefig(savename + date + '.jpg',dpi=100.)
        plt.close()

def improve_spatial_scatter(improve,m,date,param,vmn=None,vmx=None,cmap='viridis',savename=''):
    import matplotlib.pyplot as plt
    new = improve[improve.datetime == date]
    x,y = m(new.Lon.values,new.Lat.values)
    plt.scatter(x,y,c=new[param].values,vmin=vmin,vmax=vmax,cmap=cmap)
    if savename != '':
        plt.savefig(savename + date + '.jpg',dpi=100.)
    plt.close()

def write_monthly_aqs_data_latlon_sitename(ifile='',ofile=''):
    import pandas as pd
    import datetime
    from numpy import arange

    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M')
    f =  pd.read_csv(ifile,parse_dates={'datetime':['Date GMT', 'Time GMT'],'datetime_local':["Date Local","Time Local"]},date_parser=dateparse)
    for i in arange(1,12):
        cols = ['datetime','datetime_local','Latitude', 'Longitude','Sample Measurement','Site Num','Units of Measure','Parameter Name',"State Code","County Code"]
        temp = f[(f['datetime'] >= datetime.datetime(2014,i , 1, 0, 0)) & (f['datetime'] < datetime.datetime(2014, i+1, 1, 0, 0))][cols]
        month = datetime.date(1990,i,1).strftime('%B')
        temp['SCS'] = array(temp['State Code'].values * 1.E7 + temp['County Code'].values * 1.E4 + temp['Site Num'].values,dtype='int32')
        temp.to_hdf(ofile + month + '.hdf','w',format='table')
    cols = ['datetime','datetime_local','Latitude', 'Longitude','Sample Measurement','Site Num','Units of Measure','Parameter Name',"State Code","County Code"]
    temp = f[(f['datetime'] >= datetime.datetime(2014,12, 1, 0, 0)) & (f['datetime'] <= datetime.datetime(2014, 12, 31, 23, 0))][cols]
    month = datetime.date(1990,12,1).strftime('%B')
    temp['SCS'] = array(temp['State Code'].values * 1.E7 + temp['County Code'].values * 1.E4 + temp['Site Num'].values,dtype='int32')
    temp.to_hdf(ofile + month + '.hdf','df',format='table')

def doy2date(year=2007, doy=0):
    import datetime
    dt = datetime.datetime(year, 1, 1)
    delta = datetime.timedelta(days=doy - 1)
    return dt + delta

def doy2dates(year=2007,doy=[]):
    from numpy import array
    dates = []
    for i in doy:
        dates.append(doy2date(year=year,doy=i))
    return array(dates)

def tzutc(lon,lat,dates):
    import pytz
    from tzwhere import tzwhere

    tz = tzwhere.tzwhere(forceTZ=True,shapely=True)
    a = dates.astype('M8[s]').astype('O')
    offset = []
    for i,j,d in zip(lon,lat,a):
        l = tz.tzNameAt(j, i,forceTZ=True)
        timezone = pytz.timezone(l)
        n = d.replace(tzinfo=pytz.UTC)
        r = d.replace(tzinfo=timezone)
        rdst = timezone.normalize(r)
        offset.append((rdst.utcoffset()).total_seconds()//3600)
    return array(offset)

def plot_timeseries(dataframe,siteave=True,ylabel='PM10 Concentration',savename='',title='State title'):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import gc
    #format plot stuff
    import seaborn as sns
    sns.set_style('whitegrid')

    if siteave:
        domainave = dataframe.groupby('datetime').mean()
        plt.plot(domainave.index.values,domainave['Sample Measurement'].values,'k',label='OBS')
        plt.plot(domainave.index.values,domainave['cmaq'].values,label='CMAQ')
        plt.ylabel(ylabel)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        plt.xticks(rotation=30)
        plt.title( title +' ' + str(dataframe['SCS'].nunique()) + ' Sites')
        plt.tight_layout()
        plt.legend(loc='best')
        if savename != '':
            plt.savefig(savename+'_average.jpg',dpi=100)
            plt.close()
    else:
        plt.plot(dataframe.index,dataframe['Sample Measurement'],'k',label='OBS')
        plt.plot(dataframe.index,dataframe['cmaq'],label='CMAQ')
        plt.ylabel(ylabel)
        plt.xticks(rotation=30)
        plt.title('Site ' + str(dataframe['SCS'].unique()))
        plt.tight_layout()
        plt.legend(loc='best')
        if savename != '':
            plt.savefig(savename+ str(dataframe['SCS'].unique()[0])+ '.jpg',dpi=100)
            plt.close()

    gc.collect()


def plot_timeseries_daily(dataframe,siteave=True,ylabel='PM10 Concentration',savename='',title='State title'):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import gc
    #format plot stuff
    import seaborn as sns
    sns.set_style('whitegrid')
    
    if siteave:
        domainave = dataframe.groupby('datetime_local').mean()
        plt.plot(domainave.index.values,domainave['Arithmetic Mean'].values,'k',label='OBS')
        plt.plot(domainave.index.values,domainave['cmaq'].values,label='CMAQ')
        plt.ylabel(ylabel)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        plt.xticks(rotation=30)
        plt.title( title +' ' + str(dataframe['SCS'].nunique()) + ' Sites')
        plt.tight_layout()
        plt.legend(loc='best')
        if savename != '':
            plt.savefig(savename+'_average.jpg',dpi=100)
            plt.close()
    else:
        plt.plot(dataframe.index,dataframe['Arithmetic Mean'],'k',label='OBS')
        plt.plot(dataframe.index,dataframe['cmaq'],label='CMAQ')
        plt.ylabel(ylabel)
        plt.xticks(rotation=30)
        plt.title('Site ' + str(dataframe['SCS'].unique()))
        plt.tight_layout()
        plt.legend(loc='best')
        if savename != '':
            plt.savefig(savename+ str(dataframe['SCS'].unique()[0])+ '.jpg',dpi=100)
            plt.close()
    gc.collect()
            
def plot_allsites_timeseries(dataframe,ylabel='PM10 Concentration',savename=''):
    sites = dataframe.SCS.unique()
    dataframe.index = dataframe.datetime
    for i in sites:
        sitedf = dataframe[dataframe['SCS'] == i]
        plot_timeseries(sitedf,siteave=False,ylabel=ylabel,savename=savename)
    plot_timeseries(dataframe,siteave=True,ylabel=ylabel,savename=savename)


def kdtree_interp(gridobj,cmaqvar2d,lons,lats):
    from pyresample import geometry,image, kd_tree
    from numpy import meshgrid
    lat = gridobj.variables['LAT'][:][0,0,:,:].squeeze()
    lon = gridobj.variables['LON'][:][0,0,:,:].squeeze()
    first_grid_def = geometry.GridDefinition(lons=lon, lats=lat)
    xx,yy = meshgrid(lons,lats)
    second_grid_def = geometry.GridDefinition(lons=xx, lats=yy)
    result = kd_tree.resample_nearest(first_grid_def, test,second_grid_def, radius_of_influence=grid.XCELL*grid.YCELL, fill_value=None,nprocs=2)
    return result.diag()
