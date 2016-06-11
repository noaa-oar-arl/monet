from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from numpy import array, zeros, arange

import cmaqroutines as cr
import mystats as mstats
from tools import search_listinlist


def load_single_cmaq_run(fname='aqm.nuopc.t12z.20140511.conc.ncf'):
    from netCDF4 import Dataset
    return Dataset(fname, 'r')


def load_multi_cmaq_runs(fname='aqm.nuopc.t12z.2014051*'):
    from netCDF4 import MFDataset
    return MFDataset(fname)


def get_cmaq_dates(cmaqobj):
    tflag1 = array(cmaqobj.variables['TFLAG'][:, 0, 0], dtype='|S7')
    tflag2 = array(cmaqobj.variables['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
    date = []
    for i, j in zip(tflag1, tflag2):
        date.append(datetime.strptime(i + j, '%Y%j%H'))

    return array(date)


def load_aqs_pm10_data(dates):
    month = dates[0].strftime('%B.hdf')
    year = dates[0].strftime('%Y_')
    aqs = pd.read_hdf('/naqfc/noscrub/Barry.Baker/AQS_DATA/AQS_HOURLY_PM_10_' + year + month)
    con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
    aqs = aqs[con]
    aqs.index = arange(aqs.index.shape[0])
    return aqs


def load_aqs_pm25_data(dates):
    year = dates[0].strftime('%Y_')
    aqs = pd.read_hdf('/data/aqf/barryb/AQS_DATA/AQS_HOURLY_PM_25_' + year + 'May.hdf')
    con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
    aqs = aqs[con]
    aqs.index = arange(aqs.index.shape[0])
    return aqs


def load_daily_aqs_pm25_data(dates):
    year = dates[0].strftime('%Y_')
    aqs = pd.read_hdf('AQS_DATA/DAILY_AQS_PM25_FRM_FEM_88101_2015.hdf')
    con = (aqs.datetime_local >= dates[0]) & (aqs.datetime_local <= dates[-1])
    aqs = aqs[con]
    aqs.index = arange(aqs.index.shape[0])
    return aqs


def load_aqs_o3_data(dates):
    month = dates[0].strftime('%B.hdf')
    year = dates[0].strftime('%Y_')
    aqs = pd.read_hdf('/naqfc/noscrub/Barry.Baker/AQS_DATA/AQS_HOURLY_OZONE_' + year + month)
    con = (aqs.datetime >= dates[0]) & (aqs.datetime <= dates[-1])
    aqs = aqs[con]
    aqs.index = arange(aqs.index.shape[0])
    return aqs


def get_surface_dust(cmaqobj):
    dustvars = array(
            ['ASO4J', 'ASO4K', 'ANO3J', 'ANO3K', 'ACLJ', 'ACLK', 'ANH4J', 'ANAJ', 'ACAJ', 'AMGJ', 'AKJ', 'APOCJ',
             'APNCOMJ', 'AECJ', 'AFEJ', 'AALJ', 'ASIJ', 'ATIJ', 'AMNJ', 'AH2OJ', 'AOTHRJ', 'ASOIL'])
    keys = array(cmaqobj.variables.keys())
    cmaqvars, temp = search_listinlist(keys, dustvars)
    pm = zeros(cmaqobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
    for i in cmaqvars[1:]:
        pm += cmaqobj.variables[keys[i]][:, 0, :, :].squeeze()
    return pm


def get_surface_pm25_dust(cmaqobj):
    pm25vars = array(
            ['ASO4J', 'ANO3J', 'ACLJ', 'ANH4J', 'ANAJ', 'ACAJ', 'AMGJ', 'AKJ', 'APOCJ', 'APNCOMJ', 'AECJ', 'AFEJ',
             'AALJ', 'ASIJ', 'ATIJ', 'AMNJ', 'AH2OJ', 'AOTHRJ'])
    keys = array(cmaqobj.variables.keys())
    cmaqvars, temp = search_listinlist(keys, pm25vars)
    pm = zeros(cmaqobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
    for i in cmaqvars[1:]:
        pm += cmaqobj.variables[keys[i]][:, 0, :, :].squeeze()
    return pm


def get_surface_pm25(cmaqobj):
    # THIS NEEDS EDITED FOR FULL LIST OF VARS
    pm25vars = array(['AALJ', 'AALK1J', 'AALK2J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J',
                      'ACAJ', 'ACLI', 'ACLJ', 'AECI', 'AECJ',
                      'AFEJ', 'AH2OI', 'AH2OJ', 'AISO1J', 'AISO2J', 'AISO3J', 'AKJ', 'AMGJ',
                      'AMNJ', 'ANAI', 'ANAJ', 'ANH4I', 'ANH4J', 'ANO3I',
                      'ANO3J', 'AOLGAJ', 'AOLGBJ', 'AORGCJ', 'AOTHRI',
                      'AOTHRJ', 'APAH1J', 'APAH2J', 'APAH3J', 'APNCOMI', 'APNCOMJ',
                      'APOCI', 'APOCJ', 'ASEACAT', 'ASIJ', 'ASO4I', 'ASO4J', 'ASQTJ', 'ATIJ', 'ATOL1J', 'ATOL2J',
                      'ATOL3J', 'ATRP1J', 'ATRP2J', 'AXYL1J', 'AXYL2J', 'AXYL3J', 'A25I', 'A25J', 'AORGAI', 'AORGAJ',
                      'AORGPAI', 'AORGPAJ', 'AORGBI', 'AORGBJ'])
    keys = array(cmaqobj.variables.keys())
    cmaqvars, temp = search_listinlist(keys, pm25vars)
    pm = zeros(cmaqobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
    for i in cmaqvars[1:]:
        pm += cmaqobj.variables[keys[i]][:, 0, :, :].squeeze()
    return pm


def get_surface_pm(cmaqobj):
    pm25vars = array(['AALJ', 'AALK1J', 'AALK2J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J',
                      'ACAJ', 'ACLI', 'ACLJ', 'ACLK', 'ACORS', 'AECI', 'AECJ',
                      'AFEJ', 'AH2OI', 'AH2OJ', 'AH2OK', 'AISO1J', 'AISO2J', 'AISO3J', 'AKJ', 'AMGJ',
                      'AMNJ', 'ANAI', 'ANAJ', 'ANH4I', 'ANH4J', 'ANH4K', 'ANO3I',
                      'ANO3J', 'ANO3K', 'AOLGAJ', 'AOLGBJ', 'AORGCJ', 'AOTHRI',
                      'AOTHRJ', 'APAH1J', 'APAH2J', 'APAH3J', 'APNCOMI', 'APNCOMJ',
                      'APOCI', 'APOCJ', 'ASEACAT', 'ASIJ', 'ASO4I', 'ASO4J',
                      'ASO4K', 'ASOIL', 'ASQTJ', 'ATIJ', 'ATOL1J', 'ATOL2J',
                      'ATOL3J', 'ATRP1J', 'ATRP2J', 'AXYL1J', 'AXYL2J', 'AXYL3J', 'A25I', 'A25J', 'AORGAI', 'AORGAJ',
                      'AORGPAI', 'AORGPAJ', 'AORGBI', 'AORGBJ'])
    keys = array(cmaqobj.variables.keys())
    cmaqvars, temp = search_listinlist(keys, pm25vars)
    pm = zeros(cmaqobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
    for i in cmaqvars[1:]:
        pm += cmaqobj.variables[keys[i]][:, 0, :, :].squeeze()
    return pm


def get_surface_pm10(cmaqobj):
    # THIS NEEDS EDITED FOR FULL LIST OF VARS
    pm25vars = array(['ACORS', 'ASOIL', 'ASEACAT', 'ACLK', 'ASO4K', 'ANH4K', 'ANO3K', 'AH2OK'])
    keys = array(cmaqobj.variables.keys())
    cmaqvars, temp = search_listinlist(keys, pm25vars)
    pm = zeros(cmaqobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
    for i in cmaqvars[1:]:
        pm += cmaqobj.variables[keys[i]][:, 0, :, :].squeeze()
    return pm


def get_surface_pm10_dust(cmaqobj):
    # THIS NEEDS EDITED FOR FULL LIST OF VARS
    pmvars = array(['ASO4K', 'ANO3K', 'ACLK', 'ASOIL'])
    keys = array(cmaqobj.variables.keys())
    cmaqvars, temp = search_listinlist(keys, pmvars)
    pm = zeros(cmaqobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
    for i in cmaqvars[1:]:
        pm += cmaqobj.variables[keys[i]][:, 0, :, :].squeeze()
    return pm


def get_surface_cmaqvar(cmaqobj, param='O3'):
    param = param.upper()
    if param == 'PM25':
        var = get_surface_pm25(cmaqobj)
    elif param == 'PM10':
        var = get_surface_pm10(cmaqobj)
    elif param == 'PM25_DUST':
        var = get_surface_pm25_dust(cmaqobj)
    elif param == 'PM10_DUST':
        var = get_surface_pm10_dust(cmaqobj)
    elif param == 'PM_TOT':
        var = get_surface_pm(cmaqobj)
    else:
        var = cmaqobj.variables[param][:, 0, :, :].squeeze()
    return var


def load_conus_basemap(path):
    from six.moves import cPickle as pickle
    return pickle.load(open(path + '/basemap-cmaq_conus.p', 'rb'))


def interp_to_aqs_sites(cmaqvar, aqs, gridobj, dates):
    from scipy.interpolate import griddata
    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()

    con = (aqs.datetime == dates[0]) & (aqs.Latitude.values > lat.min()) & (aqs.Latitude.values < lat.max()) & (
    aqs.Longitude.values > lon.min()) & (aqs.Longitude.values < lon.max())
    new = aqs[con]
    cmaq_val = pd.DataFrame(griddata((lon.flatten(), lat.flatten()), cmaqvar[0, :, :].flatten(),
                                     (new.Longitude.values, new.Latitude.values), method='nearest'), columns=['cmaq'],
                            index=new.index)
    new = new.join(cmaq_val)
    for i, j in enumerate(dates[1:]):
        con = (aqs.datetime == j) & (aqs.Latitude.values > lat.min()) & (aqs.Latitude.values < lat.max()) & (
        aqs.Longitude.values > lon.min()) & (aqs.Longitude.values < lon.max())
        newt = aqs[con]
        cmaq_val = pd.DataFrame(griddata((lon.flatten(), lat.flatten()), cmaqvar[i, :, :].flatten(),
                                         (newt.Longitude.values, newt.Latitude.values), method='nearest'),
                                columns=['cmaq'], index=newt.index)
        newt = newt.join(cmaq_val)
        new = new.append(newt)
    return new


def interp_to_aqs_sites_daily_pm25(cmaqvar, aqs, gridobj, dates):
    from scipy.interpolate import griddata
    from datetime import timedelta, datetime
    from numpy import unique
    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()
    con = (aqs.Latitude.values > lat.min()) & (aqs.Latitude.values < lat.max()) & (aqs.Longitude.values > lon.min()) & (
    aqs.Longitude.values < lon.max())
    #    aqs = aqs[aqs['SCS'] == 720610005]
    aqsn = aqs[con]
    dt = []
    for i in aqsn.utcoffset.values:
        dt.append(timedelta(hours=i))
    aqsn['utc timedelta'] = array(dt)
    aqsnlat = aqsn.Latitude.values
    scs, index = unique(aqsn.SCS.values, return_index=True)
    ln = aqsn.Longitude.values[index]
    ll = aqsn.Latitude.values[index]
    utctimedelta = aqsn['utc timedelta'].values[index]
    print scs.shape, ln.shape
    cmaq = pd.DataFrame(
        griddata((lon.flatten(), lat.flatten()), cmaqvar[0, :, :].flatten(), (ln, ll), method='nearest'),
        columns=['cmaq'])
    cmaq['SCS'], cmaq['utc timedelta'] = scs, utctimedelta
    # cmaq['SCS'] = scs
    arr = array([datetime(2000, 1, 1) + timedelta(hours=utctimedelta[k]) for k in range(ll.shape[0])])
    arr[:] = dates[0] + timedelta(hours=k)
    print 'Interpolating values to AQS Surface Sites for PM25 24H, Date : ', dates[0].strftime('%B %d %Y   %H utc')
    cmaq['datetime_local'] = arr  # + utctimedelta
    for i, j in enumerate(dates[1:]):
        print 'Interpolating values to AQS Surface Sites for PM25 24H, Date : ', j.strftime('%B %d %Y   %H utc')
        cmaq2 = pd.DataFrame(
            griddata((lon.flatten(), lat.flatten()), cmaqvar[i, :, :].flatten(), (ln, ll), method='nearest'),
            columns=['cmaq'])
        cmaq2['SCS'], cmaq2['utc timedelta'] = scs, utctimedelta
        # cmaq2['SCS'] = scs
        arr = array([datetime(2000, 1, 1) + timedelta(hours=k) for k in range(ll.shape[0])])
        arr[:] = j
        cmaq2.index = arr
        cmaq = pd.concat([cmaq, cmaq2])

    scs, index = unique(aqsn.SCS.values, return_index=True)
    i = scs[0]
    cmaqr = cmaq[cmaq.SCS == i]
    cmaqr = cmaqr.resample('24H').mean()
    cmaqr['datetime_local'] = cmaqr.index
    aqsnr = aqsn[aqsn.SCS == i]
    new = pd.merge(cmaqr, aqsnr, on='datetime_local', how='right')
    for i in scs[1:]:
        cmaqr = cmaq[cmaq.SCS == i]
        cmaqr = cmaqr.resample('24H').mean()
        cmaqr['datetime_local'] = cmaqr.index
        aqsnr = aqsn[aqsn.SCS == i]
        new = new.append(pd.merge(cmaqr, aqsnr, on='datetime_local'))

    new = new.reset_index()
    new['SCS'] = new['SCS_x']
    new = new[['cmaq', 'SCS', 'datetime_local', 'State Code', 'Latitude', 'Longitude', 'Observation Percent',
               'Arithmetic Mean', '1st Max Value', '1st Max Hour', 'AQI', 'State Name', 'Local Site Name',
               'County Code', 'Site Num', 'utcoffset', 'datetime']]

    return new


def calc_stats(pandasobj):
    mb = mstats.MB(pandasobj['Sample Measurement'].values, pandasobj['cmaq'].values)  # mean bias
    mdnb = mstats.AC(pandasobj['Sample Measurement'].values, pandasobj['cmaq'].values)  # median bias
    fe = mstats.FE(pandasobj['Sample Measurement'].values, pandasobj['cmaq'].values)  # fractional error
    r2 = mstats.R2(pandasobj['Sample Measurement'].values, pandasobj['cmaq'].values)  # pearsonr ** 2
    d1 = mstats.d1(pandasobj['Sample Measurement'].values, pandasobj['cmaq'].values)  # modifed index of agreement
    e1 = mstats.E1(pandasobj['Sample Measurement'].values,
                   pandasobj['cmaq'].values)  # Modified Coefficient of Efficiency, E1
    ioa = mstats.IOA(pandasobj['Sample Measurement'].values, pandasobj['cmaq'].values)  # Index of Agreement
    rmse = mstats.RMSE(pandasobj['Sample Measurement'].values, pandasobj['cmaq'].values)
    return mb, mdnb, fe, r2, d1, e1, ioa, rmse


def calc_stats_daily(pandasobj):
    mb = mstats.MB(pandasobj['Arithmetic Mean'].values, pandasobj['cmaq'].values)  # mean bias
    mdnb = mstats.AC(pandasobj['Arithmetic Mean'].values, pandasobj['cmaq'].values)  # median bias
    fe = mstats.FE(pandasobj['Arithmetic Mean'].values, pandasobj['cmaq'].values)  # fractional error
    r2 = mstats.R2(pandasobj['Arithmetic Mean'].values, pandasobj['cmaq'].values)  # pearsonr ** 2
    d1 = mstats.d1(pandasobj['Arithmetic Mean'].values, pandasobj['cmaq'].values)  # modifed index of agreement
    e1 = mstats.E1(pandasobj['Arithmetic Mean'].values,
                   pandasobj['cmaq'].values)  # Modified Coefficient of Efficiency, E1
    ioa = mstats.IOA(pandasobj['Arithmetic Mean'].values, pandasobj['cmaq'].values)  # Index of Agreement
    rmse = mstats.RMSE(pandasobj['Arithmetic Mean'].values, pandasobj['cmaq'].values)
    return mb, mdnb, fe, r2, d1, e1, ioa, rmse


def aqs_state_comparison(df, scatter=False, time=False, stat=True):
    sns.set_style('whitegrid')
    first = True
    for k, i in enumerate(df['State Name'].unique()):
        temp = df[df['State Name'] == i]

        # calculate statistics
        mnb, mdnb, fe, r2, d1, e1, ioa, rmse = calc_stats(temp)

        # put the stats in a dfframe
        if first:
            s1 = pd.Series([mnb], index=[k], name='Normalized Mean Bias')
            s2 = pd.Series([fe], index=[k], name='Fractional Error')
            s3 = pd.Series([r2], index=[k], name='R**2')
            s4 = pd.Series([d1], index=[k], name='Modified Index of Agreement; d1')
            s5 = pd.Series([e1], index=[k], name='E1')
            s6 = pd.Series([ioa], index=[k], name='Index of Agreement')
            s7 = pd.Series([i], index=[k], name='State Name')
            s8 = pd.Series([mdnb], index=[k], name='Anomaly Correlation')
            s9 = pd.Series([rmse], index=[k], name='RMSE')
            first = False
        else:
            s1 = s1.append(pd.Series([mnb], index=[k], name='Normalized Mean Bias'))
            s2 = s2.append(pd.Series([fe], index=[k], name='Fractional Error'))
            s3 = s3.append(pd.Series([r2], index=[k], name='R**2'))
            s4 = s4.append(pd.Series([d1], index=[k], name='Modified Index of Agreement; d1'))
            s5 = s5.append(pd.Series([e1], index=[k], name='E1'))
            s6 = s6.append(pd.Series([ioa], index=[k], name='Index of Agreement'))
            s7 = s7.append(pd.Series([i], index=[k], name='State Name'))
            s8 = s8.append(pd.Series([mdnb], index=[k], name='Anomaly Correlation'))
            s9 = s9.append(pd.Series([rmse], index=[k], name='RMSE'))
            # make scatterplot
        if scatter:
            sns.jointplot(x='Sample Measurement', y='cmaq', data=temp, kind='reg')
            plt.xlabel('Obs ' + temp['Units of Measure'].unique())
            plt.ylabel('CMAQ ' + temp['Units of Measure'].unique())
            plt.title('State Code ' + str(i) + ' PM10 Comparison : ' + str(temp.count()['index']) + 'Sites')
            plt.tight_layout()
        if time:
            plt.figure(figsize=(12, 6))
            cr.plot_timeseries(temp, siteave=True)

    stats = pd.concat([s1, s2, s3, s4, s5, s6, s7, s8, s9], axis=1).reset_index()
    if stat:
        f = plt.figure(figsize=(14, 10))
        ax1 = f.add_subplot(221)
        ax2 = f.add_subplot(222)
        ax3 = f.add_subplot(223, sharex=ax1)
        ax4 = f.add_subplot(224, sharex=ax2)

        sns.barplot(x='State Name', y='Normalized Mean Bias', data=stats, ax=ax1)
        ax1.set_ylabel('Normalized Mean Bias');
        ax1.set_xlabel('')
        plt.setp(ax2.get_xticklabels(), visible=False)
        plt.setp(ax1.get_xticklabels(), visible=False)
        sns.barplot(x='State Name', y='Index of Agreement', data=stats, ax=ax2)
        ax2.set_ylabel('Index of Agreement');
        ax2.set_xlabel('')
        sns.barplot(x='State', y='Modified Index of Agreement; d1', data=stats, ax=ax3)
        ax3.set_ylabel('Modified Index of Agreement');
        ax3.set_xlabel('')
        sns.barplot(x='State', y='E1', data=stats, ax=ax4)
        ax4.set_ylabel('E1');
        ax4.set_xlabel('')
        plt.tight_layout()

        f = plt.figure(figsize=(14, 10))
        ax5 = f.add_subplot(221)
        ax6 = f.add_subplot(222)
        ax7 = f.add_subplot(223, sharex=ax5)
        ax8 = f.add_subplot(224, sharex=ax6)
        plt.setp(ax6.get_xticklabels(), visible=False)
        plt.setp(ax5.get_xticklabels(), visible=False)
        sns.barplot(x='State Name', y='R**2', data=stats, ax=ax5)
        ax5.set_ylabel('R**2');
        ax5.set_xlabel('')
        sns.barplot(x='State Name', y='Fractional Error', data=stats, ax=ax6)
        ax6.set_ylabel('Fractional Error');
        ax6.set_xlabel('')
        sns.barplot(x='State Name', y='RMSE', data=stats, ax=ax7)
        ax7.set_ylabel('RMSE');
        ax7.set_xlabel('')
        sns.barplot(x='State Name', y='Anomaly Correlation', data=stats, ax=ax8)
        ax8.set_ylabel('Anomaly Correlation');
        ax8.set_xlabel('')
        plt.tight_layout()
    return stats


def aqs_state_comparison_24h_pm25(df, scatter=False, time=False, stat=True):
    sns.set_style('whitegrid')
    first = True
    for k, i in enumerate(df['State Name'].unique()):
        temp = df[df['State Name'] == i]

        # calculate statistics
        mnb, mdnb, fe, r2, d1, e1, ioa, rmse = calc_stats_daily(temp)

        # put the stats in a dfframe
        if first:
            s1 = pd.Series([mnb], index=[k], name='Normalized Mean Bias')
            s2 = pd.Series([fe], index=[k], name='Fractional Error')
            s3 = pd.Series([r2], index=[k], name='R**2')
            s4 = pd.Series([d1], index=[k], name='Modified Index of Agreement; d1')
            s5 = pd.Series([e1], index=[k], name='E1')
            s6 = pd.Series([ioa], index=[k], name='Index of Agreement')
            s7 = pd.Series([i], index=[k], name='State Name')
            s8 = pd.Series([mdnb], index=[k], name='Anomaly Correlation')
            s9 = pd.Series([rmse], index=[k], name='RMSE')
            first = False
        else:
            s1 = s1.append(pd.Series([mnb], index=[k], name='Normalized Mean Bias'))
            s2 = s2.append(pd.Series([fe], index=[k], name='Fractional Error'))
            s3 = s3.append(pd.Series([r2], index=[k], name='R**2'))
            s4 = s4.append(pd.Series([d1], index=[k], name='Modified Index of Agreement; d1'))
            s5 = s5.append(pd.Series([e1], index=[k], name='E1'))
            s6 = s6.append(pd.Series([ioa], index=[k], name='Index of Agreement'))
            s7 = s7.append(pd.Series([i], index=[k], name='State Name'))
            s8 = s8.append(pd.Series([mdnb], index=[k], name='Anomaly Correlation'))
            s9 = s9.append(pd.Series([rmse], index=[k], name='RMSE'))
        # make scatterplot
        if scatter:
            sns.jointplot(x='Arithmetic Mean', y='cmaq', data=temp, kind='reg')
            plt.xlabel('Obs ' + temp['Units of Measure'].unique())
            plt.ylabel('CMAQ ' + temp['Units of Measure'].unique())
            plt.title('State Name ' + str(i) + ' PM10 Comparison : ' + str(temp.count()['index']) + 'Sites')
            plt.tight_layout()
        if time:
            plt.figure(figsize=(12, 6))
            cr.plot_timeseries(temp, siteave=True)

    stats = pd.concat([s1, s2, s3, s4, s5, s6, s7, s8, s9], axis=1).reset_index()
    if stat:
        f = plt.figure(figsize=(18, 10))
        ax1 = f.add_subplot(221)
        ax2 = f.add_subplot(222)
        ax3 = f.add_subplot(223, sharex=ax1)
        ax4 = f.add_subplot(224, sharex=ax2)

        sns.barplot(x='State Name', y='Normalized Mean Bias', data=stats, ax=ax1, color='steelblue')
        ax1.set_ylabel('Normalized Mean Bias');
        ax1.set_xlabel('')
        plt.setp(ax2.get_xticklabels(), visible=False)
        plt.setp(ax1.get_xticklabels(), visible=False)
        sns.barplot(x='State Name', y='Index of Agreement', data=stats, ax=ax2, color='steelblue')
        ax2.set_ylabel('Index of Agreement');
        ax2.set_xlabel('')
        sns.barplot(x='State Name', y='Modified Index of Agreement; d1', data=stats, ax=ax3, color='steelblue')
        ax3.set_ylabel('Modified Index of Agreement');
        ax3.set_xlabel('')
        sns.barplot(x='State Name', y='E1', data=stats, ax=ax4, color='steelblue')
        ax4.set_ylabel('E1');
        ax4.set_xlabel('')
        ax3.set_xticklabels(ax3.xaxis.get_majorticklabels(), rotation=87)
        ax4.set_xticklabels(ax4.xaxis.get_majorticklabels(), rotation=87)
        plt.tight_layout()

        f = plt.figure(figsize=(118, 10))
        ax5 = f.add_subplot(221)
        ax6 = f.add_subplot(222)
        ax7 = f.add_subplot(223, sharex=ax5)
        ax8 = f.add_subplot(224, sharex=ax6)
        plt.setp(ax6.get_xticklabels(), visible=False)
        plt.setp(ax5.get_xticklabels(), visible=False)
        sns.barplot(x='State Name', y='R**2', data=stats, ax=ax5, color='steelblue')
        ax5.set_ylabel('R**2');
        ax5.set_xlabel('')
        sns.barplot(x='State Name', y='Fractional Error', data=stats, ax=ax6, color='steelblue')
        ax6.set_ylabel('Fractional Error');
        ax6.set_xlabel('')
        sns.barplot(x='State Name', y='RMSE', data=stats, ax=ax7, color='steelblue')
        ax7.set_ylabel('RMSE');
        ax7.set_xlabel('')
        sns.barplot(x='State Name', y='Anomaly Correlation', data=stats, ax=ax8, color='steelblue')
        ax8.set_ylabel('Anomaly Correlation');
        ax8.set_xlabel('')
        ax7.set_xticklabels(ax7.xaxis.get_majorticklabels(), rotation=87)
        ax8.set_xticklabels(ax8.xaxis.get_majorticklabels(), rotation=87)
        plt.tight_layout()
    return stats


def compare_aqs_pm10(cmaqobj, gridobj, statecompare=True, spatial=True, scatter=False, time=False):
    from numpy import ceil

    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()

    cmaq = get_surface_cmaqvar(cmaqobj, param='PM_TOT')
    dates = get_cmaq_dates(cmaqobj)
    aqs = load_aqs_pm10_data(dates)
    print 'Interpolating values to AQS Surface Sites for PM Total, Date : ', dates[0].strftime('%B %d %Y   %H utc')
    data = interp_to_aqs_sites(cmaq, aqs, gridobj, dates)

    if statecompare:
        # If Scatter == True then it will create scatter plots for each state and all sites within it
        # IF Time    == True then it will create average time series plots for each state
        # If stat    == true then it will create a statistical bar graph for 8 statistics for each state
        print 'Generating State Comparison'

        statestats = aqs_state_comparison(data, scatter=scatter, time=time, stat=statecompare)

    if spatial:
        # this will loop over each date and create the spatial plot with comparisons to observational data overlayed
        print 'Generating Spatial Comparison'
        for index, i in enumerate(dates):
            vmin = 1.
            vmax = ceil(aqs['Sample Measurement'].median() + aqs['Sample Measurement'].std())
            cmaqvar = cmaq[index, :, :].squeeze()
            m, c = cr.make_spatial_plot(cmaqvar, gridobj, dates[index], vmin=vmin, vmax=vmax,
                                        pick='cmaq_grids/basemap-cmaq_conus.p')
            cr.aqs_pm_spatial_scatter(data, m, i.strftime('%Y-%m-%d %H:%M:%S'), vmin=vmin, vmax=vmax)


def compare_aqs_pm25(cmaqobj, gridobj, statecompare=True, spatial=True, scatter=False, time=False):
    from numpy import ceil

    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()

    cmaq = get_surface_cmaqvar(cmaqobj, param='PM25')
    dates = get_cmaq_dates(cmaqobj)
    dates, index = unique(dates, return_index=True)
    cmaq = cmaq[index, :, :]
    aqs = load_aqs_pm25_data(dates)
    print 'Interpolating values to AQS Surface Sites for PM25, Date : ', dates[0].strftime('%B %d %Y   %H utc')
    data = interp_to_aqs_sites(cmaq, aqs, gridobj, dates)

    if statecompare:
        # If Scatter == True then it will create scatter plots for each state and all sites within it
        # IF Time    == True then it will create average time series plots for each state
        # If stat    == true then it will create a statistical bar graph for 8 statistics for each state
        statestats = aqs_state_comparison(data, scatter=scatter, time=time, stat=statecompare)

    if spatial:
        # this will loop over each date and create the spatial plot with comparisons to observational data overlayed
        vmn = 1.
        vmx = ceil(aqs['Sample Measurement'].median() + aqs['Sample Measurement'].std())
        print 'Making Spatial Plots'
        m = load_conus_basemap('cmaq_grids/')
        for index, i in enumerate(dates):
            cmaqvar = cmaq[index, :, :].squeeze()
            m, c = cr.make_spatial_plot(cmaqvar, gridobj, i, m, vmin=vmn, vmax=vmx)
            cr.aqs_pm_spatial_scatter(aqs, m, i.strftime('%Y-%m-%d %H:%M:%S'), 'Sample Measurement', vmin=vmn, vmax=vmx)
            plt.show()


def compare_aqs_pm10_dust(cmaqobj, gridobj, statecompare=True, spatial=True, scatter=False, time=False):
    from numpy import ceil

    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()

    cmaq = get_surface_cmaqvar(cmaqobj, param='PM10_DUST')
    dates = get_cmaq_dates(cmaqobj)
    aqs = load_aqs_pm10_data(dates)
    print 'Interpolating values to AQS Surface Sites for DUST, Date : ', dates[0].strftime('%B %d %Y   %H utc')
    data = interp_to_aqs_sites(cmaq, aqs, gridobj, dates)

    if statecompare:
        # If Scatter == True then it will create scatter plots for each state and all sites within it
        # IF Time    == True then it will create average time series plots for each state
        # If stat    == true then it will create a statistical bar graph for 8 statistics for each state
        statestats = aqs_state_comparison(data, scatter=scatter, time=time, stat=statecompare)

    if spatial:
        # this will loop over each date and create the spatial plot with comparisons to observational data overlayed
        vmin = 1.
        vmax = ceil(aqs['Sample Measurement'].median() + aqs['Sample Measurement'].std())
        for index, i in enumerate(dates):
            cmaqvar = cmaq[index, :, :].squeeze()
            m, c = cr.make_spatial_plot(cmaqobj, gridobj, dates[index], vmin=vmin, vmax=vmax,
                                        pick='cmaq_grids/basemap-cmaq_conus.p')
            cr.improve_spatial_scatter(improve, m, i.strftime('%Y-%m-%d %H:%M:%S'), 'Sample Measurement', vmn=vmin,
                                       vmax=vmax)


def compare_aqs_ozone(cmaqobj, gridobj, statecompare=True, spatial=True, scatter=False, time=False):
    from numpy import ceil

    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()

    cmaq = get_surface_cmaqvar(cmaqobj, param='O3') * 1000.
    print cmaq.max(), cmaq.mean()
    dates = get_cmaq_dates(cmaqobj)
    aqs = load_aqs_o3_data(dates)
    print 'Interpolating values to AQS Surface Sites for O3, Date : ', dates[0].strftime('%B %d %Y   %H utc')
    data = interp_to_aqs_sites(cmaqvar, aqs, gridobj, dates)

    if statecompare:
        # If Scatter == True then it will create scatter plots for each state and all sites within it
        # IF Time    == True then it will create average time series plots for each state
        # If stat    == true then it will create a statistical bar graph for 8 statistics for each state
        statestats = aqs_state_comparison(data, scatter=scatter, time=time, stat=statecompare)

    if spatial:
        # this will loop over each date and create the spatial plot with comparisons to observational data overlayed
        vmin = 1.
        vmax = ceil(aqs['Sample Measurement'].median() + aqs['Sample Measurement'].std())
        for index, i in enumerate(dates):
            cmaqvar = cmaq[index, :, :].squeeze()
            m, c = cr.make_spatial_plot(cmaqobj, gridobj, dates[index], vmin=vmin, vmax=vmax,
                                        pick='cmaq_grids/basemap-cmaq_conus.p')
            cr.improve_spatial_scatter(improve, m, i.strftime('%Y-%m-%d %H:%M:%S'), 'Sample Measurement', vmin=vmin,
                                       vmax=vmax)


def compare_aqs_24h_pm25(cmaqobj, gridobj, statecompare=True, spatial=True, scatter=False, time=False, firstday=True):
    from numpy import ceil, unique

    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()

    print 'Getting CMAQ values'
    if firstday:
        cmaq = get_surface_cmaqvar(cmaqobj, param='pm25')
        dates = get_cmaq_dates(cmaqobj)
        dates, index = unique(dates, return_index=True)
        aqs = load_daily_aqs_pm25_data(dates)
        cmaq = cmaq[index, :, :]
    print 'Interpolating values to AQS Surface Sites for 24H Mean PM25, Date : ', dates[0].strftime('%B %d %Y   %H utc')
    data = interp_to_aqs_sites_daily_pm25(cmaq, aqs, gridobj, dates)

    if statecompare:
        # If Scatter == True then it will create scatter plots for each state and all sites within it
        # IF Time    == True then it will create average time series plots for each state
        # If stat    == true then it will create a statistical bar graph for 8 statistics for each state
        statestats = aqs_state_comparison_24h_pm25(data, scatter=scatter, time=time, stat=statecompare)

    if spatial:
        # this will loop over each date and create the spatial plot with comparisons to observational data overlayed
        vmin = 1.
        vmax = ceil(data['Arithmetic Mean'].median() + data['Arithmetic Mean'].std())
        for index, i in enumerate(dates):
            cmaqvar = cmaq[index, :, :].squeeze()
            m, c = cr.make_spatial_plot(cmaqobj, gridobj, dates[index], vmin=vmin, vmax=vmax,
                                        pick='cmaq_grids/basemap-cmaq_conus.p')
            cr.improve_spatial_scatter(improve, m, i.strftime('%Y-%m-%d'), 'Arithmetic Mean', vmin=vmin, vmax=vmax)

    data.to_hdf('2015_interpolated_data.hdf', 'df', format='table')
