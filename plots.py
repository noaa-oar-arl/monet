import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

sns.set_context('poster')


# CMAQ Spatial Plots
def make_spatial_plot(cmaqvar, gridobj, date, m, dpi=None, savename=''):
    from numpy import arange
    fig = plt.figure(figsize=(12, 6), frameon=False)
    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()
    # define map and draw boundries
    m.drawstates()
    m.drawcoastlines(linewidth=.3)
    m.drawcountries()
    x, y = m(lon, lat)
    plt.axis('off')

    cmap = plt.cm.get_cmap('plasma')
    norm = normval(0, 150., cmap)
    m.pcolormesh(x, y, cmaqvar, norm=norm, cmap=cmap)
    titstring = date.strftime('%B %d %Y %H')
    plt.title(titstring)

    c = plt.colorbar(ticks=arange(0, 150, 10) + 10)
    c.set_ticklabels([str(s) for s in arange(0, 150, 10) + 10])

    plt.tight_layout()
    if savename != '':
        plt.savefig(savename + date.strftime('%Y%m%d_%H.jpg'), dpi=dpi)
        plt.close()
    return c


def normval(vmin, vmax, cmap):
    from numpy import arange
    from matplotlib.colors import BoundaryNorm
    bounds = arange(vmin, vmax+5., 5.)
    norm = BoundaryNorm(boundaries=bounds, ncolors=cmap.N)
    return norm


# Spatial Plotting of AQS on basemap instance m
def aqs_spatial_scatter(aqs, m, date, savename=''):
    new = aqs[aqs.datetime == date]
    x, y = m(new.Longitude.values, new.Latitude.values)
    cmap = plt.cm.get_cmap('plasma')
    norm = normval(0, 150., cmap)
    plt.scatter(x, y, c=new['Obs_value'].values, norm=norm, cmap=cmap)
    if savename != '':
        plt.savefig(savename + date + '.jpg', dpi=75.)
        plt.close()


def airnow_spatial_scatter(df, m, date, savename=''):
    new = df[df.datetime == date]
    x, y = m(new.Longitude.values, new.Latitude.values)
    cmap = plt.cm.get_cmap('plasma')
    norm = normval(0, 150., cmap)
    plt.scatter(x, y, c=new['Obs'].values, norm=norm, cmap=cmap, edgecolors='w', linewidths=.1)
    if savename != '':
        plt.savefig(savename + date + '.jpg', dpi=75.)
        plt.close()

def airnow_8hr_spatial_scatter(df, m, date, savename=''):
    fig = plt.figure(figsize=(12, 6), frameon=False)
    m.drawcoastlines(linewidth=.3)
    m.drawstates()
    m.drawcountries()

    plt.axis('off')
    new = df[df.datetime_local == date]
    x, y = m(new.Longitude.values, new.Latitude.values)
    cmap = plt.cm.get_cmap('plasma')
    norm = normval(-40, 40., cmap)
    ss = (new.Obs - new.CMAQ).abs()
    colors = new.Obs - new.CMAQ
    m.scatter(x, y, s=ss,c=colors, norm=norm, cmap=cmap)
    if savename != '':
        plt.savefig(savename + date + '.jpg', dpi=75.)
        plt.close()


# Spatial Plotting of Improve on maps
def improve_spatial_scatter(improve, m, date, param, vmin=None, vmax=None, cmap='viridis', savename=''):
    new = improve[improve.datetime == date]
    x, y = m(new.Lon.values, new.Lat.values)
    plt.scatter(x, y, c=new[param].values, vmin=vmin, vmax=vmax, cmap=cmap)
    if savename != '':
        plt.savefig(savename + date + '.jpg', dpi=75.)
    plt.close()


# Time series implementation for AQS
def plot_timeseries(dataframe, domain_ave=True, ylabel='PM10 Concentration', savename='', title='', convert=True):
    import gc
    # format plot stuff
    sns.set_style('whitegrid')
    if convert:
        dataframe['Obs_value'] *= 1000.
    if domain_ave:
        domainave = dataframe.groupby('datetime').mean()
        plt.plot(domainave.index.values, domainave['Obs_value'].values, 'k', label='OBS')
        plt.plot(domainave.index.values, domainave['cmaq'].values, label='CMAQ')
        plt.ylabel(ylabel)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        plt.xticks(rotation=30)
        plt.title(title)
        plt.tight_layout()
        plt.legend(loc='best')
        if savename != '':
            plt.savefig(savename + '_average.jpg', dpi=100)
            plt.close()
    else:
        plt.plot(dataframe.index, dataframe['Obs_value'], 'k', label='OBS')
        plt.plot(dataframe.index, dataframe['cmaq'], label='CMAQ')
        plt.ylabel(ylabel)
        plt.xticks(rotation=30)
        plt.title(title)
        plt.tight_layout()
        plt.legend(loc='best')
        if savename != '':
            plt.savefig(savename + str(dataframe['SCS'].unique()[0]) + '.jpg', dpi=100)
            plt.close()

    gc.collect()


# Plot each site in the AQS dataframe
def plot_allsites_timeseries(dataframe, ylabel='PM10 Concentration', savename=''):
    sites = dataframe.SCS.unique()
    dataframe.index = dataframe.datetime
    for i in sites:
        sitedf = dataframe[dataframe['SCS'] == i]
        plot_timeseries(sitedf, siteave=False, ylabel=ylabel, savename=savename)
    plot_timeseries(dataframe, siteave=True, ylabel=ylabel, savename=savename)


def airnow_timeseries(df, title=''):
    # this is the average for N sites if more than one site exists
    from numpy import sqrt, linspace
    import matplotlib.dates as mdates
    sns.set_style('ticks')
    df.index = df.datetime
    g = df.groupby('Species')
    f, ax = plt.subplots(3, 1, figsize=(15, 8), sharex=True)
    ax[0].plot(g.get_group('NOX').resample('H').mean().dropna().Obs, color='darkslategrey', label='Obs NOx', marker='o')
    ax[0].plot(g.get_group('NOX').resample('H').mean().dropna().CMAQ, color='chocolate', label='CMAQ NOx')
    ax[0].legend(loc=9)
    #ax1 = ax[0].twinx()
    ####################################################################################################################
    #ax1.plot(g.get_group('NOY').resample('H').mean().dropna().Obs, color='dimgrey', label='Obs NOy', marker='o',
    #         ls='-.')
    #ax1.plot(g.get_group('NOY').resample('H').mean().dropna().CMAQ, color='tomato', label='CMAQ NOy')
    #ax1.set_ylabel('NOy', color='tomato')
    #ax1.legend(loc=1)
    ####################################################################################################################
    ax[1].plot(g.get_group('OZONE').resample('H').mean().dropna().Obs, color='darkslategrey', label='Obs', marker='o')
    ax[1].plot(g.get_group('OZONE').resample('H').mean().dropna().CMAQ, color='cornflowerblue', label='CMAQ Ozone',
               lw=2)
    ax[1].legend(loc=0)
    ####################################################################################################################
    mbnox = (g.get_group('NOX').CMAQ - g.get_group('NOX').Obs).resample('H').mean()
    rmses = sqrt((g.get_group('NOX').Obs - g.get_group('NOX').CMAQ) ** 2).resample('H').mean()
    dt = g.get_group('NOX').resample('H').mean().index
    ax[2].plot(dt, mbnox, color='darkorange')
    ax3 = ax[2].twinx()
    ax3.plot(dt,rmses, color='darkorange', ls='--')
    mbnox = (g.get_group('OZONE').CMAQ - g.get_group('OZONE').Obs).resample('H').mean()
    rmses = sqrt((g.get_group('OZONE').Obs - g.get_group('OZONE').CMAQ) ** 2).resample('H').mean()
    ax[2].plot(dt, mbnox, color='cornflowerblue')
    ax3.plot(dt, rmses, color='cornflowerblue', ls='--')
    ####################################################################################################################
    ax[0].set_ylabel('NOx (pbb)', color='chocolate')
    ax[1].set_ylabel('Ozone (pbb)', color='cornflowerblue')
    ax[2].set_ylabel('Bias (Solid)')
    ax3.set_ylabel('RMSE (Dashed)')
    ax[0].set_title(title)
    ax3.set_yticks(linspace(ax3.get_ybound()[0], ax3.get_ybound()[1], 6))
    ax[2].set_yticks(linspace(ax[2].get_ybound()[0], ax[2].get_ybound()[1], 6))
    ax[2].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
    ax[2].set_xlabel('Time (mm/dd HH)')
    plt.tight_layout()

    f, ax = plt.subplots(3, 1, figsize=(15, 8), sharex=True)
    ax[0].set_title(title)

    ####################################################################################################################
    ax[0].plot(g.get_group('CO').resample('H').mean().dropna().Obs, color='darkslategrey',
               label='Obs CO', marker='o')  # this is the average for N sites
    ax[0].plot(g.get_group('CO').resample('H').mean().dropna().CMAQ, color='seagreen',
               label='CMAQ CO', lw=2)  # this is the average for N sites
    ax[0].legend(loc=0)
    ####################################################################################################################
    ax[1].plot(g.get_group('SO2').resample('H').mean().dropna().Obs, color='darkslategrey', label='Obs SO2', marker='o')
    ax[1].plot(g.get_group('SO2').resample('H').mean().dropna().CMAQ, color='slateblue', label='CMAQ SO2', ls='-', lw=2)
    ax[1].legend(loc=0)
    ####################################################################################################################
    mbnox = (g.get_group('CO').CMAQ - g.get_group('CO').Obs).resample('H').mean()
    rmses = sqrt((g.get_group('CO').Obs - g.get_group('CO').CMAQ) ** 2).resample('H').mean()
    dt = g.get_group('CO').resample('H').mean().index
    ax3 = ax[2].twinx()
    ax[2].plot(dt, mbnox, color='seagreen')
    ax3.plot(dt, rmses, color='seagreen', ls='--')
    mbnox = (g.get_group('SO2').CMAQ - g.get_group('SO2').Obs).resample('H').mean()
    rmses = sqrt((g.get_group('SO2').Obs - g.get_group('SO2').CMAQ) ** 2).resample('H').mean()
    ax[2].plot(dt, mbnox, color='slateblue')
    ax3.plot(dt, rmses, color='slateblue', ls='--')
    ####################################################################################################################

    ax[0].set_ylabel('CO (pbb)', color='seagreen')
    ax[1].set_ylabel('SO2 (pbb)', color='slateblue')
    ax[2].set_ylabel('Bias (Solid)')
    ax3.set_ylabel('RMSE (Dashed)')
    ax3.set_yticks(linspace(ax3.get_ybound()[0], ax3.get_ybound()[1], 6))
    ax[2].set_yticks(linspace(ax[2].get_ybound()[0], ax[2].get_ybound()[1], 6))
    ax[2].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
    ax[2].set_xlabel('Time (mm/dd HH)')
    plt.tight_layout()


def airnow_domain_bar(df):
    import mystats
    from pandas import DataFrame
    from numpy import array
    name, nmb, ioa, r2, rmse = [], [], [], [], []
    for n, g in df.groupby('Species'):
        name.append(n)
        nmb.append(mystats.NMB(g.Obs.values, g.CMAQ.values))
        ioa.append(mystats.IOA(g.Obs.values, g.CMAQ.values))
        r2.append(mystats.R2(g.Obs.values, g.CMAQ.values))
        rmse.append(mystats.RMSE(g.Obs.values, g.CMAQ.values))
        sns.set_style('white')
        df2 = DataFrame(array([nmb, rmse, r2, ioa]).T,
                        columns=['Normalized Mean Bias', 'RMSE', 'R2', 'Index of Agreement'])
    f, ax = plt.subplots(figsize=(8, 5))
    df2.plot.bar(alpha=.9, width=.9, ax=ax)
    ax.set_title('Domain Statistics')
    ax.xaxis.set_ticklabels(name, rotation=35)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.set_title('Domain Statistics')


def airnow_kdeplots(df):
    from numpy import max
    from scipy.stats import linregress
    from scipy.stats import scoreatpercentile as score
    from mystats import NMB, NME, MB
    sns.set_style('ticks')
    for n, g in df.groupby('Species'):
        g = g.copy().dropna()
        tt = linregress(g.Obs.values, g.CMAQ.values)
        plt.figure(figsize=(10, 7))
        sns.kdeplot(g.Obs)
        sns.kdeplot(g.CMAQ)
        sns.despine()

        maxval1 = score(g.CMAQ.values, per=99.5)
        maxval2 = score(g.Obs.values, per=99.5)
        maxval = max([maxval1, maxval2])
        nmb = NMB(g.Obs.values, g.CMAQ.values)
        nme = NME(g.Obs.values, g.CMAQ.values)
        mb = MB(g.Obs.values, g.CMAQ.values)
        airnow_footer_text(g,nmb,nme,mb,tt[2])
        plt.xlim([0, maxval])
        plt.xlabel(n+'  (' + g.Units.unique()[0] + ')')
        plt.ylabel('P(' + n + ')')
        plt.tight_layout()


def airnow_scatter(df):
    from numpy import max, arange, linspace
    from scipy.stats import scoreatpercentile as score
    from scipy.stats import linregress
    from mystats import NMB, NME, MB
    sns.set_style('ticks')
    for n, g in df.groupby('Species'):
        plt.figure(figsize=(10, 7))
        g = g.copy().dropna()

        tt = linregress(g.Obs.values, g.CMAQ.values)

        maxval1 = score(g.CMAQ.values, per=99.9)
        maxval2 = score(g.Obs.values, per=99.9)
        maxval = max([maxval1, maxval2])
        nmb = NMB(g.Obs.values, g.CMAQ.values)
        nme = NME(g.Obs.values, g.CMAQ.values)
        mb = MB(g.Obs.values, g.CMAQ.values)
        textstr = '$R^2$    = $%.3f$\nNMB = $%.2f$\nNME = $%.2f$\nMB  = $%.2f$' % (tt[2], nmb, nme, mb)
        plt.scatter(g.Obs, g.CMAQ, c='cornflowerblue', marker='o', edgecolors='w', alpha=.3)
        x = arange(0, maxval)
        if maxval <= 10.:
            x = linspace(0, maxval, 25)
        plt.plot(x, x, '--', color='slategrey')
        plt.plot(x, tt[0] * x + tt[1], color='tomato')
        plt.xlim([0, maxval])
        plt.ylim([0, maxval])
        ax = plt.gca().axes
        airnow_footer_text(g,nmb,nme,mb,tt[2])
        sns.despine()
        plt.xlabel('Obs')
        plt.ylabel('CMAQ')
        plt.title(n + '  (' + g.Units.unique()[0] + ')')
        plt.tight_layout()



def airnow_footer_text(df,nmb,nme,mb,r2):
    from numpy import unique
    plt.figtext(.03,.04,df.datetime.min().strftime('START DATE: %Y-%m-%d %H UTC'),fontsize=11,family='monospace')
    plt.figtext(.03,.02,df.datetime.max().strftime('END DATE  : %Y-%m-%d %H UTC'),fontsize=11,family='monospace')
    plt.figtext(0.8,.02,'R2 = %.3f'%r2,fontsize=11,family='monospace')
    plt.figtext(0.9,.02,'NME = %.1f'%nme,fontsize=11,family='monospace')
    plt.figtext(0.8,.04,'MB = %.1f'%mb,fontsize=11,family='monospace')
    plt.figtext(0.9,.04,'NMB = %.1f'%nmb,fontsize=11,family='monospace')
    plt.figtext(.3,.04,'SITES: '+str(unique(df.SCS.values).shape[0]),fontsize=11,family='monospace')
    plt.figtext(.3,.02,'MEASUREMENTS: '+str(df.SCS.count()),fontsize=11,family='monospace')

