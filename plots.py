import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import seaborn as sns

colors = ['#DA70D6', '#228B22', '#FA8072', '#FF1493']
sns.set_palette(sns.color_palette(colors))

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
    bounds = arange(vmin, vmax + 5., 5.)
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
    m.scatter(x, y, s=ss, c=colors, norm=norm, cmap=cmap)
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


def airnow_timeseries_param(df, title='', fig=None, label=None, color=None, footer=True):
    import matplotlib.dates as mdates
    sns.set_style('ticks')
    df.index = df.datetime
    if fig == None:

        f = plt.figure(figsize=(12, 7))
        if label == None:
            label = 'CMAQ'
        species = df.Species.unique().astype('|S8')[0]
        units = df.Units.unique().astype('|S8')[0]
        obs = df.Obs.resample('H').mean()
        obserr = df.Obs.resample('H').std()
        cmaq = df.CMAQ.resample('H').mean()
        cmaqerr = df.CMAQ.resample('H').std()
        plt.plot(obs, color='darkslategrey')
        plt.plot(cmaq, color='dodgerblue',label=label)
        plt.legend(loc='best')

        plt.fill_between(df.datetime.unique(), obs - obserr, obs + obserr, alpha=.2, color='darkslategrey')
        plt.fill_between(df.datetime.unique(), cmaq - cmaqerr, cmaq + cmaqerr, alpha=.2, color='dodgerblue')

        ax = plt.gca().axes
        ax.set_xlabel('UTC Time (mm/dd HH)')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
        plt.title(title)
        minval = min([(obs-obserr).min(),(cmaq-cmaqerr).min()])
        minval = max([minval,0])
        plt.gca().set_ylim(bottom=minval)
        ylabel = species + ' (' + units + ')'
        plt.gca().axes.set_ylabel(ylabel)
        if footer:
            airnow_footer_text(df)
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        cmaq = df.CMAQ.resample('H').mean()
        cmaqerr = df.CMAQ.resample('H').std()
        lin, = ax.plot(cmaq, label=label)
        ax.fill_between(df.datetime.unique(), cmaq - cmaqerr, cmaq + cmaqerr, alpha=.2, color=lin.get_color())
        plt.legend(loc='best')


def airnow_timeseries_error_param(df, title='', fig=None, label=None, footer=True):
    import matplotlib.dates as mdates
    from numpy import sqrt
    sns.set_style('ticks')

    df.index = df.datetime
    if fig == None:
        plt.figure(figsize=(12, 7))

        species = df.Species.unique().astype('|S8')[0]
        units = df.Units.unique().astype('|S8')[0]

        mb = (df.CMAQ - df.Obs).resample('H').mean()
        rmse = sqrt((df.CMAQ - df.Obs) ** 2).resample('H').mean()

        a = plt.plot(mb, label='Mean Bias', color='dodgerblue')
        ax = plt.gca().axes
        ax2 = ax.twinx()
        b = ax2.plot(rmse,label='RMSE', color='tomato')
        #b = plt.plot(rmse, label='RMSE', color='tomato')
        lns = a + b
        labs = [l.get_label() for l in lns]
        plt.legend(lns, labs, loc='best')

        ax.set_xlabel('UTC Time (mm/dd HH)')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
        plt.title(title)
        ylabel = species + ' (' + units + ')'
        ax.set_ylabel('MB ' + ylabel, color='dodgerblue')
        ax2.set_ylabel('RMSE ' + ylabel, color='tomato')
        if footer:
            airnow_footer_text(df)
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax1 = fig.get_axes()[0]
        ax2 = fig.get_axes()[1]
        mb = (df.CMAQ - df.Obs).resample('H').mean()
        rmse = sqrt((df.CMAQ - df.Obs) ** 2).resample('H').mean()
        ax1.plot(mb, label=label + ' MB')
        ax2.plot(rmse, label=label + ' RMSE')
        lns = ax1.get_lines()[:] + ax2.get_lines()[1:]
        labs = [l.get_label() for l in lns]
        plt.legend(lns, labs, loc='best')


def airnow_timeseries_rmse_param(df, title='', fig=None, label=None, footer=True):
    import matplotlib.dates as mdates
    from numpy import sqrt
    sns.set_style('ticks')
    df.index = df.datetime
    if fig == None:
        plt.figure(figsize=(12, 7))
        species = df.Species.unique().astype('|S8')[0]
        units = df.Units.unique().astype('|S8')[0]
        rmse = sqrt((df.CMAQ - df.Obs) ** 2).resample('H').mean()
        plt.plot(rmse,color='dodgerblue',label=label)
        ylabel = species + ' (' + units + ')'
        plt.gca().axes.set_ylabel('RMSE '+ ylabel)
        if footer:
            airnow_footer_text(df)
        ax = plt.gca().axes
        ax.set_xlabel('UTC Time (mm/dd HH)')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        rmse = sqrt((df.CMAQ - df.Obs) ** 2).resample('H').mean()
        ax.plot(rmse,label=label)
        plt.legend(loc='best')

def airnow_timeseries_mb_param(df, title='', fig=None, label=None, footer=True):
    import matplotlib.dates as mdates
    sns.set_style('ticks')
    df.index = df.datetime
    if fig == None:
        plt.figure(figsize=(12, 7))
        species = df.Species.unique().astype('|S8')[0]
        units = df.Units.unique().astype('|S8')[0]
        mb = (df.CMAQ - df.Obs).resample('H').mean()
        plt.plot(mb,color='dodgerblue',label=label)
        ylabel = species + ' (' + units + ')'
        plt.gca().axes.set_ylabel('MB '+ ylabel)
        plt.gca().axes.set_xlabel('UTC Time (mm/dd HH)')
        plt.gca().axes.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
        if footer:
            airnow_footer_text(df)
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        rmse = (df.CMAQ - df.Obs).resample('H').mean()
        ax.plot(rmse,label=label)
        plt.legend(loc='best')


def airnow_kdeplots_param(df, title=None, fig=None, label=None, footer=True):
    from scipy.stats import scoreatpercentile as score
    sns.set_style('ticks')

    if fig == None:
        maxval1 = score(df.CMAQ.values, per=99.5)
        maxval2 = score(df.Obs.values, per=99.5)
        maxval = max([maxval1, maxval2])
        plt.figure(figsize=(10, 7))

        sns.kdeplot(df.Obs, color='darkslategrey')
        sns.kdeplot(df.CMAQ, color='dodgerblue')
        sns.despine()

        plt.xlim([0, maxval])
        plt.xlabel(df.Species.unique()[0] + '  (' + df.Units.unique()[0] + ')')
        plt.title(title)
        plt.gca().axes.set_ylabel('P(' + df.Species.unique()[0] + ')')
        if footer:
            airnow_footer_text(df)
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        sns.kdeplot(df.CMAQ, ax=ax, label=label)


def airnow_diffpdfs_param(df, title=None, fig=None, label=None, footer=True):
    from scipy.stats import scoreatpercentile as score
    sns.set_style('ticks')

    maxval = score(df.CMAQ.values - df.Obs.values, per=99.9)
    minval = score(df.CMAQ.values - df.Obs.values, per=.1)
    if fig == None:
        plt.figure(figsize=(10, 7))
        if label == 'None':
            label = 'CMAQ - Obs'
        sns.kdeplot(df.CMAQ.values - df.Obs.values, color='darkslategrey', label=label)
        sns.despine()
        plt.xlim([minval, maxval])
        plt.xlabel(df.Species.unique()[0] + ' Difference (' + df.Units.unique()[0] + ')')
        plt.title(title)
        plt.gca().axes.set_ylabel('P( Model - Obs )')
        if footer:
            airnow_footer_text(df)
        plt.tight_layout()
    else:
        ax = fig.get_axes()[0]
        sns.kdeplot(df.CMAQ.values - df.Obs.values, ax=ax, label=label)


def airnow_scatter_param(df, title=None, fig=None, label=None, footer=True):
    from numpy import max, arange, linspace,isnan
    from scipy.stats import scoreatpercentile as score
    from scipy.stats import linregress
    sns.set_style('ticks')

    species, units = df.Species.unique()[0], df.Units.unique()[0]

    maxval1 = score(df.CMAQ.values, per=99.5)
    maxval2 = score(df.Obs.values, per=99.5)
    maxval = max([maxval1, maxval2])
    if fig == None:
        plt.figure(figsize=(10, 7))

        plt.scatter(df.Obs, df.CMAQ, c='cornflowerblue', marker='o', edgecolors='w', alpha=.3, label=label)
        x = arange(0, maxval + 1)
        if maxval <= 10.:
            x = linspace(0, maxval, 25)
        plt.plot(x, x, '--', color='slategrey')
        mask = ~isnan(df.Obs.values) & ~isnan(df.CMAQ.values)
        tt = linregress(df.Obs.values[mask], df.CMAQ.values[mask])
        plt.plot(x, tt[0] * x + tt[1], color='tomato')

        plt.xlim([0, maxval])
        plt.ylim([0, maxval])
        plt.xlabel('Obs ' + species + ' (' + units + ')')
        plt.title(title)
        plt.gca().axes.set_ylabel('Model ' + species + ' (' + units + ')')
        if footer:
            airnow_footer_text(df)
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        l, = ax.scatter(df.Obs, df.CMAQ, marker='o', edgecolors='w', alpha=.3, label=label)
        tt = linregress(df.Obs.values, df.CMAQ.values)
        ax.plot(df.Obs.unique(), tt[0] * df.Obs.unique() + tt[1], color=l.get_color())
        plt.legend(loc='Best')


def airnow_diffscatter_param(df, title=None, fig=None, label=None, footer=True):
    from scipy.stats import scoreatpercentile as score
    sns.set_style('ticks')
    df = df.dropna()
    if fig == None:
        species, units = df.Species.unique()[0], df.Units.unique()[0]
        maxval = score(df.Obs.values, per=99.9)
        minvaly = score(df.CMAQ.values - df.Obs.values, per=.1)
        maxvaly = score(df.CMAQ.values - df.Obs.values, per=99.9)
        plt.figure(figsize=(10, 7))

        plt.scatter(df.Obs, df.CMAQ - df.Obs, c='cornflowerblue', marker='o', edgecolors='w', alpha=.3, label=label)
        plt.plot((0, maxval), (0, 0), '--', color='darkslategrey')

        plt.xlim([0, maxval])
        plt.ylim([minvaly, maxvaly])
        plt.xlabel('Obs ' + species + ' (' + units + ')')
        plt.title(title)
        plt.gca().axes.set_ylabel('Model - Obs ' + species + ' (' + units + ')')
        if footer:
            airnow_footer_text(df)
        plt.tight_layout()
    else:
        ax = fig.get_axes()[0]
        ax.scatter(df.Obs, df.CMAQ - df.Obs, marker='o', edgecolors='w', alpha=.3, label=label)
        plt.legend(loc='best')


def airnow_timeseries(df, title=''):
    # this is the average for N sites if more than one site exists
    from numpy import sqrt, linspace
    import matplotlib.dates as mdates
    sns.set_style('ticks')
    df.index = df.datetime
    g = df.groupby('Species')
    f, ax = plt.subplots(3, 1, figsize=(15, 8), sharex=True)
    ax[0].plot(g.get_group('NOX').resample('H').mean().dropna().Obs, color='darkslategrey', label='Obs NOx', marker='o')
    ax[0].plot(g.get_group('NOX').resample('H').mean().dropna().CMAQ, color='darkorange', label='CMAQ NOx')
    ax[0].legend(loc=9)
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
    ax3.plot(dt, rmses, color='darkorange', ls='--')
    mbnox = (g.get_group('OZONE').CMAQ - g.get_group('OZONE').Obs).resample('H').mean()
    rmses = sqrt((g.get_group('OZONE').Obs - g.get_group('OZONE').CMAQ) ** 2).resample('H').mean()
    ax[2].plot(dt, mbnox, color='cornflowerblue')
    ax3.plot(dt, rmses, color='cornflowerblue', ls='--')
    ####################################################################################################################
    ax[0].set_ylabel('NOx (pbb)', color='darkorange')
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
        airnow_footer_text(g)
        plt.xlim([0, maxval])
        plt.xlabel(n + '  (' + g.Units.unique()[0] + ')')
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
        airnow_footer_text(g)
        sns.despine()
        plt.xlabel('Obs')
        plt.ylabel('CMAQ')
        plt.title(n + '  (' + g.Units.unique()[0] + ')')
        plt.tight_layout()


def airnow_footer_text(df):
    from numpy import unique
    from mystats import NMB, NME, MB, d1
    nmb = NMB(df.Obs.values, df.CMAQ.values)
    nme = NME(df.Obs.values, df.CMAQ.values)
    mb = MB(df.Obs.values, df.CMAQ.values)
    d1ioa = d1(df.Obs.values, df.CMAQ.values)
    plt.figtext(.03, .04, df.datetime.min().strftime('START DATE: %Y-%m-%d %H UTC'), fontsize=11, family='monospace')
    plt.figtext(.03, .02, df.datetime.max().strftime('END DATE  : %Y-%m-%d %H UTC'), fontsize=11, family='monospace')
    plt.figtext(0.8, .02, 'd1 = %.3f' % d1ioa, fontsize=11, family='monospace')
    plt.figtext(0.9, .02, 'NME = %.1f' % nme, fontsize=11, family='monospace')
    plt.figtext(0.8, .04, 'MB = %.1f' % mb, fontsize=11, family='monospace')
    plt.figtext(0.9, .04, 'NMB = %.1f' % nmb, fontsize=11, family='monospace')
    plt.figtext(.3, .04, 'SITES: ' + str(unique(df.SCS.values).shape[0]), fontsize=11, family='monospace')
    plt.figtext(.3, .02, 'MEASUREMENTS: ' + str(df.SCS.count()), fontsize=11, family='monospace')
