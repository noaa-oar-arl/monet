import matplotlib.pyplot as plt
import seaborn as sns

import mystats
import taylordiagram as td
from colorbars import colorbar_index

# colors = ['#1e90ff','#045C5C','#00A847','#DB4291','#BB7E5D']
colors = ['#1e90ff', '#DA70D6', '#228B22', '#FA8072', '#FF1493']
sns.set_palette(sns.color_palette(colors))

sns.set_context('poster')

# CMAQ Spatial Plots


def make_spatial_plot(cmaqvar, m, dpi=None, plotargs={}, ncolors=15, discrete=False):
    # create figure
    f, ax = plt.subplots(1, 1, figsize=(11, 6), frameon=False)
    # determine colorbar
    if 'cmap' not in plotargs:
        plotargs['cmap'] = 'viridis'
    if discrete and 'vmin' in plotargs and 'vmax' in plotargs:
        c, cmap = colorbar_index(ncolors, plotargs['cmap'], minval=plotargs['vmin'], maxval=plotargs['vmax'], basemap=m)
        plotargs['cmap'] = cmap
        m.imshow(cmaqvar, **plotargs)
        vmin, vmax = plotargs['vmin'], plotargs['vmax']
    elif discrete:
        temp = m.imshow(cmaqvar, **plotargs)
        vmin, vmax = temp.get_clim()
        c, cmap = colorbar_index(ncolors, plotargs['cmap'], minval=vmin, maxval=vmax, basemap=m)
        plotargs['cmap'] = cmap
        m.imshow(cmaqvar, vmin=vmin, vmax=vmax, **plotargs)
    else:
        temp = m.imshow(cmaqvar, **plotargs)
        c = m.colorbar()
        vmin, vmax = temp.get_clim()
        cmap = plotargs['cmap']
    # draw borders
    m.drawstates()
    m.drawcoastlines(linewidth=.3)
    m.drawcountries()
    return f, ax, c, cmap, vmin, vmax


def make_spatial_contours(cmaqvar, gridobj, date, m, dpi=None, savename='', discrete=True, ncolors=None, dtype='int', **kwargs):
    fig = plt.figure(figsize=(11, 6), frameon=False)
    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()
    # define map and draw boundries
    m.drawstates()
    m.drawcoastlines(linewidth=.3)
    m.drawcountries()
    x, y = m(lon, lat)
    plt.axis('off')
    m.contourf(x, y, cmaqvar, **kwargs)
    cmap = kwargs['cmap']
    levels = kwargs['levels']
    if discrete:
        c, cmap = colorbar_index(ncolors, cmap, minval=levels[0], maxval=levels[-1], basemap=m, dtype=dtype)
#        m.contourf(x, y, cmaqvar, **kwargs,cmap=cmap)
    # c, cmap = colorbar_index(ncolors, cmap, minval=vmin, maxval=vmax)
    else:
        c = m.colorbar()
    titstring = date.strftime('%B %d %Y %H')
    plt.title(titstring)

    plt.tight_layout()
    if savename != '':
        plt.savefig(savename + date.strftime('%Y%m%d_%H.jpg'), dpi=dpi)
        plt.close()
    return c


def wind_quiver(ws, wdir, gridobj, m, **kwargs):
    import tools
    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()
    # define map and draw boundries
    x, y = m(lon, lat)
    u, v = tools.wsdir2uv(ws, wdir)
    quiv = m.quiver(x[::15, ::15], y[::15, ::15], u[::15, ::15], v[::15, ::15], **kwargs)
    return quiv


def wind_barbs(ws, wdir, gridobj, m, **kwargs):
    import tools
    lat = gridobj.variables['LAT'][0, 0, :, :].squeeze()
    lon = gridobj.variables['LON'][0, 0, :, :].squeeze()
    # define map and draw boundries
    x, y = m(lon, lat)
    u, v = tools.wsdir2uv(ws, wdir)
    m.barbs(x[::15, ::15], y[::15, ::15], u[::15, ::15], v[::15, ::15], **kwargs)


def normval(vmin, vmax, cmap):
    from numpy import arange
    from matplotlib.colors import BoundaryNorm
    bounds = arange(vmin, vmax + 5., 5.)
    norm = BoundaryNorm(boundaries=bounds, ncolors=cmap.N)
    return norm


def spatial_scatter(df, m, discrete=False, plotargs={}, create_cbar=True):
    x, y = m(df.Longitude.values, df.Latitude.values)
    s = 20
    if create_cbar:
        if discrete:
            cmap = cmap_discretize(cmap, ncolors)
            # s = 20
            if (type(plotargs(vmin)) == None) | (type(plotargs(vmax)) == None):
                plt.scatter(x, y, c=df['Obs'].values, **plotargs)
            else:
                plt.scatter(x, y, c=df['Obs'].values, **plotargs)
        else:
            plt.scatter(x, y, c=df['Obs'].values, **plotargs)
    else:
        plt.scatter(x, y, c=df['Obs'].values, **plotargs)


def spatial_stat_scatter(df, m, date, stat=mystats.MB, ncolors=15, fact=1.5, cmap='RdYlBu_r'):
    new = df[df.datetime == date]
    x, y = m(new.Longitude.values, new.Latitude.values)
    cmap = cmap_discretize(cmap, ncolors)
    colors = new.CMAQ - new.Obs
    ss = (new.Obs - new.CMAQ).abs() * fact


def spatial_bias_scatter(df, m, date, vmin=None, vmax=None, savename='', ncolors=15, fact=1.5, cmap='RdBu_r'):
    from scipy.stats import scoreatpercentile as score
    from numpy import around
#    plt.figure(figsize=(11, 6), frameon=False)
    f, ax = plt.subplots(figsize=(11, 6), frameon=False)
    ax.set_facecolor('white')
    diff = (df.CMAQ - df.Obs)
    top = around(score(diff.abs(), per=95))
    new = df[df.datetime == date]
    x, y = m(new.Longitude.values, new.Latitude.values)
    c, cmap = colorbar_index(ncolors, cmap, minval=top * -1, maxval=top, basemap=m)
    c.ax.tick_params(labelsize=13)
#    cmap = cmap_discretize(cmap, ncolors)
    colors = new.CMAQ - new.Obs
    ss = (new.CMAQ - new.Obs).abs() / top * 100.
    ss[ss > 300] = 300.
    plt.scatter(x, y, c=colors, s=ss, vmin=-1. * top, vmax=top, cmap=cmap, edgecolors='k', linewidths=.25, alpha=.7)
    if savename != '':
        plt.savefig(savename + date + '.jpg', dpi=75.)
        plt.close()
    return f, ax, c


def eight_hr_spatial_scatter(df, m, date, savename=''):
    fig = plt.figure(figsize=(11, 6), frameon=False)
    m.drawcoastlines(linewidth=.3)
    m.drawstates()
    m.drawcountries()

    plt.axis('off')
    new = df[df.datetime_local == date]
    x, y = m(new.Longitude.values, new.Latitude.values)
    cmap = plt.cm.get_cmap('plasma')
    norm = normval(-40, 40., cmap)
    ss = (new.Obs - new.CMAQ).abs() / top * 100.
    colors = new.Obs - new.CMAQ
    m.scatter(x, y, s=ss, c=colors, norm=norm, cmap=cmap)
    if savename != '':
        plt.savefig(savename + date + '.jpg', dpi=75.)
        plt.close()


def timeseries_param(df, col='Obs', ax=None, sample='H', plotargs={}, fillargs={}, title='', label=None):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    col : type
        Description of parameter `col` (the default is 'Obs').
    ax : type
        Description of parameter `ax` (the default is None).
    sample : type
        Description of parameter `sample` (the default is 'H').
    plotargs : type
        Description of parameter `plotargs` (the default is {}).
    fillargs : type
        Description of parameter `fillargs` (the default is {}).
    title : type
        Description of parameter `title` (the default is '').
    label : type
        Description of parameter `label` (the default is None).

    Returns
    -------
    type
        Description of returned object.

    """
    import pandas as pd

    if ax is None:
        f, ax = plt.subplots(figsize=(11, 6), frameon=False)

    sns.set_palette(sns.color_palette(colors))
    sns.set_style('ticks')
    df.index = df.datetime
    m = df.groupby(pd.Grouper(freq=sample)).mean()
    e = df.groupby(pd.Grouper(freq=sample)).std()
    species = df.Species[0]
    unit = df.Units[0]
    upper = m[col] + e[col]
    lower = m[col] - e[col]
    lower.loc[lower < 0] = 0
    lower = lower.values
    if col == 'Obs':
        plotargs['color'] = 'darkslategrey'
    if col == 'Obs':
        fillargs['color'] = 'darkslategrey'
    if col != 'Obs' and 'color' not in plotargs:
        plotargs['color'] = None

    m[col].plot(ax=ax, **plotargs)
    ax.fill_between(m[col].index, lower, upper, **fillargs)
    if label is None:
        ax.set_ylabel(species + ' (' + unit + ')')
    else:
        ax.set_ylabel(label)
    plt.legend()
    plt.title(title)
    plt.tight_layout()
    return ax


def timeseries_error_param(df, col='Obs', ax=None, resample=False, freq='H', plotargs={}, fillargs={}, title='', label=None):
    """Short summary.

    Parameters
    ----------
    df : pandas DataFrame
        pandas dataframe with a column labeled datetime
    col : string
        Description of parameter `col` (the default is 'Obs').
    ax : matplotlib axis handle
        pass a matplotlib axis handle.  Default none creates a new figure and axes handle.
    resample : bool
        Set to true or false to resample the dataframe.  (the default is 'H'  plotargs)
    freq : str
        String for the default frequency to resample.  See http://pandas.pydata.org/pandas-docs/stable/timeseries.html for more documentation (the default is 'H'  plotargs).
    fillargs : dictionary
        (the default is {}).
    title : type
        Description of parameter `title` (the default is '').
    label : type
        Description of parameter `label` (the default is None).

    Returns
    -------
    type
        Description of returned object. """
    import pandas as pd

    if ax is None:
        f, ax = plt.subplots(figsize=(11, 6), frameon=False)

    sns.set_palette(sns.color_palette(colors))
    sns.set_style('ticks')
    df.index = df.datetime
    m = df.groupby(pd.Grouper(freq=sample)).mean()
    e = df.groupby(pd.Grouper(freq=sample)).std()
    species = df.Species[0]
    unit = df.Units[0]
    upper = m[col] + e[col]
    lower = m[col] - e[col]
    lower.loc[lower < 0] = 0
    lower = lower.values
    if col == 'Obs':
        plotargs['color'] = 'darkslategrey'
    if col == 'Obs':
        fillargs['color'] = 'darkslategrey'
    if col != 'Obs' and 'color' not in plotargs:
        plotargs['color'] = None

    m[col].plot(ax=ax, **plotargs)
    ax.fill_between(m[col].index, lower, upper, **fillargs)
    if label is None:
        ax.set_ylabel(species + ' (' + unit + ')')
    else:
        ax.set_ylabel(label)
    plt.legend()
    plt.title(title)
    plt.tight_layout()
    return ax


# def timeseries_error_param(df, title='', fig=None, label=None, footer=True, sample='H'):
#     """
#
#     :param df:
#     :param title:
#     :param fig:
#     :param label:
#     :param footer:
#     :param sample:
#     """
#     import matplotlib.dates as mdates
#     from numpy import sqrt
#     sns.set_style('ticks')
#
#     df.index = df.datetime
#     if fig is None:
#         plt.figure(figsize=(13, 8))
#
#         species = df.Species.unique().astype('|S8')[0]
#         units = df.Units.unique().astype('|S8')[0]
#
#         mb = (df.CMAQ - df.Obs).resample(sample).mean()
#         rmse = sqrt((df.CMAQ - df.Obs) ** 2).resample(sample).mean()
#
#         a = plt.plot(mb, label='Mean Bias', color='dodgerblue')
#         ax = plt.gca().axes
#         ax2 = ax.twinx()
#         b = ax2.plot(rmse, label='RMSE', color='tomato')
#         lns = a + b
#         labs = [l.get_label() for l in lns]
#         plt.legend(lns, labs, loc='best')
#
#         ax.set_xlabel('UTC Time (mm/dd HH)')
#         ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
#         plt.title(title)
#         ylabel = species + ' (' + units + ')'
#         ax.set_ylabel('MB ' + ylabel, color='dodgerblue')
#         ax2.set_ylabel('RMSE ' + ylabel, color='tomato')
#         if footer:
#             footer_text(df)
#         plt.tight_layout()
#         plt.grid(alpha=.5)
#     else:
#         ax1 = fig.get_axes()[0]
#         ax2 = fig.get_axes()[1]
#         mb = (df.CMAQ - df.Obs).resample(sample).mean()
#         rmse = sqrt((df.CMAQ - df.Obs) ** 2).resample(sample).mean()
#         ax1.plot(mb, label=label + ' MB')
#         ax2.plot(rmse, label=label + ' RMSE')
#         lns = ax1.get_lines()[:] + ax2.get_lines()[1:]
#         labs = [l.get_label() for l in lns]
#         plt.legend(lns, labs, loc='best')


def timeseries_rmse_param(df, title='', fig=None, label=None, footer=True, sample='H'):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    title : type
        Description of parameter `title` (the default is '').
    fig : type
        Description of parameter `fig` (the default is None).
    label : type
        Description of parameter `label` (the default is None).
    footer : type
        Description of parameter `footer` (the default is True).
    sample : type
        Description of parameter `sample` (the default is 'H').

    Returns
    -------
    type
        Description of returned object.

    """
    import matplotlib.dates as mdates
    from numpy import sqrt
    sns.set_style('ticks')
    df.index = df.datetime
    if fig is None:
        plt.figure(figsize=(13, 8))
        species = df.Species.unique().astype('|S8')[0]
        units = df.Units.unique().astype('|S8')[0]
        rmse = sqrt((df.CMAQ - df.Obs) ** 2).resample(sample).mean()
        plt.plot(rmse, color='dodgerblue', label=label)
        ylabel = species + ' (' + units + ')'
        plt.gca().axes.set_ylabel('RMSE ' + ylabel)
        if footer:
            footer_text(df)
        ax = plt.gca().axes
        ax.set_xlabel('UTC Time (mm/dd HH)')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        rmse = sqrt((df.CMAQ - df.Obs) ** 2).resample(sample).mean()
        ax.plot(rmse, label=label)
        plt.legend(loc='best')


def timeseries_mb_param(df, title='', fig=None, label=None, footer=True, sample='H'):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    title : type
        Description of parameter `title` (the default is '').
    fig : type
        Description of parameter `fig` (the default is None).
    label : type
        Description of parameter `label` (the default is None).
    footer : type
        Description of parameter `footer` (the default is True).
    sample : type
        Description of parameter `sample` (the default is 'H').

    Returns
    -------
    type
        Description of returned object.

    """
    import matplotlib.dates as mdates
    sns.set_style('ticks')
    df.index = df.datetime
    if fig is None:
        plt.figure(figsize=(13, 8))
        species = df.Species.unique().astype('|S8')[0]
        units = df.Units.unique().astype('|S8')[0]
        mb = (df.CMAQ - df.Obs).resample(sample).mean()
        plt.plot(mb, color='dodgerblue', label=label)
        ylabel = species + ' (' + units + ')'
        plt.gca().axes.set_ylabel('MB ' + ylabel)
        plt.gca().axes.set_xlabel('UTC Time (mm/dd HH)')
        plt.gca().axes.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H'))
        if footer:
            footer_text(df)
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        rmse = (df.CMAQ - df.Obs).resample(sample).mean()
        ax.plot(rmse, label=label)
        plt.legend(loc='best')


def kdeplots_param(df, title=None, fig=None, label=None, footer=True, cumulative=False):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    title : type
        Description of parameter `title` (the default is None).
    fig : type
        Description of parameter `fig` (the default is None).
    label : type
        Description of parameter `label` (the default is None).
    footer : type
        Description of parameter `footer` (the default is True).
    cumulative : type
        Description of parameter `cumulative` (the default is False).

    Returns
    -------
    type
        Description of returned object.

    """
    from scipy.stats import scoreatpercentile as score
    sns.set_style('ticks')

    if fig is None:

        if cumulative:
            plt.figure(figsize=(13, 8))
            sns.kdeplot(df.Obs, color='darkslategrey', cumulative=True, label='Obs')
            sns.kdeplot(df.CMAQ, color='dodgerblue', cumulative=True, label=label)
        else:
            maxval1 = score(df.CMAQ.values, per=99.5)
            maxval2 = score(df.Obs.values, per=99.5)
            maxval = max([maxval1, maxval2])
            plt.figure(figsize=(13, 8))
            sns.kdeplot(df.Obs, color='darkslategrey')
            sns.kdeplot(df.CMAQ, color='dodgerblue', label=label)

        sns.despine()
        if not cumulative:
            plt.xlim([0, maxval])
        plt.xlabel(df.Species.unique()[0] + '  (' + df.Units.unique()[0] + ')')
        plt.title(title)
        plt.gca().axes.set_ylabel('P(' + df.Species.unique()[0] + ')')
        if footer:
            footer_text(df)
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        sns.kdeplot(df.CMAQ, ax=ax, label=label, cumulative=cumulative)


def diffpdfs_param(df, title=None, fig=None, label=None, footer=True):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    title : type
        Description of parameter `title` (the default is None).
    fig : type
        Description of parameter `fig` (the default is None).
    label : type
        Description of parameter `label` (the default is None).
    footer : type
        Description of parameter `footer` (the default is True).

    Returns
    -------
    type
        Description of returned object.

    """
    from scipy.stats import scoreatpercentile as score
    sns.set_style('ticks')

    maxval = score(df.CMAQ.values - df.Obs.values, per=99.9)
    minval = score(df.CMAQ.values - df.Obs.values, per=.1)
    if fig is None:
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
            footer_text(df)
        plt.tight_layout()
    else:
        ax = fig.get_axes()[0]
        sns.kdeplot(df.CMAQ.values - df.Obs.values, ax=ax, label=label)


def scatter_param(df, title=None, fig=None, label=None, footer=True):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    title : type
        Description of parameter `title` (the default is None).
    fig : type
        Description of parameter `fig` (the default is None).
    label : type
        Description of parameter `label` (the default is None).
    footer : type
        Description of parameter `footer` (the default is True).

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import max, arange, linspace, isnan
    from scipy.stats import scoreatpercentile as score
    from scipy.stats import linregress
    sns.set_style('ticks')

    species, units = df.Species.unique()[0], df.Units.unique()[0]
    mask = ~isnan(df.Obs.values) & ~isnan(df.CMAQ.values)
    maxval1 = score(df.CMAQ.values[mask], per=99.5)
    maxval2 = score(df.Obs.values[mask], per=99.5)
    maxval = max([maxval1, maxval2])
    print maxval
    if fig is None:
        plt.figure(figsize=(10, 7))

        plt.scatter(df.Obs, df.CMAQ, c='cornflowerblue', marker='o', edgecolors='w', alpha=.3, label=label)
        x = arange(0, maxval + 1)
        if maxval <= 10.:
            x = linspace(0, maxval, 25)
        plt.plot(x, x, '--', color='slategrey')
        tt = linregress(df.Obs.values[mask], df.CMAQ.values[mask])
        plt.plot(x, tt[0] * x + tt[1], color='tomato')

        plt.xlim([0, maxval])
        plt.ylim([0, maxval])
        plt.xlabel('Obs ' + species + ' (' + units + ')')
        plt.title(title)
        plt.gca().axes.set_ylabel('Model ' + species + ' (' + units + ')')
        if footer:
            footer_text(df)
        plt.tight_layout()
        plt.grid(alpha=.5)
    else:
        ax = fig.get_axes()[0]
        l, = ax.scatter(df.Obs, df.CMAQ, marker='o', edgecolors='w', alpha=.3, label=label)
        tt = linregress(df.Obs.values, df.CMAQ.values)
        ax.plot(df.Obs.unique(), tt[0] * df.Obs.unique() + tt[1], color=l.get_color())
        plt.legend(loc='Best')


def diffscatter_param(df, title=None, fig=None, label=None, footer=True):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    title : type
        Description of parameter `title` (the default is None).
    fig : type
        Description of parameter `fig` (the default is None).
    label : type
        Description of parameter `label` (the default is None).
    footer : type
        Description of parameter `footer` (the default is True).

    Returns
    -------
    type
        Description of returned object.

    """
    from scipy.stats import scoreatpercentile as score
    from numpy import isnan
    sns.set_style('ticks')
    df = df.dropna()
    mask = ~isnan(df.Obs.values) & ~isnan(df.CMAQ.values)
    if fig is None:
        species, units = df.Species.unique()[0], df.Units.unique()[0]
        maxval = score(df.Obs.values[mask], per=99.9)
        minvaly = score(df.CMAQ.values[mask] - df.Obs.values[mask], per=.1)
        maxvaly = score(df.CMAQ.values[mask] - df.Obs.values[mask], per=99.9)
        plt.figure(figsize=(10, 7))

        plt.scatter(df.Obs.values[mask], df.CMAQ.values[mask] - df.Obs.values[mask], c='cornflowerblue', marker='o',
                    edgecolors='w', alpha=.3, label=label)
        plt.plot((0, maxval), (0, 0), '--', color='darkslategrey')

        plt.xlim([0, maxval])
        plt.ylim([minvaly, maxvaly])
        plt.xlabel('Obs ' + species + ' (' + units + ')')
        plt.title(title)
        plt.gca().axes.set_ylabel('Model - Obs ' + species + ' (' + units + ')')
        if footer:
            footer_text(df)
        plt.tight_layout()
    else:
        ax = fig.get_axes()[0]
        mask = ~isnan(df.Obs.values) & ~isnan(df.CMAQ.values)
        ax.scatter(df.Obs.values[mask], df.CMAQ.values[mask] - df.Obs.values[mask], marker='o', edgecolors='w',
                   alpha=.3, label=label)
        plt.legend(loc='best')


def taylordiagram(df, marker='o', label='CMAQ', addon=False, dia=None):
    from numpy import corrcoef

    df = df.drop_duplicates().dropna(subset=['Obs', 'CMAQ'])

    if not addon and dia is None:
        f = plt.figure(figsize=(12, 10))
        sns.set_style('ticks')
        obsstd = df.Obs.std()

        dia = td.TaylorDiagram(obsstd, fig=f, rect=111, label='Obs')
        plt.grid(linewidth=1, alpha=.5)

        cc = corrcoef(df.Obs.values, df.CMAQ.values)[0, 1]
        dia.add_sample(df.CMAQ.std(), cc, marker=marker, zorder=9, ls=None, label=label)
        contours = dia.add_contours(colors='0.5')
        plt.clabel(contours, inline=1, fontsize=10)
        plt.grid(alpha=.5)
        plt.legend(fontsize='small', loc='best')
        plt.tight_layout()

    elif not addon and dia is not None:
        print 'Do you want to add this on? if so please turn the addon keyword to True'
    elif addon and dia is None:
        print 'Please pass the previous Taylor Diagram Instance with dia keyword...'
    else:
        cc = corrcoef(df.Obs.values, df.CMAQ.values)[0, 1]
        dia.add_sample(df.CMAQ.std(), cc, marker=marker, zorder=9, ls=None, label=label)
        plt.legend(fontsize='small', loc='best')
        plt.tight_layout()
    return dia
