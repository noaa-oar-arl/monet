"""plotting routines"""
import matplotlib.pyplot as plt
import seaborn as sns

from . import taylordiagram as td
from .colorbars import colorbar_index

# colors = ['#1e90ff','#045C5C','#00A847','#DB4291','#BB7E5D']
colors = ["#1e90ff", "#DA70D6", "#228B22", "#FA8072", "#FF1493"]
sns.set_palette(sns.color_palette(colors))

sns.set_context("poster")


# CMAQ Spatial Plots
def make_spatial_plot(modelvar, m, dpi=None, plotargs={}, ncolors=15, discrete=False):
    f, ax = plt.subplots(1, 1, figsize=(11, 6), frameon=False)
    # determine colorbar
    if "cmap" not in plotargs:
        plotargs["cmap"] = "viridis"
    if discrete and "vmin" in plotargs and "vmax" in plotargs:
        c, cmap = colorbar_index(
            ncolors, plotargs["cmap"], minval=plotargs["vmin"], maxval=plotargs["vmax"], basemap=m
        )
        plotargs["cmap"] = cmap
        m.imshow(modelvar, **plotargs)
        vmin, vmax = plotargs["vmin"], plotargs["vmax"]
    elif discrete:
        temp = m.imshow(modelvar, **plotargs)
        vmin, vmax = temp.get_clim()
        c, cmap = colorbar_index(ncolors, plotargs["cmap"], minval=vmin, maxval=vmax, basemap=m)
        plotargs["cmap"] = cmap
        m.imshow(modelvar, vmin=vmin, vmax=vmax, **plotargs)
    else:
        temp = m.imshow(modelvar, **plotargs)
        c = m.colorbar()
        vmin, vmax = temp.get_clim()
        cmap = plotargs["cmap"]
    # draw borders
    m.drawstates()
    m.drawcoastlines(linewidth=0.3)
    m.drawcountries()
    return f, ax, c, cmap, vmin, vmax


def spatial(modelvar, **kwargs):
    if kwargs["ax"] is None:
        f, ax = plt.subplots(1, 1, figsize=(11, 6), frameon=False)
        kwargs["ax"] = ax
    ax = modelvar.plot(**kwargs)
    return ax


def make_spatial_contours(
    modelvar,
    gridobj,
    date,
    m,
    dpi=None,
    savename="",
    discrete=True,
    ncolors=None,
    dtype="int",
    **kwargs
):
    plt.figure(figsize=(11, 6), frameon=False)
    lat = gridobj.variables["LAT"][0, 0, :, :].squeeze()
    lon = gridobj.variables["LON"][0, 0, :, :].squeeze()
    # define map and draw boundaries
    m.drawstates()
    m.drawcoastlines(linewidth=0.3)
    m.drawcountries()
    x, y = m(lon, lat)
    plt.axis("off")
    m.contourf(x, y, modelvar, **kwargs)
    cmap = kwargs["cmap"]
    levels = kwargs["levels"]
    if discrete:
        c, cmap = colorbar_index(
            ncolors, cmap, minval=levels[0], maxval=levels[-1], basemap=m, dtype=dtype
        )
    else:
        c = m.colorbar()
    titstring = date.strftime("%B %d %Y %H")
    plt.title(titstring)

    plt.tight_layout()
    if savename != "":
        plt.savefig(savename + date.strftime("%Y%m%d_%H.jpg"), dpi=dpi)
        plt.close()
    return c


def wind_quiver(ws, wdir, gridobj, m, **kwargs):
    from . import tools

    lat = gridobj.variables["LAT"][0, 0, :, :].squeeze()
    lon = gridobj.variables["LON"][0, 0, :, :].squeeze()
    # define map and draw boundaries
    x, y = m(lon, lat)
    u, v = tools.wsdir2uv(ws, wdir)
    quiv = m.quiver(x[::15, ::15], y[::15, ::15], u[::15, ::15], v[::15, ::15], **kwargs)
    return quiv


def wind_barbs(ws, wdir, gridobj, m, **kwargs):
    import tools

    lat = gridobj.variables["LAT"][0, 0, :, :].squeeze()
    lon = gridobj.variables["LON"][0, 0, :, :].squeeze()
    # define map and draw boundaries
    x, y = m(lon, lat)
    u, v = tools.wsdir2uv(ws, wdir)
    m.barbs(x[::15, ::15], y[::15, ::15], u[::15, ::15], v[::15, ::15], **kwargs)


def normval(vmin, vmax, cmap):
    from matplotlib.colors import BoundaryNorm
    from numpy import arange

    bounds = arange(vmin, vmax + 5.0, 5.0)
    norm = BoundaryNorm(boundaries=bounds, ncolors=cmap.N)
    return norm


# def spatial_scatter(df, m, discrete=False, plotargs={}, create_cbar=True):
#     from .colorbars import cmap_discretize
#     x, y = m(df.longitude.values, df.Latitude.values)
#     s = 20
#     if create_cbar:
#         if discrete:
#             cmap = cmap_discretize(cmap, ncolors)
#             # s = 20
#           if (type(plotargs(vmin)) == None) | (type(plotargs(vmax)) == None):
#                 plt.scatter(x, y, c=df['Obs'].values, **plotargs)
#             else:
#                 plt.scatter(x, y, c=df['Obs'].values, **plotargs)
#         else:
#             plt.scatter(x, y, c=df['Obs'].values, **plotargs)
#     else:
#         plt.scatter(x, y, c=df['Obs'].values, **plotargs)

# def spatial_stat_scatter(df,
#                          m,
#                          date,
#                          stat=mystats.MB,
#                          ncolors=15,
#                          fact=1.5,
#                          cmap='RdYlBu_r'):
#     new = df[df.datetime == date]
#     x, y = m(new.longitude.values, new.latitude.values)
#     cmap = cmap_discretize(cmap, ncolors)
#     colors = new.CMAQ - new.Obs
#     ss = (new.Obs - new.CMAQ).abs() * fact


def spatial_bias_scatter(
    df, m, date, vmin=None, vmax=None, savename="", ncolors=15, fact=1.5, cmap="RdBu_r"
):
    from numpy import around
    from scipy.stats import scoreatpercentile as score

    #    plt.figure(figsize=(11, 6), frameon=False)
    f, ax = plt.subplots(figsize=(11, 6), frameon=False)
    ax.set_facecolor("white")
    diff = df.CMAQ - df.Obs
    top = around(score(diff.abs(), per=95))
    new = df[df.datetime == date]
    x, y = m(new.longitude.values, new.latitude.values)
    c, cmap = colorbar_index(ncolors, cmap, minval=top * -1, maxval=top, basemap=m)

    c.ax.tick_params(labelsize=13)
    #    cmap = cmap_discretize(cmap, ncolors)
    colors = new.CMAQ - new.Obs
    ss = (new.CMAQ - new.Obs).abs() / top * 100.0
    ss[ss > 300] = 300.0
    plt.scatter(
        x,
        y,
        c=colors,
        s=ss,
        vmin=-1.0 * top,
        vmax=top,
        cmap=cmap,
        edgecolors="k",
        linewidths=0.25,
        alpha=0.7,
    )

    if savename != "":
        plt.savefig(savename + date + ".jpg", dpi=75.0)
        plt.close()
    return f, ax, c


# def eight_hr_spatial_scatter(df, m, date, savename=''):
#     fig = plt.figure(figsize=(11, 6), frameon=False)
#     m.drawcoastlines(linewidth=.3)
#     m.drawstates()
#     m.drawcountries()
#
#     plt.axis('off')
#     new = df[df.datetime_local == date]
#     x, y = m(new.longitude.values, new.latitude.values)
#     cmap = plt.cm.get_cmap('plasma')
#     norm = normval(-40, 40., cmap)
#     ss = (new.Obs - new.CMAQ).abs() / top * 100.
#     colors = new.Obs - new.CMAQ
#     m.scatter(x, y, s=ss, c=colors, norm=norm, cmap=cmap)
#     if savename != '':
#         plt.savefig(savename + date + '.jpg', dpi=75.)
#         plt.close()


def timeseries(
    df,
    x="time",
    y="obs",
    ax=None,
    plotargs={},
    fillargs={"alpha": 0.2},
    title="",
    ylabel=None,
    label=None,
):
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
    if ax is None:
        f, ax = plt.subplots(figsize=(11, 6), frameon=False)

    sns.set_style("ticks")
    df.index = df[x]
    m = df.groupby("time").mean()  # mean values for each sample time period
    e = df.groupby("time").std()  # std values for each sample time period
    variable = df.variable[0]
    if df.columns.isin(["units"]).max():
        unit = df.units[0]
    else:
        unit = "None"
    upper = m[y] + e[y]
    lower = m[y] - e[y]
    lower.loc[lower < 0] = 0
    lower = lower.values
    if "alpha" not in fillargs:
        fillargs["alpha"] = 0.2
    if label is not None:
        m.rename(columns={y: label}, inplace=True)
    else:
        label = y
    m[label].plot(ax=ax, **plotargs)
    ax.fill_between(m[label].index, lower, upper, **fillargs)
    if ylabel is None:
        ax.set_ylabel(variable + " (" + unit + ")")
    else:
        ax.set_ylabel(label)
    ax.set_xlabel("")
    plt.legend()
    plt.title(title)
    plt.tight_layout()
    return ax


def kdeplot(df, title=None, label=None, ax=None, **kwargs):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    col : type
        Description of parameter `col` (the default is 'obs').
    title : type
        Description of parameter `title` (the default is None).
    label : type
        Description of parameter `label` (the default is None).
    ax : type
        Description of parameter `ax` (the default is ax).
    **kwargs : type
        Description of parameter `**kwargs`.

    Returns
    -------
    type
        Description of returned object.

    """
    sns.set_style("ticks")

    if ax is None:
        f, ax = plt.subplots(figsize=(11, 6), frameon=False)
        sns.despine()

    ax = sns.kdeplot(df, ax=ax, label=label, **kwargs)
    return ax


def scatter(df, x=None, y=None, title=None, label=None, ax=None, **kwargs):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    x : type
        Description of parameter `x` (the default is 'obs').
    y : type
        Description of parameter `y` (the default is 'model').
    **kwargs : type
        Description of parameter `**kwargs`.

    Returns
    -------
    type
        Description of returned object.

    """
    sns.set_style("ticks")

    if ax is None:
        f, ax = plt.subplots(figsize=(8, 6), frameon=False)
    ax = sns.regplot(data=df, x=x, y=y, label=label, **kwargs)
    plt.title(title)
    return ax


def taylordiagram(
    df,
    marker="o",
    col1="obs",
    col2="model",
    label1="OBS",
    label2="MODEL",
    scale=1.5,
    addon=False,
    dia=None,
):
    from numpy import corrcoef

    df = df.drop_duplicates().dropna(subset=[col1, col2])

    if not addon and dia is None:
        f = plt.figure(figsize=(12, 10))
        sns.set_style("ticks")
        obsstd = df[col1].std()

        dia = td.TaylorDiagram(obsstd, scale=scale, fig=f, rect=111, label=label1)
        plt.grid(linewidth=1, alpha=0.5)
        cc = corrcoef(df[col1].values, df[col2].values)[0, 1]
        dia.add_sample(df[col2].std(), cc, marker=marker, zorder=9, ls=None, label=label2)
        contours = dia.add_contours(colors="0.5")
        plt.clabel(contours, inline=1, fontsize=10)
        plt.grid(alpha=0.5)
        plt.legend(fontsize="small", loc="best")

    elif not addon and dia is not None:
        print("Do you want to add this on? if so please turn the addon keyword to True")
    elif addon and dia is None:
        print("Please pass the previous Taylor Diagram Instance with dia keyword...")
    else:
        cc = corrcoef(df.Obs.values, df.CMAQ.values)[0, 1]
        dia.add_sample(df.CMAQ.std(), cc, marker=marker, zorder=9, ls=None, label=label1)
        plt.legend(fontsize="small", loc="best")
        plt.tight_layout()
    return dia
