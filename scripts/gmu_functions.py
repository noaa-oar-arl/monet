#!/user/bin/python
import matplotlib.pyplot as plt
from numpy import *


def readfile(file):
    from netCDF4 import MFDataset
    return MFDataset(file)


def make_spatial_plot(cmaqvar, x, y, date, m, dpi=None, savename='', levs=arange(0, 100, 10), cmap='YlGnBu'):
    fig = plt.figure(figsize=(12, 6), frameon=False)
    # define map and draw boundries
    m.drawstates()
    m.drawcoastlines(linewidth=.3)
    m.drawcountries()
    plt.axis('off')
    ncolors = len(levs)
    c, cmap = colorbar_index(ncolors, cmap, levs)
    m.pcolormesh(x, y, cmaqvar, vmin=vmin, vmax=vmax, cmap=cmap)
    titstring = date
    plt.title(titstring)

    plt.tight_layout()
    if savename != '':
        plt.savefig(savename + date.strftime('%Y%m%d_%H.jpg'), dpi=dpi)
        plt.close()
    return c


def colorbar_index(ncolors, cmap, levels):
    import matplotlib.cm as cm
    cmap = cmap_discretize(cmap, ncolors)
    mappable = cm.ScalarMappable(cmap=cmap)
    mappable.set_array([])
    mappable.set_clim(-0.5, ncolors + 0.5)
    colorbar = plt.colorbar(mappable, format='%1.2g')
    colorbar.set_ticks(levels)

    return colorbar, cmap


def cmap_discretize(cmap, N):
    """
    Return a discrete colormap from the continuous colormap cmap.
    cmap: colormap instance, eg. cm.jet. 
    N: number of colors.
    Example
        x = resize(arange(100), (5,100))
        djet = cmap_discretize(cm.jet, 5)
        imshow(x, cmap=djet)
    """
    import matplotlib.colors as mcolors
    import numpy as np

    if type(cmap) == str:
        cmap = plt.get_cmap(cmap)
    colors_i = np.concatenate((np.linspace(0, 1., N), (0., 0., 0., 0.)))
    colors_rgba = cmap(colors_i)
    indices = np.linspace(0, 1., N + 1)
    cdict = {}
    for ki, key in enumerate(('red', 'green', 'blue')):
        cdict[key] = [(indices[i], colors_rgba[i - 1, ki], colors_rgba[i, ki])
                      for i in xrange(N + 1)]
    # Return colormap object.
    return mcolors.LinearSegmentedColormap(cmap.name + "_%d" % N, cdict, 1024)


def load_conus_basemap(grdobj):
    from mpl_toolkits.basemap import Basemap
    latitude = gridobj.variables['LAT'][:][0, 0, :, :].squeeze()
    longitude = gridobj.variables['LON'][:][0, 0, :, :].squeeze()
    lat1 = gridobj.P_ALP
    lat2 = gridobj.P_BET
    lon1 = gridobj.P_GAM
    lon0 = gridobj.XCENT
    lat0 = gridobj.YCENT
    m = Basemap(projection='laea', resolution='h', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                llcrnrlat=latitude[0, 0], urcrnrlat=latitude[-1, -1], llcrnrlon=longitude[0, 0],
                urcrnrlon=longitude[-1, -1], rsphere=6371200.,
                area_thresh=50.)
    x, y = m(longitude, latitude)
    return m, x, y


def average_8hr(xx, dates):
    return xx


def get_dates(concobj):
    from datetime import datetime
    from pandas import DataFrame

    from numpy import array
    tflag1 = array(concobj.variables['TFLAG'][:, 0, 0], dtype='|S7')
    tflag2 = array(concobj.variables['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
    date = []
    for i, j in zip(tflag1, tflag2):
        date.append(datetime.strptime(i + j, '%Y%j%H'))
    dates = array(date)
    r = DataFrame(dates, columns=['dates'])
    rr = r.drop_duplicates(keep='last')
    indexdates = rr.index.values
    utc = pytz.timezone('UTC')
    est = pytz.timezone('US/Eastern')
    dates = [utc.localize(i) for i in dates]
    est_dates = array([i.astimezone(est) for i in dates])
    return est_dates[indexdates], indexdates


def main():
    from netCDF4 import Dataset, MFDataset
    from numpy import unique, array
    concfiles = ['temp/20161114/aqm.t12z.aconc.ncf']
    grid = Dataset('MAY2014/aqm.t12z.grdcro2d.ncf')

    concobj = MFDataset(concfiles)
    d, index = get_dates(concobj)
    jds = array([int(i.strftime('%j')) for i in d])
    ujds = unique(jds)[1, 2]

    m, x, y = load_conus_basemap(grid)
    # get ozone
    o3 = concobj.variables['O3'][index, 0, :, :].squeeze() * 1000.
    # to make an image for each time loop through
    for i, j in enumerate(d):
        date = j.strftime('%m/%d/%Y %H')
        make_spatial_plot(o3[i, :, :], x, y, date, m, levs=arange(0, 100, 10), cmap='viridis_r'):
        plt.savefig(j.strftime('%Y%m%d%H_o3.jpg'), dpi=75)

    concobj = MFDataset(['temp/20161114/aqm.t12z.aconc.ncf'])
    pm25 = concobj.variables['PM25'][index, 0, :, :].squeeze()
    # to make an image for each time loop through
    for i, j in enumerate(d):
        date = j.strftime('%m/%d/%Y %H')
        make_spatial_plot(o3[i, :, :], x, y, date, m, levs=arange(0, 70, 10), cmap='viridis_r'):
        plt.savefig(j.strftime('%Y%m%d%H_pm25.jpg'), dpi=75)


def __init__():
    main()
