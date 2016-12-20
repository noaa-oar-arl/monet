import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from matplotlib import cm
from numpy import vstack, arange, linspace


def colorbar_index(ncolors, cmap, minval=None, maxval=None):
    import matplotlib.cm as cm
    import numpy as np
    cmap = cmap_discretize(cmap, ncolors)
    mappable = cm.ScalarMappable(cmap=cmap)
    mappable.set_array([])
    mappable.set_clim(-0.5, ncolors + 0.5)
    colorbar = plt.colorbar(mappable, format='%1.2g')
    colorbar.set_ticks(np.linspace(0, ncolors, ncolors))
    if (type(minval) == None) & (type(maxval) != None):
        colorbar.set_ticklabels(np.around(np.linspace(0, maxval, ncolors).astype('float'), 2))
    elif (type(minval) == None) & (type(maxval) == None):
        colorbar.set_ticklabels(np.around(np.linspace(0, ncolors, ncolors).astype('float'), 2))
    else:
        colorbar.set_ticklabels(np.around(np.linspace(minval, maxval, ncolors).astype('float'), 2))

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


def o3cmap():
    # This function returns the colormap and bins for the ozone spatial plots
    # this is designed to have a vmin =0 and vmax = 140
    # return cmap,bins
    colors1 = cm.viridis(linspace(0, 1, 128))
    colors2 = cm.OrRd(linspace(.2, 1, 128))
    colors = vstack((colors1, colors2))
    return mcolors.LinearSegmentedColormap.from_list('o3cmap', colors), arange(0, 140.5, .5)


def pm25cmap():
    # This function returns the colormap and bins for the PM spatial plots
    # this is designed to have a vmin =0 and vmax = 140
    # return cmap,bins
    colors1 = cm.viridis(linspace(0, 1, 128))
    colors2 = cm.OrRd(linspace(.2, 1, 128))
    colors = vstack((colors1, colors2))
    return mcolors.LinearSegmentedColormap.from_list('pm25cmap', colors), arange(0, 70.2, .2)


def noxcmap():
    # This function returns the colormap and bins for the NO2/NO/NOx spatial plots
    # this is designed to have a vmin =0 and vmax = 140
    # return cmap,bins
    colors1 = cm.viridis(linspace(0, 1, 128))
    colors2 = cm.plasma_r(linspace(.042, .75, 128))
    colors = vstack((colors1, colors2))
    return mcolors.LinearSegmentedColormap.from_list('noxcmap', colors), arange(0, 70.2, .2)


def so2cmap():
    # This function returns the colormap and bins for the NO2/NO/NOx spatial plots
    # this is designed to have a vmin =0 and vmax = 140
    # return cmap,bins
    colors1 = cm.viridis(linspace(0, 1, 128))
    colors2 = cm.plasma_r(linspace(.042, .75, 128))
    colors = vstack((colors1, colors2))
    return mcolors.LinearSegmentedColormap.from_list('noxcmap', colors), arange(0, 75.2, .2)


def pm10cmap():
    # This function returns the colormap and bins for the NO2/NO/NOx spatial plots
    # this is designed to have a vmin =0 and vmax = 140
    # return cmap,bins
    colors1 = cm.viridis(linspace(0, 1, 128))
    colors2 = cm.plasma_r(linspace(.042, .75, 128))
    colors = vstack((colors1, colors2))
    return mcolors.LinearSegmentedColormap.from_list('noxcmap', colors), arange(0, 150.5, .5)
