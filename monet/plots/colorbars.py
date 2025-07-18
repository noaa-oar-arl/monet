"""Colorbar helper functions"""

import matplotlib.pyplot as plt


def colorbar_index(ncolors, cmap, minval=None, maxval=None, dtype="int", basemap=None):
    """Create a colorbar with discrete colors and custom tick labels.

    Parameters
    ----------
    ncolors : int
        Number of discrete colors to use in the colorbar.
    cmap : str or matplotlib.colors.Colormap
        Colormap to discretize and use for the colorbar.
    minval : float, optional
        Minimum value for the colorbar tick labels. If None and maxval is None,
        tick labels will range from 0 to ncolors. If None and maxval is provided,
        tick labels will range from 0 to maxval.
    maxval : float, optional
        Maximum value for the colorbar tick labels. If None, tick labels
        will range from 0 or minval to ncolors.
    dtype : str or type, default "int"
        Data type for tick label values (e.g., "int", "float").
    basemap : matplotlib.mpl_toolkits.basemap.Basemap, optional
        Basemap instance to attach the colorbar to. If None, uses plt.colorbar.

    Returns
    -------
    tuple
        (colorbar, discretized_cmap) where:
        - colorbar is the matplotlib.colorbar.Colorbar instance
        - discretized_cmap is the discretized colormap
    """
    import matplotlib.cm as cm
    import numpy as np

    cmap = cmap_discretize(cmap, ncolors)
    mappable = cm.ScalarMappable(cmap=cmap)
    mappable.set_array([])
    mappable.set_clim(-0.5, ncolors + 0.5)
    if basemap is not None:
        colorbar = basemap.colorbar(mappable, format="%1.2g")
    else:
        colorbar = plt.colorbar(mappable, format="%1.2g", fontsize=12)
    colorbar.set_ticks(np.linspace(0, ncolors, ncolors))
    if (minval is None) & (maxval is not None):
        colorbar.set_ticklabels(np.around(np.linspace(0, maxval, ncolors).astype(dtype), 2))
    elif (minval is None) & (maxval is None):
        colorbar.set_ticklabels(np.around(np.linspace(0, ncolors, ncolors).astype(dtype), 2))
    else:
        colorbar.set_ticklabels(np.around(np.linspace(minval, maxval, ncolors).astype(dtype), 2))

    return colorbar, cmap


def cmap_discretize(cmap, N):
    """Return a discrete colormap from a continuous colormap.

    Creates a new colormap by discretizing an existing continuous colormap
    into N distinct colors while preserving the color transitions.

    Parameters
    ----------
    cmap : str or matplotlib.colors.Colormap
        Colormap instance or registered colormap name to discretize.
        Example: cm.jet, 'viridis', etc.
    N : int
        Number of discrete colors to use in the new colormap.

    Returns
    -------
    matplotlib.colors.LinearSegmentedColormap
        A new colormap object with N discrete colors based on the input colormap.
        The name will be the original colormap name with "_N" appended.

    Examples
    --------
    >>> from numpy import arange
    >>> from numpy.ma import resize
    >>> from matplotlib.pyplot import imshow
    >>> from matplotlib.cm import jet
    >>> x = resize(arange(100), (5, 100))
    >>> djet = cmap_discretize(jet, 5)
    >>> imshow(x, cmap=djet)
    """
    import matplotlib.colors as mcolors
    import numpy as np

    if isinstance(cmap, str):
        cmap = plt.get_cmap(cmap)
    colors_i = np.concatenate((np.linspace(0, 1.0, N), (0.0, 0.0, 0.0, 0.0)))
    colors_rgba = cmap(colors_i)
    indices = np.linspace(0, 1.0, N + 1)
    cdict = {}
    for ki, key in enumerate(("red", "green", "blue")):
        cdict[key] = [
            (indices[i], colors_rgba[i - 1, ki], colors_rgba[i, ki]) for i in range(N + 1)
        ]
    # Return colormap object.
    return mcolors.LinearSegmentedColormap(cmap.name + "_%d" % N, cdict, 1024)


# def o3cmap():
#     import matplotlib.cm as cm
#     # This function returns the colormap and bins for the ozone spatial plots
#     # this is designed to have a vmin =0 and vmax = 140
#     # return cmap,bins
#     colors1 = cm.viridis(linspace(0, 1, 128))
#     colors2 = cm.OrRd(linspace(.2, 1, 128))
#     colors = vstack((colors1, colors2))
#     return mcolors.LinearSegmentedColormap.from_list('o3cmap', colors), arange(
#         0, 140.5, .5)
#
#
# def pm25cmap():
#     from matplotlib.cm import viridis, OrRd
#     # This function returns the colormap and bins for the PM spatial plots
#     # this is designed to have a vmin =0 and vmax = 140
#     # return cmap,bins
#     colors1 = viridis(linspace(0, 1, 128))
#     colors2 = OrRd(linspace(.2, 1, 128))
#     colors = vstack((colors1, colors2))
#     cc = mcolors.LinearSegmentedColormap.from_list('pm25cmap', colors), arange(
#         0, 70.2, .2)
#     return cc
#
#
# def wscmap():
#     from matplotlib.cm import viridis, OrRd
#     # This function returns the colormap and bins for the PM spatial plots
#     # this is designed to have a vmin =0 and vmax = 140
#     # return cmap,bins
#     colors1 = viridis(linspace(0, 1, 128))
#     colors2 = OrRd(linspace(.2, 1, 128))
#     colors = vstack((colors1, colors2))
#     return mcolors.LinearSegmentedColormap.from_list('wscmap', colors), arange(
#         0, 40.2, .2)
#
#
# def tempcmap():
#     from matplotlib.cm import viridis, OrRd
#     # This function returns the colormap and bins for the PM spatial plots
#     # this is designed to have a vmin =0 and vmax = 140
#     # return cmap,bins
#     colors1 = viridis(linspace(0, 1, 128))
#     colors2 = OrRd(linspace(.2, 1, 128))
#     colors = vstack((colors1, colors2))
#     return mcolors.LinearSegmentedColormap.from_list('tempcmap',
#                                                      colors), arange(
#                                                          250, 320.5, .5)
#
#
# def sradcmap():
#     from matplotlib.cm import viridis, plasma_r
#     # This function returns the colormap and bins for the PM spatial plots
#     # this is designed to have a vmin =0 and vmax = 140
#     # return cmap,bins
#     colors1 = viridis(linspace(0, 1, 128))
#     colors2 = plasma_r(linspace(.2, 1, 128))
#     colors = vstack((colors1, colors2))
#     return mcolors.LinearSegmentedColormap.from_list('sradcmap',
#                                                      colors), arange(
#                                                          0, 1410., 10)
#
#
# def noxcmap():
#     """Short summary.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     from matplotlib.cm import viridis, plasma_r
#     # This function returns the colormap and bins for the NO2/NO/NOx spatial plots
#     # this is designed to have a vmin =0 and vmax = 140
#     # return cmap,bins
#     colors1 = viridis(linspace(0, 1, 128))
#     colors2 = plasma_r(linspace(.042, .75, 128))
#     colors = vstack((colors1, colors2))
#     return mcolors.LinearSegmentedColormap.from_list('noxcmap',
#                                                      colors), arange(
#                                                          0, 40.2, .2)
#
#
# def rhcmap():
#     """Short summary.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     from matplotlib.cm import viridis, plasma_r
#     # This function returns the colormap and bins for the NO2/NO/NOx spatial
#     # plots
#     # this is designed to have a vmin =0 and vmax = 140
#     # return cmap,bins
#     colors1 = viridis(linspace(0, 1, 128))
#     colors2 = plasma_r(linspace(.042, .75, 128))
#     colors = vstack((colors1, colors2))
#     return mcolors.LinearSegmentedColormap.from_list('noxcmap',
#                                                      colors), arange(
#                                                          0, 100.5, .5)
#
#
# def so2cmap():
#     """Short summary.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     from matplotlib.cm import viridis, plasma_r
#     colors1 = viridis(linspace(0, 1, 128))
#     colors2 = plasma_r(linspace(.042, .75, 128))
#     colors = vstack((colors1, colors2))
#     return mcolors.LinearSegmentedColormap.from_list('noxcmap',
#                                                      colors), arange(
#                                                          0, 14.1, .1)
#
#
# def pm10cmap():
#     import matplotlib.cm as cm
#     # This function returns the colormap and bins for the NO2/NO/NOx spatial plots
#     # this is designed to have a vmin =0 and vmax = 140
#     # return cmap,bins
#     colors1 = cm.viridis(linspace(0, 1, 128))
#     colors2 = cm.plasma_r(linspace(.042, .75, 128))
#     colors = vstack((colors1, colors2))
#     return mcolors.LinearSegmentedColormap.from_list('noxcmap',
#                                                      colors), arange(
#                                                          0, 150.5, .5)
