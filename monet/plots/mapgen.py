""" map utilities """
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt


def draw_map(ax=None,
             crs=None,
             natural_earth=False,
             coastlines=True,
             states=False,
             countries=True,
             resolution='10m',
             extent=None,
             **kwargs):
    """Short summary.

    Parameters
    ----------
    ax : type
        Description of parameter `ax` (the default is None).
    natural_earth : bool
        Description of parameter `natural_earth` (the default is True).
    coastlines : bool
        Description of parameter `coastlines` (the default is True).
    states : bool
        Description of parameter `states` (the default is True).
    countries : bool
        Description of parameter `countries` (the default is True).
    state_resolutions : bool
        Description of parameter `state_resolutions` (the default is '10m').
    extent : [lon_min,lon_max,lat_min,lat_max]
        Description of parameter `extent` (the default is None).

    Returns
    -------
    type
        Description of returned object.

    """
    con2 = 'subplot_kw' in kwargs and 'projection' not in kwargs['subplot_kw']
    if ax is None and kwargs is not None and crs is None:
        if 'subplot_kw' not in kwargs:
            kwargs['subplot_kw'] = {'projection': ccrs.PlateCarree()}
        elif con2:
            kwargs['subplot_kw']['projection'] = ccrs.PlateCarree()
        f, ax = plt.subplots(figsize=(10, 5), **kwargs)
    elif ax is None and crs is not None:
        f, ax = plt.subplots(figsize=(10, 5), subplot_kw={'projection': crs})
    else:
        f, ax = plt.subplots(
            figsize=(10, 5), subplot_kw={'projection': ccrs.PlateCarree()})
    if natural_earth:
        # ax.stock_img()
        ax.add_feature(cfeature.OCEAN)
        ax.add_feature(cfeature.LAND)
        ax.add_feature(cfeature.LAKES)
        ax.add_feature(cfeature.RIVERS)

    if states:
        states_provinces = cfeature.NaturalEarthFeature(
            category='cultural',
            name='admin_1_states_provinces_lines',
            scale=resolution,
            facecolor='none')
        ax.add_feature(states_provinces, edgecolor='black')

    if coastlines:
        ax.coastlines(resolution)

    if countries:
        ax.add_feature(cfeature.BORDERS)

    if states:
        ax.add_feature(states_provinces)

    if extent is not None:
        ax.set_extent(extent)

    return ax
