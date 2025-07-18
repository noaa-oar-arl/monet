"""Map utilities."""

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt


def draw_map(
    *,
    crs=None,
    natural_earth=False,
    coastlines=True,
    states=False,
    counties=False,
    countries=True,
    resolution="10m",
    extent=None,
    figsize=(10, 5),
    linewidth=0.25,
    return_fig=False,
    **kwargs,
):
    """Draw a map with Cartopy.

    Creates a map using Cartopy with configurable features like coastlines,
    borders, and natural earth elements. This function simplifies the process
    of creating maps for spatial visualization.

    Parameters
    ----------
    crs : cartopy.crs.Projection
        The map projection.
        If set, this takes precedence over the possible ``kwargs['subplot_kw']['projection']``.
        If unset (``None``), defaults to ``ccrs.PlateCarree()``.
    natural_earth : bool
        Add the Cartopy Natural Earth ocean, land, lakes, and rivers features.
    coastlines : bool
        Add coastlines (`linewidth` applied).
    states : bool
        Add states/provinces (`linewidth` applied).
    counties : bool
        Add US counties (`linewidth` applied).
    countries : bool
        Add country borders (`linewidth` applied).
    resolution : {'10m', '50m', '110m'}
        The resolution of the Natural Earth features for coastlines, states, and counties.
        Higher resolution (e.g., '10m') provides more detail but may be slower to render.
    extent : array-like
        Set the map extent with ``[lon_min,lon_max,lat_min,lat_max]``.
    figsize : tuple
        Figure size (width, height), passed to :func:`plt.subplots() <matplotlib.pyplot.subplots>`.
        This takes precedence over the possible ``kwargs['figsize']``.
    linewidth : float
        Line width for coastlines, states, counties, and countries.
    return_fig : bool
        Return the figure and axes objects.
        By default (``False``), just the axes object is returned.
    **kwargs
        Arguments to pass to :func:`plt.subplots() <matplotlib.pyplot.subplots>`.

    Returns
    -------
    matplotlib.axes.Axes or tuple
        By default, returns just the ``ax`` (:class:`cartopy.mpl.geoaxes.GeoAxes` instance).
        If `return_fig` is true, returns a tuple of ``(fig, ax)`` where ``fig`` is the
        matplotlib Figure instance and ``ax`` is the GeoAxes instance.

    Notes
    -----
    The '10m' resolution provides the most detailed features but can be slow for
    large or global maps. '50m' is a good compromise for regional maps, while '110m'
    works well for global views.

    Examples
    --------
    >>> # Create a simple map with coastlines
    >>> ax = draw_map(coastlines=True, resolution='50m')

    >>> # Create a detailed US map with states and counties
    >>> ax = draw_map(
    ...     crs=ccrs.AlbersEqualArea(central_longitude=-95),
    ...     states=True,
    ...     counties=True,
    ...     resolution='10m',
    ...     extent=[-125, -65, 25, 50]
    ... )
    """
    kwargs["figsize"] = figsize
    if "subplot_kw" not in kwargs:
        kwargs["subplot_kw"] = {}
    if crs is not None:
        kwargs["subplot_kw"]["projection"] = crs
    else:
        if "projection" not in kwargs["subplot_kw"]:
            kwargs["subplot_kw"]["projection"] = ccrs.PlateCarree()

    fig, ax = plt.subplots(**kwargs)

    if natural_earth:
        ax.add_feature(cfeature.OCEAN)
        ax.add_feature(cfeature.LAND)
        ax.add_feature(cfeature.LAKES)
        ax.add_feature(cfeature.RIVERS)

    if states:
        states_provinces = cfeature.NaturalEarthFeature(
            category="cultural",
            name="admin_1_states_provinces_lines",
            scale=resolution,
            facecolor="none",
            edgecolor="k",
        )

    if counties:
        counties = cfeature.NaturalEarthFeature(
            category="cultural",
            name="admin_2_counties",
            scale=resolution,
            facecolor="none",
            edgecolor="k",
        )

    if coastlines:
        ax.coastlines(resolution, linewidth=linewidth)

    if countries:
        ax.add_feature(cfeature.BORDERS, linewidth=linewidth)

    if states:
        ax.add_feature(states_provinces, linewidth=linewidth)

    if counties:
        ax.add_feature(counties, linewidth=linewidth)

    if extent is not None:
        ax.set_extent(extent)

    if return_fig:
        return fig, ax
    else:
        return ax
