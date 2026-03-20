import warnings
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from PIL import Image
from scipy.stats import scoreatpercentile as score

# Import taylordiagram module to make it available for documentation
from . import taylordiagram
from .colorbars import cmap_discretize, colorbar_index
from .mapgen import draw_map
from .plots import (
    create_taylor_diagram,  # Import with original name
    kdeplot,
    normval,
    scatter,
    spatial,
    spatial_bias_scatter,
    spatial_contourf,
    spatial_imshow,
    timeseries,
    wind_barbs,
    wind_quiver,
)

# Keep the function available under a clear name
taylordiagram_function = create_taylor_diagram  # Rename for public API

__all__ = (
    #
    "savefig",
    "sp_scatter_bias",
    #
    "cmap_discretize",
    "colorbar_index",
    #
    "mapgen",
    #
    "kdeplot",
    "spatial_contourf",
    "spatial_imshow",
    "normval",
    "scatter",
    "spatial",
    "spatial_bias_scatter",
    "taylordiagram",
    "timeseries",
    "wind_barbs",
    "wind_quiver",
)


# This is the driver for all verify objects


def _dynamic_fig_size(obj: xr.DataArray) -> tuple[float, float]:
    """Try to determine a generic figure size based on the shape of obj.

    Parameters
    ----------
    obj : xarray.DataArray
        A 2D DataArray with spatial dimensions.

    Returns
    -------
    tuple
        A tuple containing (width, height) for the figure size.

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> da = xr.DataArray(np.zeros((10, 20)), dims=['lat', 'lon'])
    >>> _dynamic_fig_size(da)
    (10, 5.0)
    """
    if "x" in obj.dims:
        nx, ny = len(obj.x), len(obj.y)
        scale = float(ny) / float(nx)
    elif "latitude" in obj.dims:
        nx, ny = len(obj.longitude), len(obj.latitude)
        scale = float(ny) / float(nx)
    elif "lat" in obj.dims:
        nx, ny = len(obj.lon), len(obj.lat)
        scale = float(ny) / float(nx)
    figsize = (10, 10 * scale)
    return figsize


def savefig(
    fname: str,
    *,
    fig: Figure | None = None,
    loc: int = 1,
    decorate: bool = True,
    logo: str | None = None,
    logo_height: float | int | None = None,
    **kwargs: Any,
) -> None:
    """Save figure and add logo.

    Parameters
    ----------
    fname : str
        Output file name or path. Passed to ``plt.savefig``.
        Must include desired file extension (``.jpg`` or ``.png``).
    fig : matplotlib.figure.Figure, optional
        The figure to save. If None, the current figure (plt.gcf()) is used.
    loc : int
        The location for the logo.

        * 1 -- bottom left (default)
        * 2 -- bottom right
        * 3 -- top right
        * 4 -- top left
    decorate : bool, default: True
        Whether to add the logo.
    logo : str, optional
        Path to the logo to be used.
        If not provided, the MONET logo is used.
    logo_height : float or int, optional
        Desired logo height in pixels.
        If not provided, the original logo image dimensions are used.
        Modify to scale the logo.
    **kwargs : dict
        Passed to the ``plt.savefig`` function.

    Returns
    -------
    None

    Notes
    -----
    This function replaces the previous ``pydecorate`` implementation with a direct
    ``Pillow`` implementation for greater flexibility and reduced dependencies.

    Examples
    --------
    >>> import matplotlib.pyplot as plt
    >>> plt.plot([1, 2], [3, 4])
    >>> savefig("plot.png", loc=1)
    """
    parts = fname.split(".")
    if not len(parts) > 1:
        raise ValueError("`fname` must include a file extension, e.g. '.png'")
    ext = fname.split(".")[-1]

    # Save current figure
    if fig is None:
        fig = plt.gcf()
    fig.savefig(fname, **kwargs)

    # Add logo
    if decorate:
        if logo is None:
            logo = Path(__file__).parent / "../data/MONET-logo.png"
        if ext.lower() not in {"png", "jpg", "jpeg"}:
            raise ValueError(f"only PNG and JPEG supported, but detected extension is {ext!r}")

        img = Image.open(fname)
        logo_img = Image.open(logo)

        # Resize logo if requested
        if logo_height is not None:
            aspect_ratio = logo_img.width / logo_img.height
            logo_width = int(logo_height * aspect_ratio)
            logo_img = logo_img.resize((logo_width, int(logo_height)), Image.Resampling.LANCZOS)

        # Calculate position
        # 1 -- bottom left (default)
        # 2 -- bottom right
        # 3 -- top right
        # 4 -- top left
        if loc == 1:
            pos = (0, img.height - logo_img.height)
        elif loc == 2:
            pos = (img.width - logo_img.width, img.height - logo_img.height)
        elif loc == 3:
            pos = (img.width - logo_img.width, 0)
        elif loc == 4:
            pos = (0, 0)
        else:
            raise ValueError(f"invalid `loc` {loc!r}")

        # Paste logo (using the logo itself as mask for transparency if it has alpha)
        mask = logo_img if logo_img.mode in ("RGBA", "LA", "P") else None
        img.paste(logo_img, pos, mask)

        # PIL.Image will determine format from the filename extension
        img.save(fname)

        img.close()
        logo_img.close()


def sp_scatter_bias(
    df: pd.DataFrame,
    col1: str | None = None,
    col2: str | None = None,
    ax: Axes | None = None,
    outline: bool = False,
    tight: bool = True,
    global_map: bool = True,
    map_kwargs: dict[str, Any] | None = None,
    cbar_kwargs: dict[str, Any] | None = None,
    val_max: float | None = None,
    val_min: float | None = None,
    **kwargs: Any,
) -> Axes:
    """Create a spatial scatter plot showing the bias (difference) between two columns in a DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing latitude, longitude, and data columns to compare.
    col1 : str
        Name of the first column (reference value).
    col2 : str
        Name of the second column (comparison value).
    ax : matplotlib.axes.Axes, optional
        Axes to plot on. If None, creates a new map using draw_map.
    outline : bool, default False
        Whether to show the map outline.
    tight : bool, default True
        Whether to apply tight_layout to the figure.
    global_map : bool, default True
        Whether to set global map boundaries (-180 to 180 longitude, -90 to 90 latitude).
    map_kwargs : dict, default {}
        Keyword arguments passed to draw_map if creating a new map.
    cbar_kwargs : dict, default {}
        Keyword arguments for colorbar customization.
    val_max : float, optional
        Maximum value for color scaling. If None, uses 95th percentile of absolute differences.
    val_min : float, optional
        Minimum value for color scaling (not currently used).
    **kwargs : dict
        Additional keyword arguments passed to DataFrame.plot.scatter.

    Returns
    -------
    matplotlib.axes.Axes
        The axes object containing the plot.

    Notes
    -----
    The point size is scaled by the magnitude of the difference between col2 and col1,
    making larger differences more visually prominent. Differences are capped at 300 units
    for display purposes.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({'latitude': [40, 41], 'longitude': [-70, -71], 'obs': [1, 2], 'mod': [1.1, 1.9]})
    >>> ax = sp_scatter_bias(df, col1='obs', col2='mod')
    """
    if map_kwargs is None:
        map_kwargs = {}
    if cbar_kwargs is None:
        cbar_kwargs = {}

    if ax is None:
        ax = draw_map(**map_kwargs)

    if col1 is None or col2 is None:
        raise ValueError("User must specify col1 and col2 in the dataframe")

    dfnew = df[["latitude", "longitude", col1, col2]].dropna().copy(deep=True)
    dfnew["sp_diff"] = dfnew[col2] - dfnew[col1]
    top = score(dfnew["sp_diff"].abs(), per=95)
    if val_max is not None:
        top = val_max
    # x, y = df.longitude.values, df.latitude.values
    dfnew["sp_diff_size"] = dfnew["sp_diff"].abs() / top * 100.0
    dfnew.loc[dfnew["sp_diff_size"] > 300, "sp_diff_size"] = 300.0
    dfnew.plot.scatter(
        x="longitude",
        y="latitude",
        c=dfnew["sp_diff"],
        s=dfnew["sp_diff_size"],
        vmin=-1 * top,
        vmax=top,
        ax=ax,
        colorbar=True,
        **kwargs,
    )
    if not outline:
        _set_outline_patch_alpha(ax)
    if global_map:
        plt.xlim([-180, 180])
        plt.ylim([-90, 90])
    if tight:
        plt.tight_layout(pad=0)
    return ax


def _set_outline_patch_alpha(ax: Axes, alpha: float = 0.0) -> None:
    """Set the transparency of map outline patches for Cartopy GeoAxes.

    This function attempts multiple methods to set the alpha (transparency) of
    map outlines when using Cartopy, handling different versions and configurations.

    Parameters
    ----------
    ax : matplotlib.axes.Axes or cartopy.mpl.geoaxes.GeoAxes
        The axes object whose outline transparency should be modified.
    alpha : float, default 0
        Alpha value between 0 (fully transparent) and 1 (fully opaque).

    Returns
    -------
    None

    Notes
    -----
    The function tries multiple approaches to accommodate different Cartopy versions
    and configurations. If all attempts fail, a warning is issued.

    Examples
    --------
    >>> import cartopy.crs as ccrs
    >>> import matplotlib.pyplot as plt
    >>> ax = plt.axes(projection=ccrs.PlateCarree())
    >>> _set_outline_patch_alpha(ax, alpha=0.5)
    """
    for f in [
        lambda alpha: ax.axes.outline_patch.set_alpha(alpha),
        lambda alpha: ax.outline_patch.set_alpha(alpha),
        lambda alpha: ax.spines["geo"].set_alpha(alpha),
    ]:
        try:
            f(alpha)
        except AttributeError:
            continue
        else:
            break
    else:
        warnings.warn("unable to set outline_patch alpha", stacklevel=2)
