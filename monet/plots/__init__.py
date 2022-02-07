from __future__ import absolute_import, print_function
from multiprocessing.sharedctypes import Value

from . import colorbars, plots, taylordiagram
from .colorbars import *
from .mapgen import *
from .plots import *

__all__ = ["colorbars", "plots", "taylordiagram", "mapgen"]

# This is the driver for all verify objects


def _dynamic_fig_size(obj):
    """Try to determine a generic figure size based on the shape of obj

    Parameters
    ----------
    obj : A 2D xarray DataArray
        Description of parameter `obj`.

    Returns
    -------
    type
        Description of returned object.

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


def savefig(fname, *, loc=1, decorate=True, logo_height=None, **kwargs):
    """Save figure and add the MONET logo.

    Parameters
    ----------
    fname : str
        Output file name or path. Passed to ``plt.savefig``.
        Must include desired file extension (``.jpg`` or ``.png``).
    loc : int
        The location for the MONET logo.

        * 1 -- bottom left (default)
        * 2 -- bottom right
        * 3 -- top right
        * 4 -- top left
    decorate : bool
        Whether to add the logo.
    logo_height : float or int, optional
        Desired logo height in pixels.
        If not provided, the original logo image dimensions are used.
        Modify to scale the logo.
    **kwargs : dict
        Passed to the ``plt.savefig`` function.

    Returns
    -------
    None
    """
    from pathlib import Path

    import matplotlib.pyplot as plt
    from PIL import Image
    from pydecorate import DecoratorAGG

    data = Path(__file__).parent / "../data"

    # Save current figure
    plt.savefig(fname, **kwargs)

    # Add logo
    if decorate:
        img = Image.open(fname)
        dc = DecoratorAGG(img)  # cursor starts top-left
        dc_add_logo_kwargs = {}
        if logo_height is not None:
            dc_add_logo_kwargs["height"] = logo_height
        if loc == 1:
            dc.align_bottom()
        elif loc == 2:
            dc.align_bottom()
            dc.align_right()
        elif loc == 3:
            dc.align_right()
        elif loc == 4:
            dc.align_left()
        else:
            raise ValueError(f"invalid `loc` {loc!r}")
        logo = data / "MONET-logo.png"
        dc.add_logo(logo, **dc_add_logo_kwargs)

        ext = fname.split(".")[-1]
        if ext.lower() == "png":
            img.save(fname, "PNG")
        elif ext.lower() in {"jpg", "jpeg"}:
            img.save(fname, "JPEG")
        else:
            raise ValueError(f"only PNG and JPEG supported, but detected extension is {ext!r}")

        img.close()


def sp_scatter_bias(
    df, col1=None, col2=None, ax=None, outline=False, tight=True, global_map=True, map_kwargs={}, cbar_kwargs={}, val_max=None, val_min=None, **kwargs
):
    from scipy.stats import scoreatpercentile as score
    from numpy import around

    if ax is None:
        ax = draw_map(**map_kwargs)
    try:
        if col1 is None or col2 is None:
            print("User must specify col1 and col2 in the dataframe")
            raise ValueError
        else:
            dfnew = df[["latitude", "longitude", col1, col2]].dropna().copy(deep=True)
            dfnew["sp_diff"] = dfnew[col2] - dfnew[col1]
            top = score(dfnew["sp_diff"].abs(), per=95)
            if val_max is not None:
                top = val_max
            x, y = df.longitude.values, df.latitude.values
            dfnew["sp_diff_size"] = dfnew["sp_diff"].abs() / top * 100.0
            dfnew.loc[dfnew["sp_diff_size"] > 300, "sp_diff_size"] = 300.0
            dfnew.plot.scatter(
                x="longitude", y="latitude", c=dfnew["sp_diff"], s=dfnew["sp_diff_size"], vmin=-1 * top, vmax=top, ax=ax, colorbar=True, **kwargs
            )
            if ~outline:
                ax.outline_patch.set_alpha(0)
            if global_map:
                plt.xlim([-180, 180])
                plt.ylim([-90, 90])
            if tight:
                plt.tight_layout(pad=0)
            return ax
    except ValueError:
        exit
