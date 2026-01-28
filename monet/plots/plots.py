"""plotting routines"""

import functools
import typing as t
import warnings

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import xarray as xr

from . import taylordiagram as td

# colors = ['#1e90ff','#045C5C','#00A847','#DB4291','#BB7E5D']
colors = ["#1e90ff", "#DA70D6", "#228B22", "#FA8072", "#FF1493"]


def _default_sns_context(f):
    """Decorator to apply a default seaborn context and color palette to plotting functions."""

    @functools.wraps(f)
    def inner(*args, **kwargs):
        with sns.plotting_context("poster"), sns.color_palette(colors):
            return f(*args, **kwargs)

    return inner


def _savefig(fig, *, save_name=None, dpi=None, **kwargs):
    """Save figure."""
    if save_name is not None:
        fig.savefig(save_name, dpi=dpi, **kwargs)
        plt.close(fig)


def _create_map(fig=None, ax=None, **kwargs):
    """Create a map projection."""
    if ax is None:
        fig, ax = plt.subplots(
            figsize=(11, 8),
            subplot_kw={"projection": ccrs.LambertConformal(central_longitude=-97.5, central_latitude=38.5)},
            **kwargs,
        )
    else:
        if fig is None:
            fig = ax.figure
    return fig, ax


@_default_sns_context
def spatial_plot(
    da: xr.DataArray,
    fig: plt.Figure | None = None,
    ax: plt.Axes | None = None,
    **kwargs,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a spatial plot from an xarray.DataArray.

    Parameters
    ----------
    da : xarray.DataArray
        The data to plot.
    fig : matplotlib.figure.Figure, optional
        Figure to plot on.
    ax : matplotlib.axes.Axes, optional
        Axes to plot on.
    **kwargs
        Additional keyword arguments to pass to xarray's plot method.

    Returns
    -------
    t.Tuple[plt.Figure, plt.Axes]
        The figure and axes containing the plot.
    """
    fig, ax = _create_map(fig=fig, ax=ax)
    da.plot(ax=ax, transform=ccrs.PlateCarree(), **kwargs)
    return fig, ax


# Spatial Plots
@_default_sns_context
def spatial_imshow(
    da: xr.DataArray,
    ax: plt.Axes | None = None,
    **kwargs,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a spatial plot from an xarray.DataArray using imshow.

    Parameters
    ----------
    da : xarray.DataArray
        The data to plot.
    ax : matplotlib.axes.Axes, optional
        Axes to plot on. If None, a new figure and axes will be created.
    **kwargs
        Additional keyword arguments to pass to xarray's plot.imshow() method.

    Returns
    -------
    t.Tuple[plt.Figure, plt.Axes]
        The figure and axes containing the plot.
    """
    fig, ax = _create_map(ax=ax)
    da.plot.imshow(ax=ax, transform=ccrs.PlateCarree(), **kwargs)
    ax.coastlines()
    ax.gridlines()
    return fig, ax


@_default_sns_context
def spatial(
    da: xr.DataArray,
    fig: plt.Figure | None = None,
    ax: plt.Axes | None = None,
    **kwargs,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a spatial plot from an xarray.DataArray.

    A convenience wrapper for xarray's plot method with consistent styling.

    .. deprecated:: 24.8.1
        This function is deprecated and will be removed in a future version.
        Please use `spatial_plot` and add map features like coastlines
        and gridlines manually for more control.

    Parameters
    ----------
    da : xr.DataArray
        The data to plot spatially.
    fig : matplotlib.figure.Figure, optional
        Figure to plot on.
    ax : plt.Axes, optional
        Axes to plot on. If None, a new figure and axes will be created.
    **kwargs
        Additional keyword arguments passed to xarray's plot method.

    Returns
    -------
    t.Tuple[plt.Figure, plt.Axes]
        The figure and axes containing the plot.
    """
    warnings.warn(
        "The function `spatial` is deprecated and will be removed in a future version. Please use `spatial_plot` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    fig, ax = spatial_plot(da, fig=fig, ax=ax, **kwargs)
    ax.coastlines()
    ax.gridlines()
    return fig, ax


@_default_sns_context
def spatial_contourf(
    da: xr.DataArray,
    ax: plt.Axes | None = None,
    **kwargs,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a spatial plot from an xarray.DataArray using contourf.

    Parameters
    ----------
    da : xarray.DataArray
        The data to plot.
    ax : matplotlib.axes.Axes, optional
        Axes to plot on. If None, a new figure and axes will be created.
    **kwargs
        Additional keyword arguments to pass to xarray's plot.contourf() method.

    Returns
    -------
    t.Tuple[plt.Figure, plt.Axes]
        The figure and axes containing the plot.
    """
    fig, ax = _create_map(ax=ax)
    da.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), **kwargs)
    ax.coastlines()
    ax.gridlines()
    return fig, ax


def _thin_data(u: xr.DataArray, v: xr.DataArray, thin: int = 15) -> tuple[xr.DataArray, xr.DataArray, np.ndarray, np.ndarray]:
    """Thin the data for wind plotting.

    Parameters
    ----------
    u : xr.DataArray
        u-component of wind.
    v : xr.DataArray
        v-component of wind.
    thin : int, optional
        The thinning factor for the wind vectors. Default is 15.
    Returns
    -------
    t.Tuple[xr.DataArray, xr.DataArray, np.ndarray, np.ndarray]
        Thinned u, v, and meshgrid x and y coordinates.
    """
    # Programmatically find the spatial dimension names
    if "lat" in u.coords and "lon" in u.coords:
        y_dim, x_dim = "lat", "lon"
    elif "y" in u.coords and "x" in u.coords:
        y_dim, x_dim = "y", "x"
    else:
        # Fallback to the last two dimensions, assuming (..., y, x) order
        y_dim, x_dim = u.dims[-2:]

    thinner = {y_dim: slice(None, None, thin), x_dim: slice(None, None, thin)}
    u_thinned = u.isel(**thinner)
    v_thinned = v.isel(**thinner)

    x2d, y2d = np.meshgrid(u_thinned[x_dim], u_thinned[y_dim])

    return u_thinned, v_thinned, x2d, y2d


@_default_sns_context
def wind_quiver(
    u: xr.DataArray,
    v: xr.DataArray,
    ax: plt.Axes = None,
    thin: int = 15,
    **kwargs,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a quiver plot of wind vectors on a map.

    Parameters
    ----------
    u : xr.DataArray
        2D array of u-component of wind.
    v : xr.DataArray
        2D array of v-component of wind.
    ax : plt.Axes, optional
        Axes to plot on.
    thin : int, optional
        The thinning factor for the wind vectors. Default is 15.
    **kwargs
        Additional arguments to pass to quiver. Common options include
        'scale', 'scale_units', and 'width'.

    Returns
    -------
    t.Tuple[plt.Figure, plt.Axes]
        The figure and axes objects.
    """
    if ax is None:
        fig, ax = _create_map(ax=ax)
    else:
        fig = ax.figure

    u_thinned, v_thinned, x2d, y2d = _thin_data(u, v, thin)

    # define map and draw boundaries
    ax.quiver(
        x2d,
        y2d,
        u_thinned.values,
        v_thinned.values,
        transform=ccrs.PlateCarree(),
        **kwargs,
    )
    return fig, ax


@_default_sns_context
def wind_barbs(
    u: xr.DataArray,
    v: xr.DataArray,
    ax: plt.Axes = None,
    thin: int = 15,
    **kwargs,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a barbs plot of wind on a map.

    Parameters
    ----------
    u : xr.DataArray
        2D array of u-component of wind.
    v : xr.DataArray
        2D array of v-component of wind.
    ax : plt.Axes, optional
        Axes to plot on.
    thin : int, optional
        The thinning factor for the wind vectors. Default is 15.
    **kwargs
        Additional arguments to pass to barbs. Common options include
        'length', 'pivot', 'barb_increments'.

    Returns
    -------
    t.Tuple[plt.Figure, plt.Axes]
        The figure and axes objects.
    """
    if ax is None:
        fig, ax = _create_map(ax=ax)
    else:
        fig = ax.figure

    u_thinned, v_thinned, x2d, y2d = _thin_data(u, v, thin)

    # define map and draw boundaries
    ax.barbs(
        x2d,
        y2d,
        u_thinned.values,
        v_thinned.values,
        transform=ccrs.PlateCarree(),
        **kwargs,
    )
    return fig, ax


def normval(vmin, vmax, cmap):
    """Create a BoundaryNorm for discrete colormaps with specific bounds.

    Parameters
    ----------
    vmin : float
        Minimum value for the colormap.
    vmax : float
        Maximum value for the colormap.
    cmap : matplotlib.colors.Colormap
        The colormap to create bounds for.

    Returns
    -------
    matplotlib.colors.BoundaryNorm
        A boundary norm with evenly spaced bounds from vmin to vmax in steps of 5.0.
    """
    from matplotlib.colors import BoundaryNorm
    from numpy import arange

    bounds = arange(vmin, vmax + 5.0, 5.0)
    norm = BoundaryNorm(boundaries=bounds, ncolors=cmap.N)
    return norm


@_default_sns_context
def spatial_bias_scatter(
    ds: xr.Dataset,
    *,
    vmin: float | None = None,
    vmax: float | None = None,
    savename: str = "",
    cmap: str = "RdBu_r",
    fig: plt.Figure | None = None,
    ax: plt.Axes | None = None,
    **kwargs,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a scatter plot showing bias on a map.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset containing 'obs' and 'model' variables, and 'latitude' and
        'longitude' coordinates.
    vmin : float, optional
        Minimum value for colorscale. If None, automatically determined.
    vmax : float, optional
        Maximum value for colorscale. If None, automatically determined.
    savename : str, default ""
        If provided, save the figure to this path.
    cmap : str or matplotlib.colors.Colormap, default "RdBu_r"
        Colormap to use for bias values.
    fig : matplotlib.figure.Figure, optional
        Figure to plot on.
    ax : matplotlib.axes.Axes, optional
        Axes to plot on.
    **kwargs
        Additional keyword arguments to pass to `xarray.plot.scatter`.

    Returns
    -------
    t.Tuple[plt.Figure, plt.Axes]
        The figure and axes containing the plot.

    Notes
    -----
    The scatter points are colored by the difference (model - obs) and sized
    by the absolute magnitude of this difference, making larger biases more visible.
    """
    fig, ax = _create_map(fig=fig, ax=ax)
    ax.set_facecolor("white")

    # Create a new dataset for plotting to avoid modifying the original
    plot_ds = ds.copy(deep=False)
    plot_ds["difference"] = ds["model"] - ds["obs"]

    # Calculate size based on absolute difference.
    # A zero size is invisible, so we add a minimum size and scale.
    # The scaling factor is arbitrary and can be adjusted for better visualization.
    size = np.abs(plot_ds["difference"])
    # Avoid division by zero if all differences are zero
    if size.max() > 0:
        size = (size / size.max()) * 200 + 20
    else:
        size = xr.full_like(size, 20)

    plot_ds.plot.scatter(
        ax=ax,
        x="longitude",
        y="latitude",
        hue="difference",
        s=size,
        vmin=vmin,
        vmax=vmax,
        cmap=cmap,
        edgecolors="k",
        linewidths=0.25,
        alpha=0.7,
        transform=ccrs.PlateCarree(),
        **kwargs,
    )

    _savefig(fig, save_name=savename)
    return fig, ax


@_default_sns_context
def timeseries(
    df: "pd.DataFrame",
    x: str = "time",
    y: str = "obs",
    ax: plt.Axes | None = None,
    plotargs: dict[str, t.Any] | None = None,
    fillargs: dict[str, t.Any] | None = None,
    title: str = "",
    ylabel: str | None = None,
    label: str | None = None,
) -> plt.Axes:
    """Create a timeseries plot with shaded error bounds.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the data to plot.
    x : str, default "time"
        Column name to use for the x-axis (time).
    y : str, default "obs"
        Column name to use for the y-axis (values to plot).
    ax : plt.Axes, optional
        Axes to plot on. If None, creates a new figure and axes.
    plotargs : dict, optional
        Additional arguments to pass to DataFrame.plot().
    fillargs : dict, optional
        Additional arguments to pass to fill_between for the error shading.
        Defaults to `{"alpha": 0.2}`.
    title : str, default ""
        Title for the plot.
    ylabel : str, optional
        Y-axis label. If None, uses variable name and units from DataFrame.
    label : str, optional
        Label for the plotted line (for legend). If None, uses `y`.

    Returns
    -------
    plt.Axes
        The axes containing the plot.

    Notes
    -----
    This function groups the data by time, plots the mean values, and adds
    shading for ±1 standard deviation around the mean.
    """

    if plotargs is None:
        plotargs = {}
    if fillargs is None:
        fillargs = {"alpha": 0.2}

    with sns.axes_style("ticks"):
        if ax is None:
            _, ax = plt.subplots(figsize=(11, 6), frameon=False)

        # Group by the specified time column
        grouped = df.groupby(x)
        m = grouped.mean(numeric_only=True)
        e = grouped.std(numeric_only=True)

        variable = df["variable"].iloc[0] if "variable" in df.columns else ""
        unit = df["units"].iloc[0] if "units" in df.columns else "None"

        upper = m[y] + e[y]
        lower = m[y] - e[y]
        lower.loc[lower < 0] = 0

        plot_label = label if label is not None else y
        m = m.rename(columns={y: plot_label})

        m[plot_label].plot(ax=ax, **plotargs)
        ax.fill_between(m.index, lower.values, upper.values, **fillargs)

        if ylabel is None:
            ax.set_ylabel(f"{variable} ({unit})")
        else:
            ax.set_ylabel(ylabel)

        ax.set_xlabel("")
        ax.legend()
        ax.set_title(title)
        plt.tight_layout()

    return ax


@_default_sns_context
def kdeplot(df, title=None, label=None, ax=None, **kwargs):
    """Create a kernel density estimate plot.

    Parameters
    ----------
    df : pandas.Series or array-like
        Data to plot the distribution of.
    title : str, optional
        Title for the plot.
    label : str, optional
        Label for the plotted line (for legend).
    ax : matplotlib.axes.Axes, optional
        Axes to plot on. If None, creates a new figure and axes.
    **kwargs
        Additional arguments passed to seaborn's kdeplot.
        Common options include 'shade', 'bw', and 'color'.

    Returns
    -------
    matplotlib.axes.Axes
        The axes containing the plot.
    """
    with sns.axes_style("ticks"):
        if ax is None:
            f, ax = plt.subplots(figsize=(11, 6), frameon=False)
            sns.despine()
        ax = sns.kdeplot(df, ax=ax, label=label, **kwargs)

    return ax


@_default_sns_context
def scatter(df, x=None, y=None, title=None, label=None, ax=None, **kwargs):
    """Create a scatter plot with regression line.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the data to plot.
    x : str, optional
        Column name for x-axis values.
    y : str, optional
        Column name for y-axis values.
    title : str, optional
        Title for the plot.
    label : str, optional
        Label for the plot (for legend).
    ax : matplotlib.axes.Axes, optional
        Axes to plot on. If None, creates a new figure and axes.
    **kwargs
        Additional arguments passed to seaborn's regplot.
        Common options include 'scatter_kws', 'line_kws', and 'ci'.

    Returns
    -------
    matplotlib.axes.Axes
        The axes containing the plot.
    """
    with sns.axes_style("ticks"):
        if ax is None:
            f, ax = plt.subplots(figsize=(8, 6), frameon=False)
        ax = sns.regplot(data=df, x=x, y=y, label=label, **kwargs)
        plt.title(title)

    return ax


@_default_sns_context
def create_taylor_diagram(
    obs: "pd.Series",
    model: "pd.Series",
    model_label: str = "Model",
    obs_label: str = "Reference",
    scale: float = 1.5,
    dia: td.TaylorDiagram | None = None,
    **kwargs,
) -> td.TaylorDiagram:
    """Create a Taylor diagram from observation and model data.

    This function provides a simplified interface for creating Taylor
    diagrams to compare model output with a reference (observation) dataset.
    It can either create a new diagram or add a new model series to an
    existing diagram.

    Parameters
    ----------
    obs : pd.Series
        Time series of observations (reference data).
    model : pd.Series
        Time series of model predictions.
    model_label : str, default "Model"
        Label for the model data point in the diagram.
    obs_label : str, default "Reference"
        Label for the reference data point on the standard deviation axis.
    scale : float, default 1.5
        The radial limit of the diagram, specified as a multiple of the
        observation's standard deviation.
    dia : TaylorDiagram, optional
        If provided, add the model sample to this existing TaylorDiagram
        instance instead of creating a new one.
    **kwargs
        Additional keyword arguments passed to `TaylorDiagram.add_sample()`.
        Common options include `marker`, `color`, `ls`, and `zorder`.

    Returns
    -------
    td.TaylorDiagram
        The TaylorDiagram instance containing the plot.

    Examples
    --------
    Create a simple Taylor diagram comparing one model to observations:

    >>> import pandas as pd
    >>> import numpy as np
    >>> obs = pd.Series(np.random.rand(100), name="obs")
    >>> mod = pd.Series(np.random.rand(100), name="mod")
    >>> dia = create_taylor_diagram(obs, mod, model_label="MyModel")
    >>> plt.show()

    Add a second model to the same diagram:

    >>> mod2 = pd.Series(np.random.rand(100) * 1.2, name="mod2")
    >>> dia = create_taylor_diagram(obs, mod2, model_label="MyModel2", dia=dia)
    >>> plt.show()
    """
    # Ensure data is clean and aligned
    df = pd.DataFrame({"obs": obs, "model": model}).dropna()
    obs_clean = df["obs"]
    model_clean = df["model"]

    if dia is None:
        # Create a new diagram
        with sns.axes_style("ticks"):
            fig = plt.figure(figsize=(12, 10))
            obs_std = obs_clean.std()
            dia = td.TaylorDiagram(obs_std, scale=scale, fig=fig, rect=111, label=obs_label)
            plt.grid(linewidth=1, alpha=0.5)
            contours = dia.add_contours(colors="0.5")
            plt.clabel(contours, inline=1, fontsize=10)
            plt.grid(alpha=0.5)
    else:
        # Use the existing diagram, but ensure its reference is compatible
        if not np.isclose(dia.refstd, obs_clean.std()):
            warnings.warn(
                "The reference standard deviation of the provided diagram "
                "differs from the new observation data. "
                "Statistics will be based on the diagram's original reference.",
                UserWarning,
                stacklevel=2,
            )

    # Calculate correlation and add the model sample to the diagram
    corr = np.corrcoef(obs_clean.values, model_clean.values)[0, 1]
    model_std = model_clean.std()

    dia.add_sample(model_std, corr, label=model_label, **kwargs)

    # Finalize plot details
    dia.ax.legend(fontsize="small", loc="best")
    plt.tight_layout()

    return dia
