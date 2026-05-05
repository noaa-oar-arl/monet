"""Cartopy-based plotting utilities for MONET."""

import typing as t

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

try:
    import cartopy.crs as ccrs
    from cartopy.mpl.geoaxes import GeoAxes
except ImportError:
    ccrs = None
    GeoAxes = None


def _get_plot_xy(da):
    """Detect latitude and longitude coordinate names for xarray plotting."""
    from ..accessors.base import BaseAccessor

    lat_name, lon_name = BaseAccessor._detect_latlon_names(da)
    return lon_name, lat_name


def plot_quick_imshow(
    da: xr.DataArray,
    map_kws: dict[str, t.Any] | None = None,
    projection: t.Any | None = None,
    colorbar: bool = True,
    figsize: tuple[float, float] | None = None,
    cmap: str | t.Any | None = None,
    vmin: float | None = None,
    vmax: float | None = None,
    norm: t.Any | None = None,
    dpi: int = 150,
    xlabel: str | None = None,
    ylabel: str | None = None,
    title: str | None = None,
    cbar_label: str | None = None,
    cbar_inset: bool = False,
    xticks: list[float] | None = None,
    yticks: list[float] | None = None,
    annotations: list[dict[str, t.Any]] | None = None,
    export_path: str | None = None,
    export_formats: list[str] | None = None,
    **kwargs: t.Any,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a quick imshow plot of the data on a map using Cartopy.

    Automatically detects spatial coordinates and applies standard map features.
    Convention-aware: works with CF/COARDS and UGRID datasets.

    Parameters
    ----------
    da : xarray.DataArray
        The 2D data array to plot.
    map_kws : dict, optional
        Dictionary of keyword arguments for map features.
        Keys can be 'coastlines', 'gridlines', 'land', 'ocean', 'borders', 'lakes', 'rivers', 'states'.
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to ccrs.PlateCarree().
    colorbar : bool, default: True
        Whether to add a colorbar to the plot.
    figsize : tuple, optional
        Figure size as (width, height) in inches.
    cmap : str or matplotlib.colors.Colormap, optional
        Colormap to use for the plot.
    vmin, vmax : float, optional
        Minimum and maximum values for the color scale.
    norm : matplotlib.colors.Normalize, optional
        Normalization object for mapping values to colors.
    dpi : int, default: 150
        Resolution of the figure in dots per inch.
    xlabel, ylabel, title : str, optional
        Labels for the x-axis, y-axis, and the plot title.
    cbar_label : str, optional
        Label for the colorbar.
    cbar_inset : bool, default: False
        If True, places the colorbar as an inset on the right side of the plot.
    xticks, yticks : list, optional
        Custom tick locations for the x and y axes.
    annotations : list of dict, optional
        List of dictionaries containing arguments for `ax.annotate`.
    export_path : str, optional
        Base file path to export the figure (without extension).
    export_formats : list of str, optional
        List of formats to export (e.g., ['png', 'pdf']). Defaults to ['png'].
    **kwargs : dict
        Additional keyword arguments passed directly to `da.plot.imshow`.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The created figure object.
    ax : matplotlib.axes.Axes
        The created axes object with geographic projection.

    Examples
    --------
    >>> fig, ax = da.monet.quick_imshow(cmap='viridis', title='Surface Temperature')
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi)
    plot_args = dict(cmap=cmap, vmin=vmin, vmax=vmax, norm=norm)
    plot_args.update({k: v for k, v in kwargs.items() if k not in ["ax", "transform", "x", "y"]})

    # Detect coordinates if not provided
    x_name, y_name = _get_plot_xy(da)
    if "x" not in kwargs and x_name:
        plot_args["x"] = x_name
    if "y" not in kwargs and y_name:
        plot_args["y"] = y_name

    mesh = da.plot.imshow(ax=ax, transform=ccrs.PlateCarree(), **plot_args)

    # Map features
    if GeoAxes is not None and isinstance(ax, GeoAxes):
        coast_kws = map_kws.get("coastlines", {})
        ax.coastlines(**coast_kws)
        grid_kws = map_kws.get(
            "gridlines",
            {
                "draw_labels": True,
                "linewidth": 0.5,
                "color": "gray",
                "alpha": 0.5,
                "linestyle": "--",
            },
        )
        gl = ax.gridlines(**grid_kws)
        if hasattr(gl, "top_labels"):
            gl.top_labels = False
        if hasattr(gl, "right_labels"):
            gl.right_labels = False
        for feature_name in ["land", "ocean", "borders", "lakes", "rivers", "states"]:
            if feature_name in map_kws:
                import cartopy.feature as cfeature

                feat = getattr(cfeature, feature_name.upper(), None)
                if feat is not None:
                    ax.add_feature(feat(), **map_kws[feature_name])
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    if xticks is not None:
        ax.set_xticks(xticks)
    if yticks is not None:
        ax.set_yticks(yticks)
    if annotations:
        for ann in annotations:
            ax.annotate(**ann)
    if colorbar:
        if cbar_inset:
            from mpl_toolkits.axes_grid1.inset_locator import inset_axes

            cax = inset_axes(
                ax,
                width="5%",
                height="80%",
                loc="lower left",
                bbox_to_anchor=(1.05, 0.1, 1, 1),
                bbox_transform=ax.transAxes,
                borderpad=0,
            )
            cbar = plt.colorbar(mesh, cax=cax, orientation="vertical")
        else:
            cbar = plt.colorbar(mesh, ax=ax, orientation="vertical", pad=0.02, aspect=30)
        if cbar_label:
            cbar.set_label(cbar_label)
    fig.tight_layout()
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax


def plot_quick_map(
    da: xr.DataArray,
    map_kws: dict[str, t.Any] | None = None,
    projection: t.Any | None = None,
    colorbar: bool = True,
    figsize: tuple[float, float] | None = None,
    cmap: str | t.Any | None = None,
    vmin: float | None = None,
    vmax: float | None = None,
    norm: t.Any | None = None,
    dpi: int = 150,
    xlabel: str | None = None,
    ylabel: str | None = None,
    title: str | None = None,
    cbar_label: str | None = None,
    cbar_inset: bool = False,
    xticks: list[float] | None = None,
    yticks: list[float] | None = None,
    annotations: list[dict[str, t.Any]] | None = None,
    export_path: str | None = None,
    export_formats: list[str] | None = None,
    **kwargs: t.Any,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a quick map plot of the data using Cartopy and xarray's default plot method.

    Parameters
    ----------
    da : xarray.DataArray
        The 2D data array to plot.
    map_kws : dict, optional
        Dictionary of keyword arguments for map features.
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to ccrs.PlateCarree().
    colorbar : bool, default: True
        Whether to add a colorbar.
    figsize : tuple, optional
        Figure size as (width, height) in inches.
    cmap : str or Colormap, optional
        Colormap to use.
    vmin, vmax : float, optional
        Color limits.
    norm : Normalize, optional
        Matplotlib normalization.
    dpi : int, default: 150
        Resolution of the figure.
    xlabel, ylabel, title : str, optional
        Axis labels and plot title.
    cbar_label : str, optional
        Label for the colorbar.
    cbar_inset : bool, default: False
        If True, places the colorbar as an inset.
    xticks, yticks : list, optional
        Custom tick locations.
    annotations : list of dict, optional
        List of dictionaries containing arguments for `ax.annotate`.
    export_path : str, optional
        Base file path to export the figure.
    export_formats : list of str, optional
        List of formats to export.
    **kwargs : dict
        Additional keyword arguments passed to `da.plot`.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The created figure object.
    ax : matplotlib.axes.Axes
        The created axes object.
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi)
    plot_args = dict(cmap=cmap, vmin=vmin, vmax=vmax, norm=norm)
    plot_args.update({k: v for k, v in kwargs.items() if k not in ["ax", "transform", "x", "y"]})

    x_name, y_name = _get_plot_xy(da)
    if "x" not in kwargs and x_name:
        plot_args["x"] = x_name
    if "y" not in kwargs and y_name:
        plot_args["y"] = y_name

    mesh = da.plot(ax=ax, transform=ccrs.PlateCarree(), **plot_args)

    if GeoAxes is not None and isinstance(ax, GeoAxes):
        coast_kws = map_kws.get("coastlines", {})
        ax.coastlines(**coast_kws)
        grid_kws = map_kws.get(
            "gridlines",
            {
                "draw_labels": True,
                "linewidth": 0.5,
                "color": "gray",
                "alpha": 0.5,
                "linestyle": "--",
            },
        )
        gl = ax.gridlines(**grid_kws)
        if hasattr(gl, "top_labels"):
            gl.top_labels = False
        if hasattr(gl, "right_labels"):
            gl.right_labels = False
        for feature_name in ["land", "ocean", "borders", "lakes", "rivers", "states"]:
            if feature_name in map_kws:
                import cartopy.feature as cfeature

                feat = getattr(cfeature, feature_name.upper(), None)
                if feat is not None:
                    ax.add_feature(feat(), **map_kws[feature_name])
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    if xticks is not None:
        ax.set_xticks(xticks)
    if yticks is not None:
        ax.set_yticks(yticks)
    if annotations:
        for ann in annotations:
            ax.annotate(**ann)
    if colorbar:
        if cbar_inset:
            from mpl_toolkits.axes_grid1.inset_locator import inset_axes

            cax = inset_axes(
                ax,
                width="5%",
                height="80%",
                loc="lower left",
                bbox_to_anchor=(1.05, 0.1, 1, 1),
                bbox_transform=ax.transAxes,
                borderpad=0,
            )
            cbar = plt.colorbar(mesh, cax=cax, orientation="vertical")
        else:
            cbar = plt.colorbar(mesh, ax=ax, orientation="vertical", pad=0.02, aspect=30)
        if cbar_label:
            cbar.set_label(cbar_label)
    fig.tight_layout()
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax


def plot_quick_contourf(
    da: xr.DataArray,
    map_kws: dict[str, t.Any] | None = None,
    projection: t.Any | None = None,
    colorbar: bool = True,
    figsize: tuple[float, float] | None = None,
    cmap: str | t.Any | None = None,
    vmin: float | None = None,
    vmax: float | None = None,
    norm: t.Any | None = None,
    dpi: int = 150,
    xlabel: str | None = None,
    ylabel: str | None = None,
    title: str | None = None,
    cbar_label: str | None = None,
    cbar_inset: bool = False,
    xticks: list[float] | None = None,
    yticks: list[float] | None = None,
    annotations: list[dict[str, t.Any]] | None = None,
    export_path: str | None = None,
    export_formats: list[str] | None = None,
    **kwargs: t.Any,
) -> tuple[plt.Figure, plt.Axes]:
    """Create a quick filled contour plot of the data on a map using Cartopy.

    Parameters
    ----------
    da : xarray.DataArray
        The 2D data array to plot.
    map_kws : dict, optional
        Dictionary of keyword arguments for map features.
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to ccrs.PlateCarree().
    colorbar : bool, default: True
        Whether to add a colorbar.
    figsize : tuple, optional
        Figure size as (width, height) in inches.
    cmap : str or Colormap, optional
        Colormap to use.
    vmin, vmax : float, optional
        Color limits.
    norm : Normalize, optional
        Matplotlib normalization.
    dpi : int, default: 150
        Resolution of the figure.
    xlabel, ylabel, title : str, optional
        Axis labels and plot title.
    cbar_label : str, optional
        Label for the colorbar.
    cbar_inset : bool, default: False
        If True, places the colorbar as an inset.
    xticks, yticks : list, optional
        Custom tick locations.
    annotations : list of dict, optional
        List of dictionaries containing arguments for `ax.annotate`.
    export_path : str, optional
        Base file path to export the figure.
    export_formats : list of str, optional
        List of formats to export.
    **kwargs : dict
        Additional keyword arguments passed to `da.plot.contourf`.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The created figure object.
    ax : matplotlib.axes.Axes
        The created axes object.
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi)
    plot_args = dict(cmap=cmap, vmin=vmin, vmax=vmax, norm=norm)
    plot_args.update({k: v for k, v in kwargs.items() if k not in ["ax", "transform", "x", "y"]})

    x_name, y_name = _get_plot_xy(da)
    if "x" not in kwargs and x_name:
        plot_args["x"] = x_name
    if "y" not in kwargs and y_name:
        plot_args["y"] = y_name

    mesh = da.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), **plot_args)

    if GeoAxes is not None and isinstance(ax, GeoAxes):
        coast_kws = map_kws.get("coastlines", {})
        ax.coastlines(**coast_kws)
        grid_kws = map_kws.get(
            "gridlines",
            {
                "draw_labels": True,
                "linewidth": 0.5,
                "color": "gray",
                "alpha": 0.5,
                "linestyle": "--",
            },
        )
        gl = ax.gridlines(**grid_kws)
        if hasattr(gl, "top_labels"):
            gl.top_labels = False
        if hasattr(gl, "right_labels"):
            gl.right_labels = False
        for feature_name in ["land", "ocean", "borders", "lakes", "rivers", "states"]:
            if feature_name in map_kws:
                import cartopy.feature as cfeature

                feat = getattr(cfeature, feature_name.upper(), None)
                if feat is not None:
                    ax.add_feature(feat(), **map_kws[feature_name])
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    if xticks is not None:
        ax.set_xticks(xticks)
    if yticks is not None:
        ax.set_yticks(yticks)
    if annotations:
        for ann in annotations:
            ax.annotate(**ann)
    if colorbar:
        if cbar_inset:
            from mpl_toolkits.axes_grid1.inset_locator import inset_axes

            cax = inset_axes(
                ax,
                width="5%",
                height="80%",
                loc="lower left",
                bbox_to_anchor=(1.05, 0.1, 1, 1),
                bbox_transform=ax.transAxes,
                borderpad=0,
            )
            cbar = plt.colorbar(mesh, cax=cax, orientation="vertical")
        else:
            cbar = plt.colorbar(mesh, ax=ax, orientation="vertical", pad=0.02, aspect=30)
        if cbar_label:
            cbar.set_label(cbar_label)
    fig.tight_layout()
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax


def facet_time_map(
    da: xr.DataArray,
    time_dim: str = "time",
    ncols: int = 3,
    map_kws: dict[str, t.Any] | None = None,
    projection: t.Any | None = None,
    colorbar: bool = True,
    figsize: tuple[float, float] | None = None,
    cmap: str | t.Any | None = None,
    vmin: float | None = None,
    vmax: float | None = None,
    norm: t.Any | None = None,
    dpi: int = 150,
    xlabel: str | None = None,
    ylabel: str | None = None,
    suptitle: str | None = None,
    cbar_label: str | None = None,
    xticks: list[float] | None = None,
    yticks: list[float] | None = None,
    annotations: list[dict[str, t.Any]] | None = None,
    export_path: str | None = None,
    export_formats: list[str] | None = None,
    **kwargs: t.Any,
) -> tuple[plt.Figure, np.ndarray]:
    """Create a facet grid of map plots for each time slice in a DataArray using Cartopy.

    Parameters
    ----------
    da : xarray.DataArray
        The data array with a time dimension to plot.
    time_dim : str, default: 'time'
        The name of the time dimension.
    ncols : int, default: 3
        The number of columns in the facet grid.
    map_kws : dict, optional
        Dictionary of keyword arguments for map features.
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to ccrs.PlateCarree().
    colorbar : bool, default: True
        Whether to add a shared colorbar to the facet grid.
    figsize : tuple, optional
        Figure size. If None, it is calculated based on ncols and nrows.
    cmap : str or Colormap, optional
        Colormap to use for all subplots.
    vmin, vmax : float, optional
        Color limits for the shared color scale.
    norm : Normalize, optional
        Matplotlib normalization.
    dpi : int, default: 150
        Resolution of the figure.
    xlabel, ylabel : str, optional
        Axis labels for the subplots.
    suptitle : str, optional
        Overall title for the figure.
    cbar_label : str, optional
        Label for the colorbar.
    xticks, yticks : list, optional
        Custom tick locations for the axes.
    annotations : list of dict, optional
        List of dictionaries for each subplot's `ax.annotate`.
    export_path : str, optional
        Base file path to export the figure.
    export_formats : list of str, optional
        List of formats to export.
    **kwargs : dict
        Additional keyword arguments passed to `dat.plot`.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The created figure object.
    axes : ndarray of matplotlib.axes.Axes
        The created axes objects.
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    times = da[time_dim].values
    nt = len(times)
    ncols = min(ncols, nt)
    nrows = int(np.ceil(nt / ncols))
    if figsize is None:
        figsize = (4 * ncols, 3.5 * nrows)
    fig, axes = plt.subplots(nrows, ncols, subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi)
    axes = np.atleast_1d(axes).flatten()
    plot_args = dict(cmap=cmap, vmin=vmin, vmax=vmax, norm=norm)
    plot_args.update({k: v for k, v in kwargs.items() if k not in ["ax", "transform", "x", "y"]})

    x_name, y_name = _get_plot_xy(da)
    if "x" not in kwargs and x_name:
        plot_args["x"] = x_name
    if "y" not in kwargs and y_name:
        plot_args["y"] = y_name

    mesh = None
    for i, time_val in enumerate(times):
        ax = axes[i]
        dat = da.sel({time_dim: time_val})
        mesh = dat.plot(ax=ax, transform=ccrs.PlateCarree(), add_colorbar=False, **plot_args)
        if GeoAxes is not None and isinstance(ax, GeoAxes):
            coast_kws = map_kws.get("coastlines", {})
            ax.coastlines(**coast_kws)
            grid_kws = map_kws.get(
                "gridlines",
                {
                    "draw_labels": False,
                    "linewidth": 0.5,
                    "color": "gray",
                    "alpha": 0.5,
                    "linestyle": "--",
                },
            )
            ax.gridlines(**grid_kws)
        if xlabel:
            ax.set_xlabel(xlabel)
        if ylabel:
            ax.set_ylabel(ylabel)
        ax.set_title(str(np.datetime_as_string(time_val)))
        if xticks is not None:
            ax.set_xticks(xticks)
        if yticks is not None:
            ax.set_yticks(yticks)
        if annotations and i < len(annotations):
            ax.annotate(**annotations[i])
    for j in range(nt, len(axes)):
        fig.delaxes(axes[j])
    if colorbar and mesh is not None:
        from mpl_toolkits.axes_grid1.inset_locator import inset_axes

        cax = inset_axes(
            axes[-1],
            width="5%",
            height="80%",
            loc="lower left",
            bbox_to_anchor=(1.05, 0.1, 1, 1),
            bbox_transform=axes[-1].transAxes,
            borderpad=0,
        )
        fig.colorbar(mesh, cax=cax, orientation="vertical", label=cbar_label)
    if suptitle:
        fig.suptitle(suptitle, fontsize=14, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 0.97, 1))
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, axes


def plot_points_map(
    df: pd.DataFrame,
    lon_col: str = "longitude",
    lat_col: str = "latitude",
    projection: t.Any | None = None,
    color: str | t.Any = "C0",
    marker: str = "o",
    size: float = 40,
    edgecolor: str | t.Any = "k",
    alpha: float = 0.8,
    map_kws: dict[str, t.Any] | None = None,
    figsize: tuple[float, float] = (8, 6),
    dpi: int = 150,
    title: str | None = None,
    export_path: str | None = None,
    export_formats: list[str] | None = None,
    **kwargs: t.Any,
) -> tuple[plt.Figure, plt.Axes]:
    """Plot points from a DataFrame on a Cartopy map.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing coordinates and values to plot.
    lon_col : str, default: 'longitude'
        The name of the longitude column.
    lat_col : str, default: 'latitude'
        The name of the latitude column.
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to PlateCarree.
    color : str or array-like, default: 'C0'
        Color of the points.
    marker : str, default: 'o'
        Marker style.
    size : float, default: 40
        Size of the markers.
    edgecolor : str or array-like, default: 'k'
        Edge color of the markers.
    alpha : float, default: 0.8
        Transparency level.
    map_kws : dict, optional
        Dictionary of map feature arguments.
    figsize : tuple, default: (8, 6)
        Figure size.
    dpi : int, default: 150
        Resolution.
    title : str, optional
        Plot title.
    export_path : str, optional
        Base file path to export.
    export_formats : list of str, optional
        Formats to export.
    **kwargs : dict
        Additional keyword arguments passed to `ax.scatter`.

    Returns
    -------
    fig : matplotlib.figure.Figure
    ax : matplotlib.axes.Axes
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi)
    if GeoAxes is not None and isinstance(ax, GeoAxes):
        coast_kws = map_kws.get("coastlines", {})
        ax.coastlines(**coast_kws)
        grid_kws = map_kws.get(
            "gridlines",
            {
                "draw_labels": True,
                "linewidth": 0.5,
                "color": "gray",
                "alpha": 0.5,
                "linestyle": "--",
            },
        )
        gl = ax.gridlines(**grid_kws)
        if hasattr(gl, "top_labels"):
            gl.top_labels = False
        if hasattr(gl, "right_labels"):
            gl.right_labels = False
        for feature_name in ["land", "ocean", "borders", "lakes", "rivers", "states"]:
            if feature_name in map_kws:
                import cartopy.feature as cfeature

                feat = getattr(cfeature, feature_name.upper(), None)
                if feat is not None:
                    ax.add_feature(feat(), **map_kws[feature_name])
    ax.scatter(
        df[lon_col],
        df[lat_col],
        color=color,
        marker=marker,
        s=size,
        edgecolor=edgecolor,
        alpha=alpha,
        transform=ccrs.PlateCarree(),
        **kwargs,
    )
    if title:
        ax.set_title(title)
    fig.tight_layout()
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax


def plot_lines_map(
    df: pd.DataFrame,
    lon_col: str = "longitude",
    lat_col: str = "latitude",
    group_col: str | None = None,
    projection: t.Any | None = None,
    color: str | t.Any = "C0",
    linewidth: float = 2,
    alpha: float = 0.8,
    map_kws: dict[str, t.Any] | None = None,
    figsize: tuple[float, float] = (8, 6),
    dpi: int = 150,
    title: str | None = None,
    export_path: str | None = None,
    export_formats: list[str] | None = None,
    **kwargs: t.Any,
) -> tuple[plt.Figure, plt.Axes]:
    """Plot lines from a DataFrame on a Cartopy map.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing coordinates.
    lon_col : str, default: 'longitude'
        Longitude column name.
    lat_col : str, default: 'latitude'
        Latitude column name.
    group_col : str, optional
        Column name to group by for drawing separate lines.
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use.
    color : str, default: 'C0'
        Line color.
    linewidth : float, default: 2
        Line width.
    alpha : float, default: 0.8
        Transparency level.
    map_kws : dict, optional
        Map feature arguments.
    figsize : tuple, default: (8, 6)
        Figure size.
    dpi : int, default: 150
        Resolution.
    title : str, optional
        Plot title.
    export_path : str, optional
        Export base path.
    export_formats : list, optional
        Export formats.
    **kwargs : dict
        Additional arguments passed to `ax.plot`.

    Returns
    -------
    fig : matplotlib.figure.Figure
    ax : matplotlib.axes.Axes
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi)
    if GeoAxes is not None and isinstance(ax, GeoAxes):
        coast_kws = map_kws.get("coastlines", {})
        ax.coastlines(**coast_kws)
        grid_kws = map_kws.get(
            "gridlines",
            {
                "draw_labels": True,
                "linewidth": 0.5,
                "color": "gray",
                "alpha": 0.5,
                "linestyle": "--",
            },
        )
        gl = ax.gridlines(**grid_kws)
        if hasattr(gl, "top_labels"):
            gl.top_labels = False
        if hasattr(gl, "right_labels"):
            gl.right_labels = False
        for feature_name in ["land", "ocean", "borders", "lakes", "rivers", "states"]:
            if feature_name in map_kws:
                import cartopy.feature as cfeature

                feat = getattr(cfeature, feature_name.upper(), None)
                if feat is not None:
                    ax.add_feature(feat(), **map_kws[feature_name])
    if group_col:
        for _, group in df.groupby(group_col):
            ax.plot(
                group[lon_col],
                group[lat_col],
                color=color,
                linewidth=linewidth,
                alpha=alpha,
                transform=ccrs.PlateCarree(),
                **kwargs,
            )
    else:
        ax.plot(
            df[lon_col],
            df[lat_col],
            color=color,
            linewidth=linewidth,
            alpha=alpha,
            transform=ccrs.PlateCarree(),
            **kwargs,
        )
    if title:
        ax.set_title(title)
    fig.tight_layout()
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax
