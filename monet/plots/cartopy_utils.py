"""Cartopy-based plotting utilities for MONET."""

import matplotlib.pyplot as plt
import numpy as np

try:
    import cartopy.crs as ccrs
    from cartopy.mpl.geoaxes import GeoAxes
except ImportError:
    ccrs = None
    GeoAxes = None


def plot_quick_imshow(
    da,
    map_kws=None,
    projection=None,
    colorbar=True,
    figsize=None,
    cmap=None,
    vmin=None,
    vmax=None,
    norm=None,
    dpi=150,
    xlabel=None,
    ylabel=None,
    title=None,
    cbar_label=None,
    cbar_inset=False,
    xticks=None,
    yticks=None,
    annotations=None,
    export_path=None,
    export_formats=None,
    **kwargs,
):
    """
    Create a imshow plot of the data on a map using Cartopy.

    Parameters
    ----------
    da : xarray.DataArray
        The data to plot.
    map_kws : dict, optional
        Dictionary of keyword arguments for map features (e.g., coastlines, gridlines, features, borders, land, ocean).
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to PlateCarree.
    colorbar : bool, default: True
        Whether to add a colorbar.
    figsize : tuple, optional
        Figure size.
    cmap : str or Colormap, optional
        Colormap to use (supports colorblind-friendly options).
    vmin, vmax : float, optional
        Color limits.
    norm : Normalize, optional
        Matplotlib normalization (e.g., LogNorm).
    dpi : int, optional
        Dots per inch for export.
    xlabel, ylabel, title : str, optional
        Axis labels and plot title.
    cbar_label : str, optional
        Label for the colorbar.
    cbar_inset : bool, default: False
        Place colorbar as an inset (right) if True.
    xticks, yticks : list, optional
        Custom tick locations.
    annotations : list of dict, optional
        List of annotation dicts (e.g., {"text": "A", "xy": (lon, lat)}).
    export_path : str, optional
        Path to export the figure (without extension).
    export_formats : list, optional
        List of formats to export (e.g., ["png", "pdf"]).
    **kwargs : dict
        Additional keyword arguments for imshow.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The matplotlib figure object.
    ax : matplotlib.axes.Axes
        The matplotlib axes object.
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(
        subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi
    )
    plot_args = dict(cmap=cmap, vmin=vmin, vmax=vmax, norm=norm)
    # Remove 'ax' and 'transform' from kwargs to avoid multiple values error
    plot_args.update({k: v for k, v in kwargs.items() if k not in ["ax", "transform"]})
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
        # Extra features
        for feature_name in ["land", "ocean", "borders", "lakes", "rivers", "states"]:
            if feature_name in map_kws:
                import cartopy.feature as cfeature

                feat = getattr(cfeature, feature_name.upper(), None)
                if feat is not None:
                    ax.add_feature(feat(), **map_kws[feature_name])
    # Axis labels and title
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=12, fontweight="bold")
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=12, fontweight="bold")
    if title:
        ax.set_title(title, fontsize=14, fontweight="bold")
    # Custom ticks
    if xticks is not None:
        ax.set_xticks(xticks)
    if yticks is not None:
        ax.set_yticks(yticks)
    # Annotations
    if annotations:
        for ann in annotations:
            ax.annotate(**ann)
    # Colorbar
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
            cbar = plt.colorbar(
                mesh, ax=ax, orientation="vertical", pad=0.02, aspect=30
            )
        cbar.ax.tick_params(labelsize=10)
        if cbar_label:
            cbar.set_label(cbar_label, fontsize=12)
    fig.tight_layout()
    # Export
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(subplot_kw={"projection": projection}, figsize=figsize)
    mesh = da.plot.imshow(ax=ax, transform=ccrs.PlateCarree(), **kwargs)
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
    if colorbar:
        cbar = plt.colorbar(mesh, ax=ax, orientation="vertical", pad=0.02, aspect=30)
        cbar.ax.tick_params(labelsize=10)
    fig.tight_layout()
    return fig, ax


def plot_quick_map(
    da,
    map_kws=None,
    projection=None,
    colorbar=True,
    figsize=None,
    cmap=None,
    vmin=None,
    vmax=None,
    norm=None,
    dpi=150,
    xlabel=None,
    ylabel=None,
    title=None,
    cbar_label=None,
    cbar_inset=False,
    xticks=None,
    yticks=None,
    annotations=None,
    export_path=None,
    export_formats=None,
    **kwargs,
):
    """
    Create a publication-quality map plot of the data using Cartopy and xarray's default plot method.

    Parameters
    ----------
    da : xarray.DataArray
        The data to plot.
    map_kws : dict, optional
        Dictionary of keyword arguments for map features (e.g., coastlines, gridlines, features, borders, land, ocean).
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to PlateCarree.
    colorbar : bool, default: True
        Whether to add a colorbar.
    figsize : tuple, optional
        Figure size.
    cmap : str or Colormap, optional
        Colormap to use (supports colorblind-friendly options).
    vmin, vmax : float, optional
        Color limits.
    norm : Normalize, optional
        Matplotlib normalization (e.g., LogNorm).
    dpi : int, optional
        Dots per inch for export.
    xlabel, ylabel, title : str, optional
        Axis labels and plot title.
    cbar_label : str, optional
        Label for the colorbar.
    cbar_inset : bool, default: False
        Place colorbar as an inset (right) if True.
    xticks, yticks : list, optional
        Custom tick locations.
    annotations : list of dict, optional
        List of annotation dicts (e.g., {"text": "A", "xy": (lon, lat)}).
    export_path : str, optional
        Path to export the figure (without extension).
    export_formats : list, optional
        List of formats to export (e.g., ["png", "pdf"]).
    **kwargs : dict
        Additional keyword arguments for xarray's plot method.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The matplotlib figure object.
    ax : matplotlib.axes.Axes
        The matplotlib axes object.
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(
        subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi
    )
    plot_args = dict(cmap=cmap, vmin=vmin, vmax=vmax, norm=norm)
    plot_args.update({k: v for k, v in kwargs.items() if k not in ["ax", "transform"]})
    mesh = da.plot(ax=ax, transform=ccrs.PlateCarree(), **plot_args)
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
        # Extra features
        for feature_name in ["land", "ocean", "borders", "lakes", "rivers", "states"]:
            if feature_name in map_kws:
                import cartopy.feature as cfeature

                feat = getattr(cfeature, feature_name.upper(), None)
                if feat is not None:
                    ax.add_feature(feat(), **map_kws[feature_name])
    # Axis labels and title
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=12, fontweight="bold")
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=12, fontweight="bold")
    if title:
        ax.set_title(title, fontsize=14, fontweight="bold")
    # Custom ticks
    if xticks is not None:
        ax.set_xticks(xticks)
    if yticks is not None:
        ax.set_yticks(yticks)
    # Annotations
    if annotations:
        for ann in annotations:
            ax.annotate(**ann)
    # Colorbar
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
            cbar = plt.colorbar(
                mesh, ax=ax, orientation="vertical", pad=0.02, aspect=30
            )
        cbar.ax.tick_params(labelsize=10)
        if cbar_label:
            cbar.set_label(cbar_label, fontsize=12)
    fig.tight_layout()
    # Export
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax


def plot_quick_contourf(
    da,
    map_kws=None,
    projection=None,
    colorbar=True,
    figsize=None,
    cmap=None,
    vmin=None,
    vmax=None,
    norm=None,
    dpi=150,
    xlabel=None,
    ylabel=None,
    title=None,
    cbar_label=None,
    cbar_inset=False,
    xticks=None,
    yticks=None,
    annotations=None,
    export_path=None,
    export_formats=None,
    **kwargs,
):
    """
    Create a publication-quality filled contour plot of the data on a map using Cartopy.

    Parameters
    ----------
    da : xarray.DataArray
        The data to plot.
    map_kws : dict, optional
        Dictionary of keyword arguments for map features (e.g., coastlines, gridlines, features, borders, land, ocean).
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to PlateCarree.
    colorbar : bool, default: True
        Whether to add a colorbar.
    figsize : tuple, optional
        Figure size.
    cmap : str or Colormap, optional
        Colormap to use (supports colorblind-friendly options).
    vmin, vmax : float, optional
        Color limits.
    norm : Normalize, optional
        Matplotlib normalization (e.g., LogNorm).
    dpi : int, optional
        Dots per inch for export.
    xlabel, ylabel, title : str, optional
        Axis labels and plot title.
    cbar_label : str, optional
        Label for the colorbar.
    cbar_inset : bool, default: False
        Place colorbar as an inset (right) if True.
    xticks, yticks : list, optional
        Custom tick locations.
    annotations : list of dict, optional
        List of annotation dicts (e.g., {"text": "A", "xy": (lon, lat)}).
    export_path : str, optional
        Path to export the figure (without extension).
    export_formats : list, optional
        List of formats to export (e.g., ["png", "pdf"]).
    **kwargs : dict
        Additional keyword arguments for contourf.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The matplotlib figure object.
    ax : matplotlib.axes.Axes
        The matplotlib axes object.
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(
        subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi
    )
    plot_args = dict(cmap=cmap, vmin=vmin, vmax=vmax, norm=norm)
    plot_args.update({k: v for k, v in kwargs.items() if k not in ["ax", "transform"]})
    mesh = da.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), **plot_args)
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
        # Extra features
        for feature_name in ["land", "ocean", "borders", "lakes", "rivers", "states"]:
            if feature_name in map_kws:
                import cartopy.feature as cfeature

                feat = getattr(cfeature, feature_name.upper(), None)
                if feat is not None:
                    ax.add_feature(feat(), **map_kws[feature_name])
    # Axis labels and title
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=12, fontweight="bold")
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=12, fontweight="bold")
    if title:
        ax.set_title(title, fontsize=14, fontweight="bold")
    # Custom ticks
    if xticks is not None:
        ax.set_xticks(xticks)
    if yticks is not None:
        ax.set_yticks(yticks)
    # Annotations
    if annotations:
        for ann in annotations:
            ax.annotate(**ann)
    # Colorbar
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
            cbar = plt.colorbar(
                mesh, ax=ax, orientation="vertical", pad=0.02, aspect=30
            )
        cbar.ax.tick_params(labelsize=10)
        if cbar_label:
            cbar.set_label(cbar_label, fontsize=12)
    fig.tight_layout()
    # Export
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax


def facet_time_map(
    da,
    time_dim="time",
    ncols=3,
    map_kws=None,
    projection=None,
    colorbar=True,
    figsize=None,
    cmap=None,
    vmin=None,
    vmax=None,
    norm=None,
    dpi=150,
    xlabel=None,
    ylabel=None,
    suptitle=None,
    cbar_label=None,
    xticks=None,
    yticks=None,
    annotations=None,
    export_path=None,
    export_formats=None,
    **kwargs,
):
    """
    Create a facet grid of map plots for each time slice in a DataArray using Cartopy.

    Parameters
    ----------
    da : xarray.DataArray
        The data to plot (must have a time dimension).
    time_dim : str, default: "time"
        Name of the time dimension.
    ncols : int, default: 3
        Number of columns in the facet grid.
    map_kws : dict, optional
        Dictionary of keyword arguments for map features.
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to PlateCarree.
    colorbar : bool, default: True
        Whether to add a colorbar (shared).
    figsize : tuple, optional
        Figure size.
    cmap : str or Colormap, optional
        Colormap to use.
    vmin, vmax : float, optional
        Color limits.
    norm : Normalize, optional
        Matplotlib normalization.
    dpi : int, optional
        Dots per inch for export.
    xlabel, ylabel, suptitle : str, optional
        Axis labels and super title.
    cbar_label : str, optional
        Label for the colorbar.
    xticks, yticks : list, optional
        Custom tick locations.
    annotations : list of dict, optional
        List of annotation dicts for each subplot.
    export_path : str, optional
        Path to export the figure (without extension).
    export_formats : list, optional
        List of formats to export (e.g., ["png", "pdf"]).
    **kwargs : dict
        Additional keyword arguments for plotting.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The matplotlib figure object.
    axes : ndarray of matplotlib.axes.Axes
        The matplotlib axes objects.
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
    fig, axes = plt.subplots(
        nrows, ncols, subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi
    )
    axes = np.atleast_1d(axes).flatten()
    plot_args = dict(cmap=cmap, vmin=vmin, vmax=vmax, norm=norm)
    plot_args.update(kwargs)
    mesh = None
    for i, t in enumerate(times):
        ax = axes[i]
        dat = da.sel({time_dim: t})
        mesh = dat.plot(
            ax=ax, transform=ccrs.PlateCarree(), add_colorbar=False, **plot_args
        )
        # Map features
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
        # Axis labels and title
        if xlabel:
            ax.set_xlabel(xlabel, fontsize=10)
        if ylabel:
            ax.set_ylabel(ylabel, fontsize=10)
        ax.set_title(str(np.datetime_as_string(t)), fontsize=11)
        # Custom ticks
        if xticks is not None:
            ax.set_xticks(xticks)
        if yticks is not None:
            ax.set_yticks(yticks)
        # Annotations
        if annotations and i < len(annotations):
            ax.annotate(**annotations[i])
    # Remove unused axes
    for j in range(nt, len(axes)):
        fig.delaxes(axes[j])
    # Shared colorbar
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
    # Export
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, axes


# Pandas DataFrame mapping utilities
def plot_points_map(
    df,
    lon_col="longitude",
    lat_col="latitude",
    projection=None,
    color="C0",
    marker="o",
    size=40,
    edgecolor="k",
    alpha=0.8,
    map_kws=None,
    figsize=(8, 6),
    dpi=150,
    title=None,
    export_path=None,
    export_formats=None,
    **kwargs,
):
    """
    Plot points from a DataFrame on a Cartopy map.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with longitude and latitude columns.
    lon_col, lat_col : str
        Column names for longitude and latitude.
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to PlateCarree.
    color : str or array-like, optional
        Color for points.
    marker : str, optional
        Marker style.
    size : float or array-like, optional
        Marker size.
    edgecolor : str, optional
        Marker edge color.
    alpha : float, optional
        Marker transparency.
    map_kws : dict, optional
        Map feature keyword arguments.
    figsize : tuple, optional
        Figure size.
    dpi : int, optional
        Dots per inch for export.
    title : str, optional
        Plot title.
    export_path : str, optional
        Path to export the figure (without extension).
    export_formats : list, optional
        List of formats to export (e.g., ["png", "pdf"]).
    **kwargs : dict
        Additional keyword arguments for plt.scatter.

    Returns
    -------
    fig, ax : matplotlib Figure and Axes
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(
        subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi
    )
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
    # Plot points
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
        ax.set_title(title, fontsize=14, fontweight="bold")
    fig.tight_layout()
    # Export
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax


def plot_lines_map(
    df,
    lon_col="longitude",
    lat_col="latitude",
    group_col=None,
    projection=None,
    color="C0",
    linewidth=2,
    alpha=0.8,
    map_kws=None,
    figsize=(8, 6),
    dpi=150,
    title=None,
    export_path=None,
    export_formats=None,
    **kwargs,
):
    """
    Plot lines from a DataFrame on a Cartopy map. Optionally group by a column.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with longitude and latitude columns.
    lon_col, lat_col : str
        Column names for longitude and latitude.
    group_col : str, optional
        Column to group lines (e.g., for trajectories).
    projection : cartopy.crs.Projection, optional
        Cartopy projection to use. Defaults to PlateCarree.
    color : str or array-like, optional
        Line color.
    linewidth : float, optional
        Line width.
    alpha : float, optional
        Line transparency.
    map_kws : dict, optional
        Map feature keyword arguments.
    figsize : tuple, optional
        Figure size.
    dpi : int, optional
        Dots per inch for export.
    title : str, optional
        Plot title.
    export_path : str, optional
        Path to export the figure (without extension).
    export_formats : list, optional
        List of formats to export (e.g., ["png", "pdf"]).
    **kwargs : dict
        Additional keyword arguments for plt.plot.

    Returns
    -------
    fig, ax : matplotlib Figure and Axes
    """
    if ccrs is None:
        raise ImportError("Cartopy is required for mapping utilities.")
    if projection is None:
        projection = ccrs.PlateCarree()
    if map_kws is None:
        map_kws = {}
    fig, ax = plt.subplots(
        subplot_kw={"projection": projection}, figsize=figsize, dpi=dpi
    )
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
    # Plot lines
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
        ax.set_title(title, fontsize=14, fontweight="bold")
    fig.tight_layout()
    # Export
    if export_path:
        if export_formats is None:
            export_formats = ["png"]
        for fmt in export_formats:
            fig.savefig(f"{export_path}.{fmt}", dpi=dpi, bbox_inches="tight")
    return fig, ax
