"""Dataset accessor for MONET functionality."""

import typing as t
import warnings

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from .base import BaseAccessor


@xr.register_dataset_accessor("monet")
class MONETAccessorDataset(BaseAccessor):
    def __init__(self, xray_obj):
        """Initialize the accessor.

        Parameters
        ----------
        xray_obj : xarray.Dataset
            The Dataset this accessor will work with.
        """
        self._obj = xray_obj

    def remap_nearest_parallel(self, data, radius_of_influence=1e6, n_processes=None, **kwargs):
        """Deprecated: Remap data using nearest neighbor interpolation with parallel processing."""
        warnings.warn(
            "remap_nearest_parallel is deprecated. xregrid uses dask for parallelization.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.remap(data, method="nearest", **kwargs)

    def remap_nearest(self, data, radius_of_influence=1e6, **kwargs):
        """Deprecated: Remap data using nearest neighbor interpolation."""
        warnings.warn(
            "remap_nearest is deprecated and will be removed in a future version. "
            "Please use remap(data, method='nearest') instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.remap(data, method="nearest", radius_of_influence=radius_of_influence, **kwargs)

    def nearest_ij(self, lat=None, lon=None, **kwargs):
        """Find the nearest grid indices to given lat/lon point(s).

        Parameters
        ----------
        lat : float or array-like, optional
            Latitude value(s).
        lon : float or array-like, optional
            Longitude value(s).
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        tuple
            (i, j) indices of nearest point(s).
        """
        raise NotImplementedError("nearest_ij is not yet implemented with xregrid")

    def stratify(self, levels, vertical, axis=1, tension=0.0):
        """Deprecated: use ``interpolate_vertical`` instead."""
        import warnings

        warnings.warn(
            "stratify() is deprecated. Use interpolate_vertical(target_levels, level_dim) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        level_dim = vertical if isinstance(vertical, str) else (vertical.name or self._obj.dims[axis])
        return self.interpolate_vertical(np.asarray(levels), level_dim=level_dim, tension=tension)

    def interpolate_vertical(
        self,
        target_levels,
        level_dim: str = "level",
        tension: float = 0.0,
    ) -> xr.Dataset:
        """Vertically interpolate all variables in the Dataset to new levels using pytspack tension splines.

        Parameters
        ----------
        target_levels : array-like
            Target vertical level values.
        level_dim : str, default: ``"level"``
            Name of the vertical dimension to interpolate along.
        tension : float, default: ``0.0``
            Tension factor for the spline. ``0.0`` gives a standard cubic spline.

        Returns
        -------
        xarray.Dataset
            Dataset with all variables that have ``level_dim`` interpolated to ``target_levels``.

        Examples
        --------
        >>> ds.monet.interpolate_vertical([850, 700, 500], level_dim='pressure')
        """
        from pytspack import interpolate_vertical

        ds = self._obj
        if any(hasattr(ds[v].data, "chunks") for v in ds.data_vars) and level_dim in ds.dims:
            ds = ds.chunk({level_dim: -1})
        return interpolate_vertical(ds, target_levels, level_dim=level_dim, tension=tension)

    def quick_facet_time_map(
        self,
        var: str,
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
        time_dim: str = "time",
        ncols: int = 3,
        **kwargs: t.Any,
    ) -> tuple[plt.Figure, np.ndarray]:
        """
        Create a facet grid of map plots for each time slice in a Dataset variable using Cartopy.
        Convention-aware: supports both CF/COARDS and UGRID.

        Parameters
        ----------
        var : str
            Name of the variable in the dataset to plot.
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
        time_dim : str, default: "time"
            Name of the time dimension.
        ncols : int, default: 3
            Number of columns in the facet grid.
        **kwargs : dict
            Additional keyword arguments for plotting.

        Returns
        -------
        fig : matplotlib.figure.Figure
            The matplotlib figure object.
        axes : ndarray of matplotlib.axes.Axes
            The matplotlib axes objects.
        """
        from ..plots.cartopy_utils import facet_time_map

        return facet_time_map(
            self._obj[var],
            time_dim=time_dim,
            ncols=ncols,
            map_kws=map_kws,
            projection=projection,
            colorbar=colorbar,
            figsize=figsize,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            norm=norm,
            dpi=dpi,
            xlabel=xlabel,
            ylabel=ylabel,
            suptitle=suptitle,
            cbar_label=cbar_label,
            xticks=xticks,
            yticks=yticks,
            annotations=annotations,
            export_path=export_path,
            export_formats=export_formats,
            **kwargs,
        )
