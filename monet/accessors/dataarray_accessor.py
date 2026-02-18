"""DataArray accessor for MONET functionality."""

import datetime
import typing as t
import warnings

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from .base import BaseAccessor


@xr.register_dataarray_accessor("monet")
class MONETAccessor(BaseAccessor):
    """DataArray accessor for MONET functionality."""

    def __init__(self, xray_obj):
        """Initialize the accessor.

        Parameters
        ----------
        xray_obj : xarray.DataArray
            The DataArray this accessor will work with.
        """
        self._obj = xray_obj

    def remap_xesmf(self, data, **kwargs):
        """Deprecated: Remap data using xESMF regridding."""
        warnings.warn(
            "remap_xesmf is deprecated and will be removed in a future version. "
            "Please use remap(data, method='xesmf') or remap(data, method='conservative') instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        # Handle method argument from kwargs
        if "method" in kwargs:
            kwargs["xesmf_method"] = kwargs.pop("method")

        return self.remap(data, method="xesmf", **kwargs)

    def remap_nearest_parallel(self, data, radius_of_influence=1e6, n_processes=None, **kwargs):
        """Deprecated: Remap data using nearest neighbor interpolation with parallel processing."""
        warnings.warn(
            "remap_nearest_parallel is deprecated. xregrid uses dask for parallelization.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.remap(data, method="nearest", **kwargs)

    def stratify(self, levels, vertical, axis=1, tension=0.0):
        """Vertically interpolate data to specified levels.

        Parameters
        ----------
        levels : array-like
            Target vertical levels.
        vertical : xarray.DataArray or str
            Vertical coordinate values or name.
        axis : int, default: 1
            Axis along which to interpolate.
        tension : float, default: 0.0
            Tension factor for the spline interpolation.

        Returns
        -------
        xarray.DataArray
            Vertically interpolated data.
        """
        from ..util.resample import resample_stratify

        if isinstance(vertical, str):
            vertical = self._obj[vertical]

        out = resample_stratify(self._obj, levels, vertical, axis=axis, tension=tension)
        return out

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

    def quick_imshow(
        self,
        map_kws: dict[str, t.Any] | None = None,
        roll_dateline: bool = False,
        projection: t.Any | None = None,
        colorbar: bool = True,
        figsize: tuple[float, float] | None = None,
        **kwargs: t.Any,
    ) -> tuple[plt.Figure, plt.Axes]:
        """Create a quick imshow plot of the data with flexible options.
        Convention-aware: supports both CF/COARDS and UGRID.
        """
        from ..plots.cartopy_utils import plot_quick_imshow

        return plot_quick_imshow(
            self._obj,
            map_kws=map_kws,
            projection=projection,
            colorbar=colorbar,
            figsize=figsize,
            **kwargs,
        )

    def quick_map(
        self,
        map_kws: dict[str, t.Any] | None = None,
        roll_dateline: bool = False,
        projection: t.Any | None = None,
        colorbar: bool = True,
        figsize: tuple[float, float] | None = None,
        **kwargs: t.Any,
    ) -> tuple[plt.Figure, plt.Axes]:
        """Create a quick map plot of the data with flexible options.
        Convention-aware: supports both CF/COARDS and UGRID.
        """
        from ..plots.cartopy_utils import plot_quick_map

        return plot_quick_map(
            self._obj,
            map_kws=map_kws,
            projection=projection,
            colorbar=colorbar,
            figsize=figsize,
            **kwargs,
        )

    def quick_contourf(
        self,
        map_kws: dict[str, t.Any] | None = None,
        roll_dateline: bool = False,
        projection: t.Any | None = None,
        colorbar: bool = True,
        figsize: tuple[float, float] | None = None,
        **kwargs: t.Any,
    ) -> tuple[plt.Figure, plt.Axes]:
        """Create a quick filled contour plot of the data with flexible options.
        Convention-aware: supports both CF/COARDS and UGRID.
        """
        from ..plots.cartopy_utils import plot_quick_contourf

        return plot_quick_contourf(
            self._obj,
            map_kws=map_kws,
            projection=projection,
            colorbar=colorbar,
            figsize=figsize,
            **kwargs,
        )

    def _tight_layout(self):
        """Apply tight layout to the current figure.

        Returns
        -------
        None
        """
        from matplotlib.pyplot import subplots_adjust

        subplots_adjust(0, 0, 1, 1)

    def remap_nearest(self, data, radius_of_influence=1e6, **kwargs):
        """Deprecated: Remap data using nearest neighbor interpolation."""
        warnings.warn(
            "remap_nearest is deprecated and will be removed in a future version. "
            "Please use remap(data, method='nearest') instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.remap(data, method="nearest", radius_of_influence=radius_of_influence, **kwargs)

    def compare(
        self,
        other: xr.DataArray,
        stat: str | t.Callable = "diff",
        plot: bool = True,
        plot_method: str = "quick_map",
        stat_kwargs: dict[str, t.Any] | None = None,
        plot_kwargs: dict[str, t.Any] | None = None,
    ) -> xr.DataArray | tuple[plt.Figure, plt.Axes]:
        """
        Compute and optionally plot a statistic between this DataArray and another.
        Leverages MONET's monet_stats metrics and preserves Dask laziness.

        Parameters
        ----------
        other : xarray.DataArray
            The other DataArray to compare with.
        stat : str or callable, default: "diff"
            Statistic to compute. Can be any metric name from monet_stats
            (e.g., "RMSE", "MB", "NMB", "IOA", etc.), "diff", or a callable.
        plot : bool, default: True
            Whether to plot the result using a MONET quick plot method.
        plot_method : str, default: "quick_map"
            Which plotting method to use
            (e.g., "quick_map", "quick_imshow", "quick_contourf").
        stat_kwargs : dict, optional
            Additional kwargs for the statistic function.
        plot_kwargs : dict, optional
            Additional kwargs for the plotting function.

        Returns
        -------
        xarray.DataArray or (fig, ax)
            The statistic DataArray, or (fig, ax) if plot=True.
        """

        stat_kwargs = stat_kwargs or {}
        plot_kwargs = plot_kwargs or {}
        da1 = self._obj
        da2 = other
        # Align DataArrays
        da1, da2 = xr.align(da1, da2, join="inner")
        # Compute statistic
        stat_da = None
        if callable(stat):
            stat_da = stat(da1, da2, **stat_kwargs)
        elif isinstance(stat, str):
            if stat.lower() == "diff":
                stat_da = da1 - da2
            else:
                # Try to get the function from monet_stats
                try:
                    import monet_stats

                    func = getattr(monet_stats, stat)
                    stat_da = func(da1, da2, **stat_kwargs)
                except (ImportError, AttributeError) as e:
                    # fallback to built-in (Dask-safe)
                    if stat.lower() == "rmse":
                        stat_da = np.sqrt(((da1 - da2) ** 2).mean(dim=stat_kwargs.get("dim", None)))
                    elif stat.lower() == "mae":
                        stat_da = np.abs(da1 - da2).mean(dim=stat_kwargs.get("dim", None))
                    elif stat.lower() == "mse":
                        stat_da = ((da1 - da2) ** 2).mean(dim=stat_kwargs.get("dim", None))
                    else:
                        raise ValueError(f"Unknown stat: {stat}") from e
        else:
            raise ValueError(f"Unknown stat: {stat}")

        # Ensure stat_da is a DataArray and set name
        if not isinstance(stat_da, xr.DataArray):
            # Convert scalar to DataArray if needed
            stat_da = xr.DataArray(stat_da)

        stat_name = stat if isinstance(stat, str) else getattr(stat, "__name__", "statistic")
        stat_da.name = stat_name

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = stat_da.attrs.get("history", "")
        stat_da.attrs["history"] = history + f"\n{curr_time} > Computed comparison statistic: {stat_name}"

        if plot:
            plot_func = getattr(stat_da.monet, plot_method)
            return plot_func(**plot_kwargs)
        else:
            return stat_da

    def quick_facet_time_map(
        self,
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
        Create a facet grid of map plots for each time slice in a DataArray using Cartopy.
        Convention-aware: supports both CF/COARDS and UGRID.

        Parameters
        ----------
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
            self._obj,
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
