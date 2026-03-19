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

    def remap_xesmf(self, data, parallel=True, n_workers=None, **kwargs):
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

    def _remap_xesmf_dataset(self, dset, filename="monet_xesmf_regrid_file.nc", **kwargs):
        """Deprecated: Remap dataset using xESMF."""
        warnings.warn("_remap_xesmf_dataset is deprecated.", DeprecationWarning, stacklevel=2)
        return self.remap(dset, method="xesmf", **kwargs)

    def _remap_xesmf_dataarray(
        self,
        dataarray,
        method="bilinear",
        filename="monet_xesmf_regrid_file.nc",
        **kwargs,
    ):
        """Deprecated: Remap DataArray using xESMF."""
        warnings.warn("_remap_xesmf_dataarray is deprecated.", DeprecationWarning, stacklevel=2)
        # We can implement this via resample
        from ..util.resample import resample

        target = self._obj
        out = resample(dataarray, target, method=method, **kwargs)
        if out.name in self._obj.variables:
            out.name = out.name + "_y"
        self._obj[out.name] = out
        return out

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

    def stratify(
        self,
        levels: t.Sequence[float],
        vertical: xr.DataArray | str,
        axis: int = 1,
        tension: float = 0.0,
    ) -> xr.Dataset:
        """Vertically interpolate data to specified levels.
        Supports both Eager (NumPy) and Lazy (Dask) backends.

        Parameters
        ----------
        levels : array-like
            Target vertical levels.
        vertical : xarray.DataArray or str
            Vertical coordinate values or name of the vertical coordinate.
        axis : int, default: 1
            Axis along which to interpolate.
        tension : float, default: 0.0
            Tension factor for the spline interpolation.

        Returns
        -------
        xarray.Dataset
            Vertically interpolated dataset.

        Examples
        --------
        >>> ds.monet.stratify([100, 500, 1000], 'altitude')
        """
        if isinstance(vertical, str):
            vertical = self._obj[vertical]
        vertical_shape = vertical.shape
        vlen = -len(vertical_shape)
        loop_vars = [
            vn
            for vn in self._obj.variables
            if "z" in self._obj[vn].dims
            and vn != vertical.name
            and len(self._obj[vn].shape) >= len(vertical_shape)
            and self._obj[vn].shape[vlen:] == vertical_shape
        ]

        if not loop_vars:
            raise ValueError("No variables found with vertical dimension matching the provided coordinate")

        from ..util.resample import resample_stratify

        orig = resample_stratify(self._obj[loop_vars[0]], levels, vertical, axis=axis, tension=tension)
        dset = orig.to_dataset(name=loop_vars[0])
        dset.attrs = self._obj.attrs.copy()

        for vn in loop_vars[1:]:
            dset[vn] = resample_stratify(self._obj[vn], levels, vertical, axis=axis, tension=tension)

        # Update history
        from ..util.conventions import update_history

        update_history(dset, "Vertically stratified entire Dataset")

        return dset

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
