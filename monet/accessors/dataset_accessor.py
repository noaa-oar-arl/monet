"""Dataset accessor for MONET functionality."""

import datetime
import typing as t
import warnings

import pandas as pd
import xarray as xr

from .base import BaseAccessor, has_monet_regrid, has_xregrid

has_pyresample = False
has_xesmf = False


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

    def is_land(self, return_xarray=False):
        """Check if points are on land.

        Parameters
        ----------
        return_xarray : bool, default: False
            If True, return results as xarray. Otherwise, return numpy array.

        Returns
        -------
        xarray.DataArray or numpy.ndarray
            Boolean array where True indicates land.
        """
        try:
            import global_land_mask as glm
        except ImportError:
            raise ImportError("Please install global_land_mask from pypi")

        da = self._dataset_to_monet(self._obj)
        island = glm.is_land(da.latitude.values, da.longitude.values)
        if return_xarray:
            return da.where(island)
        else:
            return island

    def is_ocean(self, return_xarray=False):
        """Check if points are on ocean.

        Parameters
        ----------
        return_xarray : bool, default: False
            If True, return results as xarray. Otherwise, return numpy array.

        Returns
        -------
        xarray.DataArray or numpy.ndarray
            Boolean array where True indicates ocean.
        """
        try:
            import global_land_mask as glm
        except ImportError:
            raise ImportError("Please install global_land_mask from pypi")

        da = self._dataset_to_monet(self._obj)
        isocean = glm.is_ocean(da.latitude.values, da.longitude.values)
        if return_xarray:
            return da.where(isocean)
        else:
            return isocean

    def cftime_to_datetime64(self, name=None):
        """Convert cftime coordinates to numpy datetime64.

        Parameters
        ----------
        name : str, optional
            Name of the coordinate to convert. If None, tries to detect the time coordinate.

        Returns
        -------
        xarray.Dataset
            Dataset with converted time coordinate.
        """
        from numpy import vectorize

        ds = self._obj

        def cf_to_dt64(x):
            return pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S"))

        if name is None:  # assume 'time' is the column name to transform
            name = "time"
        if isinstance(ds[name].to_index(), xr.CFTimeIndex):
            ds[name] = xr.apply_ufunc(vectorize(cf_to_dt64), ds[name])
        return ds

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

    def combine_point_esmf(self, point_df, method="bilinear", **kwargs):
        """Combine this dataset with point data using ESMF LocStream.

        Deprecated as ESMF dependency is removed.
        """
        raise NotImplementedError("This function relies on ESMF which has been removed.")

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
        from ..util import resample

        target = self._obj
        out = resample.resample(dataarray, target, method=method, **kwargs)
        if out.name in self._obj.variables:
            out.name = out.name + "_y"
        self._obj[out.name] = out
        return out

    def remap(
        self,
        data: xr.DataArray | xr.Dataset,
        method: str = "nearest",
        radius_of_influence: float = 1e6,
        **kwargs: t.Any,
    ) -> xr.DataArray | xr.Dataset:
        """Remap data using xregrid or monet-regrid fallback.
        Supports both CF/COARDS and UGRID conventions.

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            Data to remap (Source).
        method : str, default: 'nearest'
            Resampling method: 'nearest', 'bilinear', or others supported by backends.
        radius_of_influence : float, default: 1e6
            Search radius in meters (unused in xregrid).
        **kwargs : dict
            Additional keyword arguments for the resampler.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Remapped data.

        Examples
        --------
        >>> ds.monet.remap(other_ds, method='bilinear')
        """
        if not has_xregrid and not has_monet_regrid:
            raise ImportError("xregrid (with esmpy) or monet-regrid is required for this functionality")

        from ..util import resample

        # Check for Dask to replicate original inconsistent API behavior
        # Original behavior:
        # If self is Dask and shapes differ: self is Source, data is Target.
        # Else: data is Source, self is Target.

        is_dask = hasattr(self._obj, "chunks") and self._obj.chunks is not None
        target_shape = data.shape if hasattr(data, "shape") else None
        source_shape = self._obj.shape if hasattr(self._obj, "shape") else None

        if is_dask and target_shape is not None and target_shape != source_shape:
            source = self._dataset_to_monet(self._obj)
            target = self._dataset_to_monet(data)
        else:
            source = self._dataset_to_monet(data)
            target = self._dataset_to_monet(self._obj)

        out = resample.resample(source, target, method=method, **kwargs)

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = out.attrs.get("history", "")
        out.attrs["history"] = history + f"\n{curr_time} > Remapped via monet.remap"

        return self._rename_to_monet_latlon(out)

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

    def nearest_latlon(self, lat=None, lon=None, cleanup=True, esmf=False, **kwargs):
        """Extract data at nearest lat/lon point(s).

        Parameters
        ----------
        lat : float or array-like, optional
            Latitude value(s).
        lon : float or array-like, optional
            Longitude value(s).
        cleanup : bool, default: True
            Whether to clean up temporary files after regridding.
        esmf : bool, default: False
            Whether to use ESMF for regridding.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        xarray.Dataset
            Dataset at nearest point(s).
        """
        if lat is None or lon is None:
            raise ValueError("Must provide latitude and longitude")

        self._obj = self._rename_latlon(self._obj)

        from ..util.interp_util import constant_1d_xesmf
        from ..util.resample import resample

        target = constant_1d_xesmf(longitude=lon, latitude=lat)
        output = resample(self._obj, target, method="nearest", **kwargs)

        return self._rename_latlon(output.squeeze())

    def interp_constant_lat(self, lat=None, lat_name="latitude", lon_name="longitude", **kwargs):
        """Interpolate data to a constant latitude.

        Parameters
        ----------
        lat : float, optional
            Latitude value to interpolate to.
        lat_name : str, default: "latitude"
            Name of the latitude coordinate.
        lon_name : str, default: "longitude"
            Name of the longitude coordinate.
        **kwargs : dict
            Additional keyword arguments for interpolation.

        Returns
        -------
        xarray.Dataset
            Interpolated dataset.
        """
        from numpy import asarray, linspace, ones

        try:
            if lat is None:
                raise RuntimeError
        except RuntimeError:
            print("Must enter lat value")

        d1 = self._dataset_to_monet(self._obj, lat_name=lat_name, lon_name=lon_name)
        longitude = linspace(d1.longitude.min(), d1.longitude.max(), len(d1.x))
        latitude = ones(longitude.shape) * asarray(lat)

        # Create target grid
        from ..util.interp_util import constant_1d_xesmf

        target = constant_1d_xesmf(latitude=latitude, longitude=longitude)

        # Use new regridding
        from ..util.resample import resample

        out = resample(self._obj, target, **kwargs)
        return self._rename_latlon(out)

    def interp_constant_lon(self, lon=None, **kwargs):
        """Interpolate data to a constant longitude.

        Parameters
        ----------
        lon : float, optional
            Longitude value to interpolate to.
        **kwargs : dict
            Additional keyword arguments for interpolation.

        Returns
        -------
        xarray.Dataset
            Interpolated dataset.
        """
        from numpy import asarray, linspace, ones

        try:
            if lon is None:
                raise RuntimeError
        except RuntimeError:
            print("Must enter lon value")

        d1 = self._dataset_to_monet(self._obj)
        latitude = linspace(d1.latitude.min(), d1.latitude.max(), len(d1.y))
        longitude = ones(latitude.shape) * asarray(lon)

        # Create target grid
        from ..util.interp_util import constant_1d_xesmf

        target = constant_1d_xesmf(latitude=latitude, longitude=longitude)

        # Use new regridding
        from ..util.resample import resample

        out = resample(self._obj, target, **kwargs)
        return self._rename_latlon(out)

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

        return dset

    def window(self, lat_min, lon_min, lat_max, lon_max):
        """Extract a spatial window from the data.

        Parameters
        ----------
        lat_min : float
            Minimum latitude.
        lon_min : float
            Minimum longitude.
        lat_max : float
            Maximum latitude.
        lon_max : float
            Maximum longitude.

        Returns
        -------
        xarray.Dataset
            Windowed dataset.
        """
        # This implementation relied on pyresample
        # Since we removed pyresample, we should implement a simple fallback or raise error
        # A simple window selection using xarray.sel or where could work if lat/lon are coords
        # But if they are 2D arrays, it is more complex.
        # Given "xregrid" doesn't seem to expose simple windowing logic, we can try using standard xarray logic if possible
        # or just raise NotImplementedError for now as it wasn't explicitly requested to be ported (only regridding).
        raise NotImplementedError("Window functionality is unavailable without pyresample")

    def pair(self, obs, **kwargs):
        """Pair this Dataset with observation data.

        Parameters
        ----------
        obs : xarray.Dataset, xarray.DataArray, pandas.DataFrame, or dask.dataframe.DataFrame
            Observation data to pair with.
        **kwargs : dict
            Additional arguments passed to `monet.pair`.

        Returns
        -------
        matched object
            Matched object of the same type as `obs`.
        """
        from ..util.combinetool import pair

        return pair(self._obj, obs, **kwargs)

    def combine_point(self, data, suffix=None, pyresample=True, **kwargs):
        """Combine point data with this Dataset.

        Note: This is a backward compatibility wrapper for `pair`.

        Parameters
        ----------
        data : pandas.DataFrame
            Point data to combine.
        suffix : str, optional
            Suffix to add to variable names.
        pyresample : bool, default: True
            Deprecated flag.
        **kwargs : dict
            Additional keyword arguments for regridding.

        Returns
        -------
        pandas.DataFrame
            Combined data.
        """
        return self.pair(data, suffix=suffix, **kwargs)

    def wrap_longitudes(self, lon_name="longitude"):
        """Wrap longitude values to [-180, 180).

        Parameters
        ----------
        lon_name : str, default: "longitude"
            Name of the longitude coordinate.

        Returns
        -------
        xarray.Dataset
            Dataset with wrapped longitudes.
        """
        dset = self._obj
        dset[lon_name] = (dset[lon_name] + 180) % 360 - 180
        return dset

    def tidy(self, lon_name="longitude"):
        """Apply tidying operations to the data.

        Parameters
        ----------
        lon_name : str, default: "longitude"
            Name of the longitude coordinate.

        Returns
        -------
        xarray.Dataset
            Tidied dataset.
        """
        d = self._obj
        wd = d.monet.wrap_longitudes(lon_name=lon_name)
        wdl = wd.sortby(wd[lon_name])
        return wdl

    def to_area_def(self, projection="platea", resolution=None, area_id=None):
        """Deprecated: Convert the dataset's coordinates to a pyresample AreaDefinition."""
        raise NotImplementedError("This function relies on pyresample which has been removed.")

    def to_swath_def(self):
        """Deprecated: Convert the dataset's coordinates to a pyresample SwathDefinition."""
        raise NotImplementedError("This function relies on pyresample which has been removed.")

    def quick_facet_time_map(
        self,
        var,
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
        time_dim="time",
        ncols=3,
        **kwargs,
    ):
        """
        Create a facet grid of map plots for each time slice in a Dataset variable using Cartopy.

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

        da = self._dataset_to_monet(self._obj[var])
        return facet_time_map(
            da,
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
