"""Dataset accessor for MONET functionality."""

import datetime
import typing as t
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from .base import BaseAccessor, has_monet_regrid, has_xregrid


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

    def is_land(self, return_xarray: bool = False) -> xr.Dataset | xr.DataArray | np.ndarray:
        """Check if points are on land.
        Supports both Eager (NumPy) and Lazy (Dask) backends via ``xarray.apply_ufunc``.
        Convention-aware: works with CF/COARDS and UGRID without forced renaming.

        Parameters
        ----------
        return_xarray : bool, default: False
            If True, return results as xarray (masked Dataset).
            Otherwise, return the boolean mask (DataArray or its underlying array).

        Returns
        -------
        xarray.Dataset, xarray.DataArray, or numpy.ndarray
            If return_xarray is True, returns a Dataset masked by land.
            Otherwise, returns a DataArray (if Dask-backed) or numpy.ndarray (if Eager) of booleans.
        """
        try:
            import global_land_mask as glm
        except ImportError:
            raise ImportError("Please install global_land_mask from pypi")

        lat = self.lat
        lon = self.lon
        if lat is None or lon is None:
            raise ValueError("Could not detect latitude and longitude coordinates.")

        # Use apply_ufunc to be backend-agnostic (handles Dask automatically if parallelized=True)
        island = xr.apply_ufunc(
            glm.is_land,
            lat,
            lon,
            dask="parallelized",
            output_dtypes=[bool],
        )

        if return_xarray:
            return self._obj.where(island)
        else:
            return island if hasattr(island.data, "chunks") else island.values

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

    def is_ocean(self, return_xarray: bool = False) -> xr.Dataset | xr.DataArray | np.ndarray:
        """Check if points are on ocean.
        Supports both Eager (NumPy) and Lazy (Dask) backends via ``xarray.apply_ufunc``.
        Convention-aware: works with CF/COARDS and UGRID without forced renaming.

        Parameters
        ----------
        return_xarray : bool, default: False
            If True, return results as xarray (masked Dataset).
            Otherwise, return the boolean mask (DataArray or its underlying array).

        Returns
        -------
        xarray.Dataset, xarray.DataArray, or numpy.ndarray
            If return_xarray is True, returns a Dataset masked by ocean.
            Otherwise, returns a DataArray (if Dask-backed) or numpy.ndarray (if Eager) of booleans.
        """
        try:
            import global_land_mask as glm
        except ImportError:
            raise ImportError("Please install global_land_mask from pypi")

        lat = self.lat
        lon = self.lon
        if lat is None or lon is None:
            raise ValueError("Could not detect latitude and longitude coordinates.")

        # Use apply_ufunc to be backend-agnostic
        isocean = xr.apply_ufunc(
            glm.is_ocean,
            lat,
            lon,
            dask="parallelized",
            output_dtypes=[bool],
        )

        if return_xarray:
            return self._obj.where(isocean)
        else:
            return isocean if hasattr(isocean.data, "chunks") else isocean.values

    def cftime_to_datetime64(self, name: str | None = None) -> xr.Dataset:
        """Convert cftime coordinates to numpy datetime64.
        Preserves Dask laziness if the time coordinate is Dask-backed.

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

        ds = self._obj.copy()

        def cf_to_dt64(x):
            try:
                return pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S"))
            except AttributeError:
                return x

        if name is None:  # assume 'time' is the column name to transform
            name = "time"

        if name in ds.coords and isinstance(ds[name].to_index(), xr.CFTimeIndex):
            ds[name] = xr.apply_ufunc(
                vectorize(cf_to_dt64),
                ds[name],
                dask="parallelized",
                output_dtypes=["datetime64[ns]"],
            )

            # Update history
            curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history = ds.attrs.get("history", "")
            ds.attrs["history"] = history + f"\n{curr_time} > Converted {name} from cftime to datetime64"

        return ds

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

        from ..util.resample import resample

        # Check for Dask to replicate original inconsistent API behavior
        # Original behavior:
        # If self is Dask and shapes differ: self is Source, data is Target.
        # Else: data is Source, self is Target.

        is_dask = hasattr(self._obj, "chunks") and self._obj.chunks is not None
        target_shape = getattr(data, "shape", None)
        source_shape = getattr(self._obj, "shape", None)

        if is_dask and target_shape is not None and target_shape != source_shape:
            source = self._obj
            target = data
        else:
            source = data
            target = self._obj

        out = resample(source, target, method=method, **kwargs)

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = out.attrs.get("history", "")
        out.attrs["history"] = history + f"\n{curr_time} > Remapped via monet.remap (method={method})"

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

    def nearest_latlon(
        self,
        lat: float | t.Sequence[float] | None = None,
        lon: float | t.Sequence[float] | None = None,
        cleanup: bool = True,
        esmf: bool = False,
        **kwargs: t.Any,
    ) -> xr.Dataset:
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

        ds = self._obj.copy()

        from ..util.interp_util import constant_1d_xesmf
        from ..util.resample import resample

        target = constant_1d_xesmf(longitude=lon, latitude=lat)
        output = resample(ds, target, method="nearest", **kwargs)

        res = output.squeeze()

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = res.attrs.get("history", "")
        res.attrs["history"] = history + f"\n{curr_time} > Extracted nearest lat/lon points"

        return res

    def interp_constant_lat(
        self,
        lat: float | None = None,
        **kwargs: t.Any,
    ) -> xr.Dataset:
        """Interpolate data to a constant latitude.
        Convention-aware: supports both CF/COARDS and UGRID.

        Parameters
        ----------
        lat : float, optional
            Latitude value to interpolate to.
        **kwargs : dict
            Additional keyword arguments for interpolation.

        Returns
        -------
        xarray.Dataset
            Interpolated dataset.
        """
        from numpy import asarray, linspace, ones

        if lat is None:
            raise ValueError("Must provide a latitude value ('lat')")

        ds = self._obj.copy()
        lat_da = self.lat
        lon_da = self.lon

        if lat_da is None or lon_da is None:
            raise ValueError("Could not detect latitude and longitude coordinates.")

        # Determine target points along detected longitude range
        # Note: We compute bounds eagerly for linspace
        lon_min = lon_da.min().values.item() if hasattr(lon_da.data, "chunks") else lon_da.min().item()
        lon_max = lon_da.max().values.item() if hasattr(lon_da.data, "chunks") else lon_da.max().item()

        longitude = linspace(lon_min, lon_max, lon_da.size)
        latitude = ones(longitude.shape) * asarray(lat)

        # Create target grid
        from ..util.interp_util import points_to_dataset

        target = points_to_dataset(latitude=latitude, longitude=longitude)

        # Use new regridding
        from ..util.resample import resample

        out = resample(ds, target, **kwargs)

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = out.attrs.get("history", "")
        out.attrs["history"] = history + f"\n{curr_time} > Interpolated to constant latitude: {lat}"

        return out

    def interp_constant_lon(self, lon: float | None = None, **kwargs: t.Any) -> xr.Dataset:
        """Interpolate data to a constant longitude.
        Convention-aware: supports both CF/COARDS and UGRID.

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

        if lon is None:
            raise ValueError("Must provide a longitude value ('lon')")

        ds = self._obj.copy()
        lat_da = self.lat
        lon_da = self.lon

        if lat_da is None or lon_da is None:
            raise ValueError("Could not detect latitude and longitude coordinates.")

        # Determine target points along detected latitude range
        # Note: We compute bounds eagerly for linspace
        lat_min = lat_da.min().values.item() if hasattr(lat_da.data, "chunks") else lat_da.min().item()
        lat_max = lat_da.max().values.item() if hasattr(lat_da.data, "chunks") else lat_da.max().item()

        latitude = linspace(lat_min, lat_max, lat_da.size)
        longitude = ones(latitude.shape) * asarray(lon)

        # Create target grid
        from ..util.interp_util import points_to_dataset

        target = points_to_dataset(latitude=latitude, longitude=longitude)

        # Use new regridding
        from ..util.resample import resample

        out = resample(ds, target, **kwargs)

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = out.attrs.get("history", "")
        out.attrs["history"] = history + f"\n{curr_time} > Interpolated to constant longitude: {lon}"

        return out

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
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = dset.attrs.get("history", "")
        dset.attrs["history"] = history + f"\n{curr_time} > Vertically stratified entire Dataset"

        return dset

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

    def combine_point(self, data, suffix=None, **kwargs):
        """Combine point data with this Dataset.

        Note: This is a backward compatibility wrapper for `pair`.

        Parameters
        ----------
        data : pandas.DataFrame
            Point data to combine.
        suffix : str, optional
            Suffix to add to variable names.
        **kwargs : dict
            Additional keyword arguments for regridding.

        Returns
        -------
        pandas.DataFrame
            Combined data.
        """
        return self.pair(data, suffix=suffix, **kwargs)

    def wrap_longitudes(self, lon_name: str | None = None) -> xr.Dataset:
        """Wrap longitude values to [-180, 180).
        Convention-aware: auto-detects longitude if lon_name is None.

        Parameters
        ----------
        lon_name : str, optional
            Name of the longitude coordinate. If None, auto-detects.

        Returns
        -------
        xarray.Dataset
            Dataset with wrapped longitudes.
        """
        if lon_name is None:
            _, lon_name = self._detect_latlon_names(self._obj)
            if lon_name is None:
                raise ValueError("Could not detect longitude coordinate.")

        dset = self._obj.copy()
        dset[lon_name] = (dset[lon_name] + 180) % 360 - 180

        # Update history for provenance
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = dset.attrs.get("history", "")
        dset.attrs["history"] = history + f"\n{curr_time} > Wrapped longitudes ({lon_name}) via monet.wrap_longitudes"

        return dset

    def tidy(self, lon_name: str | None = None) -> xr.Dataset:
        """Apply tidying operations to the data.
        Wraps longitudes and sorts by longitude.
        Convention-aware: auto-detects longitude if lon_name is None.

        Parameters
        ----------
        lon_name : str, optional
            Name of the longitude coordinate. If None, auto-detects.

        Returns
        -------
        xarray.Dataset
            Tidied dataset.
        """
        if lon_name is None:
            _, lon_name = self._detect_latlon_names(self._obj)
            if lon_name is None:
                raise ValueError("Could not detect longitude coordinate.")

        wd = self.wrap_longitudes(lon_name=lon_name)
        wdl = wd.sortby(wd[lon_name])

        # Update history for provenance
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = wdl.attrs.get("history", "")
        wdl.attrs["history"] = history + f"\n{curr_time} > Tidied via monet.tidy (lon_name={lon_name})"

        return wdl

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
