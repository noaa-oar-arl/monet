"""DataArray accessor for MONET functionality."""

import datetime
import typing as t
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from .base import BaseAccessor, has_monet_regrid, has_xregrid


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

    def wrap_longitudes(self, lon_name: str | None = None) -> xr.DataArray:
        """Wrap longitude values to [-180, 180).
        Convention-aware: auto-detects longitude if lon_name is None.

        Parameters
        ----------
        lon_name : str, optional
            Name of the longitude coordinate. If None, auto-detects.

        Returns
        -------
        xarray.DataArray
            DataArray with wrapped longitudes.
        """
        if lon_name is None:
            _, lon_name = self._detect_latlon_names(self._obj)
            if lon_name is None:
                raise ValueError("Could not detect longitude coordinate.")

        da = self._obj.copy()
        da[lon_name] = (da[lon_name] + 180) % 360 - 180

        # Update history for provenance
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = da.attrs.get("history", "")
        da.attrs["history"] = history + f"\n{curr_time} > Wrapped longitudes ({lon_name}) via monet.wrap_longitudes"

        return da

    def tidy(self, lon_name: str | None = None) -> xr.DataArray:
        """Apply tidying operations to the data.
        Wraps longitudes and sorts by longitude.
        Convention-aware: auto-detects longitude if lon_name is None.

        Parameters
        ----------
        lon_name : str, optional
            Name of the longitude coordinate. If None, auto-detects.

        Returns
        -------
        xarray.DataArray
            Tidied DataArray.
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

    def is_land(self, return_xarray: bool = False) -> xr.DataArray | np.ndarray:
        """Check if points are on land.
        Supports both Eager (NumPy) and Lazy (Dask) backends via ``xarray.apply_ufunc``.
        Convention-aware: works with CF/COARDS and UGRID without forced renaming.

        Parameters
        ----------
        return_xarray : bool, default: False
            If True, return results as xarray (masked DataArray).
            Otherwise, return the boolean mask (DataArray or its underlying array).

        Returns
        -------
        xarray.DataArray or numpy.ndarray
            If return_xarray is True, returns a DataArray masked by land.
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

    def is_ocean(self, return_xarray: bool = False) -> xr.DataArray | np.ndarray:
        """Check if points are on ocean.
        Supports both Eager (NumPy) and Lazy (Dask) backends via ``xarray.apply_ufunc``.
        Convention-aware: works with CF/COARDS and UGRID without forced renaming.

        Parameters
        ----------
        return_xarray : bool, default: False
            If True, return results as xarray (masked DataArray).
            Otherwise, return the boolean mask (DataArray or its underlying array).

        Returns
        -------
        xarray.DataArray or numpy.ndarray
            If return_xarray is True, returns a DataArray masked by ocean.
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

    def cftime_to_datetime64(self, name: str | None = None) -> xr.DataArray:
        """Convert cftime coordinates to numpy datetime64.
        Preserves Dask laziness if the time coordinate is Dask-backed.

        Parameters
        ----------
        name : str, optional
            Name of the coordinate to convert. If None, tries to detect the time coordinate.

        Returns
        -------
        xarray.DataArray
            DataArray with converted time coordinate.
        """
        from numpy import vectorize

        da = self._obj.copy()

        def cf_to_dt64(x):
            try:
                return pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S"))
            except AttributeError:
                return x

        if name is None:  # assume 'time' is the column name to transform
            name = "time"

        if name in da.coords and isinstance(da[name].to_index(), xr.CFTimeIndex):
            da[name] = xr.apply_ufunc(
                vectorize(cf_to_dt64),
                da[name],
                dask="parallelized",
                output_dtypes=["datetime64[ns]"],
            )

            # Update history
            curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history = da.attrs.get("history", "")
            da.attrs["history"] = history + f"\n{curr_time} > Converted {name} from cftime to datetime64"

        return da

    def structure_for_monet(self, lat_name="lat", lon_name="lon", return_obj=True, coards_compliant=False):
        """Structure the DataArray for use with MONET functions.

        Parameters
        ----------
        lat_name : str, default: "lat"
            Name of the latitude coordinate.
        lon_name : str, default: "lon"
            Name of the longitude coordinate.
        return_obj : bool, default: True
            Whether to return the restructured object.
        coards_compliant : bool, default: False
            Whether to enforce COARDS compliance.

        Returns
        -------
        xarray.DataArray or None
            Restructured DataArray if return_obj is True, otherwise None.
        """
        if return_obj:
            return self._dataset_to_monet(
                self._obj,
                lat_name=lat_name,
                lon_name=lon_name,
                coards_compliant=coards_compliant,
            )
        else:
            self._obj = self._dataset_to_monet(
                self._obj,
                lat_name=lat_name,
                lon_name=lon_name,
                coards_compliant=coards_compliant,
            )

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

    def interp_constant_lat(
        self,
        lat: float | None = None,
        **kwargs: t.Any,
    ) -> xr.DataArray:
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
        xarray.DataArray
            Interpolated DataArray.
        """
        from numpy import asarray, linspace, ones

        if lat is None:
            raise ValueError("Must provide a latitude value ('lat')")

        da = self._obj.copy()
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

        out = resample(da, target, **kwargs)

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = out.attrs.get("history", "")
        out.attrs["history"] = history + f"\n{curr_time} > Interpolated to constant latitude: {lat}"

        return out

    def interp_constant_lon(self, lon: float | None = None, **kwargs: t.Any) -> xr.DataArray:
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
        xarray.DataArray
            Interpolated DataArray.
        """
        from numpy import asarray, linspace, ones

        if lon is None:
            raise ValueError("Must provide a longitude value ('lon')")

        da = self._obj.copy()
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

        out = resample(da, target, **kwargs)

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = out.attrs.get("history", "")
        out.attrs["history"] = history + f"\n{curr_time} > Interpolated to constant longitude: {lon}"

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

    def nearest_latlon(
        self,
        lat: float | t.Sequence[float] | None = None,
        lon: float | t.Sequence[float] | None = None,
        cleanup: bool = True,
        esmf: bool = False,
        **kwargs: t.Any,
    ) -> xr.DataArray:
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
        xarray.DataArray
            DataArray at nearest point(s).
        """
        if lat is None or lon is None:
            raise ValueError("Must provide latitude and longitude")

        da = self._obj.copy()

        # Use xregrid via resample
        from ..util.interp_util import points_to_dataset
        from ..util.resample import resample

        # Create target grid
        target = points_to_dataset(latitude=lat, longitude=lon)
        output = resample(da, target, method="nearest", **kwargs)

        res = output.squeeze()

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = res.attrs.get("history", "")
        res.attrs["history"] = history + f"\n{curr_time} > Extracted nearest lat/lon points"

        return res

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
            Data to remap.
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
        >>> da.monet.remap(target_ds, method='bilinear')
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

    def pair(self, obs, **kwargs):
        """Pair this DataArray with observation data.

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
        """Combine point data with this DataArray.

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
