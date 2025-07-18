"""DataArray accessor for MONET functionality."""

import pandas as pd
import xarray as xr

from .base import BaseAccessor, has_pyresample, has_xesmf


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

    def wrap_longitudes(self, lon_name="longitude"):
        """Wrap longitude values to [-180, 180).

        Parameters
        ----------
        lon_name : str, default: "longitude"
            Name of the longitude coordinate.

        Returns
        -------
        xarray.DataArray
            DataArray with wrapped longitudes.
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
        xarray.DataArray
            Tidied DataArray.
        """
        d = self._obj
        wd = d.monet.wrap_longitudes(lon_name=lon_name)
        wdl = wd.sortby(wd[lon_name])
        return wdl

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
        xarray.DataArray
            DataArray with converted time coordinate.
        """
        from numpy import vectorize

        da = self._obj

        def cf_to_dt64(x):
            return pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S"))

        if name is None:  # assume 'time' is the column name to transform
            name = "time"
        if isinstance(da[name].to_index(), xr.CFTimeIndex):
            da[name] = xr.apply_ufunc(vectorize(cf_to_dt64), da[name])
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
            return self._dataset_to_monet(self._obj, lat_name=lat_name, lon_name=lon_name, coards_compliant=coards_compliant)
        else:
            self._obj = self._dataset_to_monet(self._obj, lat_name=lat_name, lon_name=lon_name, coards_compliant=coards_compliant)

    def stratify(self, levels, vertical, axis=1):
        """Vertically interpolate data to specified levels.

        Parameters
        ----------
        levels : array-like
            Target vertical levels.
        vertical : xarray.DataArray
            Vertical coordinate values.
        axis : int, default: 1
            Axis along which to interpolate.

        Returns
        -------
        xarray.DataArray
            Vertically interpolated data.
        """
        from ..util.resample import resample_stratify

        if isinstance(vertical, str):
            vertical = self._obj[vertical]

        out = resample_stratify(self._obj, levels, vertical, axis=axis)
        return out

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
        xarray.DataArray
            Interpolated DataArray.
        """
        from numpy import asarray, linspace, ones

        if has_xesmf:
            from ..util.interp_util import constant_1d_xesmf
            from ..util.resample import resample_xesmf

        try:
            if lat is None:
                raise RuntimeError
        except RuntimeError:
            print("Must enter lat value")
        d1 = self._dataset_to_monet(self._obj, lat_name=lat_name, lon_name=lon_name)
        longitude = linspace(d1.longitude.min(), d1.longitude.max(), len(d1.x))
        latitude = ones(longitude.shape) * asarray(lat)
        if has_pyresample:
            d2 = xr.DataArray(
                ones((len(longitude), len(longitude))),
                dims=["lon", "lat"],
                coords=[longitude, latitude],
            )
            d2 = self._dataset_to_monet(d2)
            result = d2.monet.remap_nearest(d1)
            return result
        elif has_xesmf:
            from ..util.interp_util import constant_1d_xesmf
            from ..util.resample import resample_xesmf
            output = constant_1d_xesmf(latitude=latitude, longitude=longitude)
            out = resample_xesmf(self._obj, output, **kwargs)
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
        xarray.DataArray
            Interpolated DataArray.
        """
        from numpy import asarray, linspace, ones
        if has_xesmf:
            from ..util.interp_util import constant_1d_xesmf
            from ..util.resample import resample_xesmf

        try:
            if lon is None:
                raise RuntimeError
        except RuntimeError:
            print("Must enter lon value")
        d1 = self._dataset_to_monet(self._obj)
        latitude = linspace(d1.latitude.min(), d1.latitude.max(), len(d1.y))
        longitude = ones(latitude.shape) * asarray(lon)
        if has_pyresample:
            if has_pyresample:
                d2 = xr.DataArray(
                    ones((len(longitude), len(longitude))),
                    dims=["lon", "lat"],
                    coords=[longitude, latitude],
                )
                d2 = self._dataset_to_monet(d2)
                result = d2.monet.remap_nearest(d1)
                return result.isel(x=0)
        if has_xesmf:
            output = constant_1d_xesmf(latitude=latitude, longitude=longitude)
            out = resample_xesmf(self._obj, output, **kwargs)
            return self._rename_latlon(out)

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
        if not has_pyresample:
            raise ImportError("requires pyresample to be installed")

        try:
            from pyresample import utils

            from ..util.interp_util import lonlat_to_swathdefinition as llsd
            from ..util.interp_util import nearest_point_swathdefinition as npsd
        except ImportError:
            raise ImportError("requires pyresample to be installed")

        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print("Must provide latitude and longitude")

        dset = self._dataset_to_monet(self._obj)
        lons, lats = utils.check_and_wrap(dset.longitude.values, dset.latitude.values)
        swath = llsd(longitude=lons, latitude=lats)
        if lon is None or lat is None:
            raise ValueError("Longitude and latitude must not be None.")
        try:
            lon_f = float(lon)
            lat_f = float(lat)
        except (TypeError, ValueError):
            raise ValueError("Longitude and latitude must be convertible to float and not None.")
        pswath = npsd(longitude=lon_f, latitude=lat_f)
        row, col = utils.generate_nearest_neighbour_linesample_arrays(swath, pswath, float(1e6))
        y, x = row[0][0], col[0][0]
        return x, y

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
        xarray.DataArray
            DataArray at nearest point(s).
        """
        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print("Must provide latitude and longitude")

        d = self._dataset_to_monet(self._obj)
        if has_pyresample:
            try:
                from pyresample import utils

                from ..util.interp_util import nearest_point_swathdefinition as npsd
            except ImportError:
                raise ImportError("requires pyresample to be installed")

            lons, lats = utils.check_and_wrap(d.longitude.values, d.latitude.values)
            swath = self._get_CoordinateDefinition(d)
            # Ensure lon and lat are not None and are convertible to float
            if lon is None or lat is None:
                raise ValueError("Longitude and latitude must not be None.")
            try:
                lon_f = float(lon)
                lat_f = float(lat)
            except (TypeError, ValueError):
                raise ValueError("Longitude and latitude must be convertible to float and not None.")
            pswath = npsd(longitude=lon_f, latitude=lat_f)
            row, col = utils.generate_nearest_neighbour_linesample_arrays(swath, pswath, **kwargs)
            y, x = row[0][0], col[0][0]
            return d.isel(x=x, y=y)
        elif has_xesmf:
            kwargs = self._check_kwargs_and_set_defaults(**kwargs)
            self._obj = self._rename_latlon(self._obj)
            from ..util.interp_util import lonlat_to_xesmf
            from ..util.resample import resample_xesmf

            target = lonlat_to_xesmf(longitude=lon, latitude=lat)
            output = resample_xesmf(self._obj, target, **kwargs)
            if cleanup:
                output = resample_xesmf(self._obj, target, cleanup=True, **kwargs)
            return self._rename_latlon(output.squeeze())

    def quick_imshow(
        self,
        map_kws=None,
        roll_dateline=False,
        projection=None,
        colorbar=True,
        figsize=None,
        **kwargs,
    ):
        """Create a quick imshow plot of the data with flexible options."""
        from ..plots.cartopy_utils import plot_quick_imshow

        da = self._dataset_to_monet(self._obj)
        return plot_quick_imshow(
            da, map_kws=map_kws, projection=projection, colorbar=colorbar, figsize=figsize, **kwargs
        )

    def quick_map(
        self,
        map_kws=None,
        roll_dateline=False,
        projection=None,
        colorbar=True,
        figsize=None,
        **kwargs,
    ):
        """Create a quick map plot of the data with flexible options."""
        from ..plots.cartopy_utils import plot_quick_map

        da = self._dataset_to_monet(self._obj)
        return plot_quick_map(
            da, map_kws=map_kws, projection=projection, colorbar=colorbar, figsize=figsize, **kwargs
        )

    def quick_contourf(
        self,
        map_kws=None,
        roll_dateline=False,
        projection=None,
        colorbar=True,
        figsize=None,
        **kwargs,
    ):
        """Create a quick filled contour plot of the data with flexible options."""
        from ..plots.cartopy_utils import plot_quick_contourf

        da = self._dataset_to_monet(self._obj)
        return plot_quick_contourf(
            da, map_kws=map_kws, projection=projection, colorbar=colorbar, figsize=figsize, **kwargs
        )

    def _tight_layout(self):
        """Apply tight layout to the current figure.

        Returns
        -------
        None
        """
        from matplotlib.pyplot import subplots_adjust

        subplots_adjust(0, 0, 1, 1)

    def _check_swath_def(self, defn):
        """Check if a SwathDefinition is valid.

        Parameters
        ----------
        defn : object
            Object to check if it's a valid SwathDefinition.

        Returns
        -------
        bool
            True if valid, False otherwise.
        """
        if not has_pyresample:
            return False

        from pyresample.geometry import SwathDefinition

        return isinstance(defn, SwathDefinition)

    def remap(self, data, method="nearest", radius_of_influence=1e6, **kwargs):
        """Remap data using pyresample (nearest or bilinear).

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            Data to remap.
        method : str, default: 'nearest'
            Resampling method: 'nearest' or 'bilinear'.
        radius_of_influence : float, default: 1e6
            Search radius in meters (for both methods).
        **kwargs : dict
            Additional keyword arguments for the resampler.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Remapped data.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")
        from ..util import resample

        source_data = self._dataset_to_monet(data)
        target_data = self._dataset_to_monet(self._obj)
        target = self._get_CoordinateDefinition(target_data)
        result = resample.resample(
            source_data, target, method=method, radius_of_influence=radius_of_influence, **kwargs
        )
        # Ensure coordinates are properly set
        if isinstance(result, xr.DataArray):
            result["latitude"] = target_data.latitude
            result["longitude"] = target_data.longitude
            result.name = source_data.name
        elif isinstance(result, xr.Dataset):
            result.coords["latitude"] = target_data.latitude
            result.coords["longitude"] = target_data.longitude
        return result

    def remap_nearest(self, data, radius_of_influence=1e6, **kwargs):
        """Remap data using nearest neighbor interpolation (wrapper for remap)."""
        return self.remap(data, method="nearest", radius_of_influence=radius_of_influence, **kwargs)

    def remap_xesmf(self, data, **kwargs):
        """Remap data using xESMF regridding.

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            Data to remap.
        **kwargs : dict
            Keyword arguments for xESMF regridding.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Remapped data.
        """
        kwargs["method"] = kwargs.get("method", "bilinear")
        if has_xesmf:
            from ..util import resample

            target = self._rename_latlon(self._obj)
            source = self._rename_latlon(data)
            out = resample.resample_xesmf(source, target, **kwargs)
            return self._rename_to_monet_latlon(out)

        else:
            print("xesmf unavailable. Try `import xesmf` and check the failure message.")

    def combine_point(self, data, suffix=None, pyresample=True, **kwargs):
        """Combine point data with this DataArray.

        Parameters
        ----------
        data : pandas.DataFrame
            Point data to combine.
        suffix : str, optional
            Suffix to add to variable names. Default is '_new'.
        pyresample : bool, default: True
            Whether to use pyresample for remapping.
        **kwargs : dict
            Additional keyword arguments for regridding.

        Returns
        -------
        pandas.DataFrame
            Combined data.
        """
        if not has_pyresample and not has_xesmf:
            raise ImportError("Either pyresample or xesmf is required for this functionality")

        if has_pyresample:
            from ..util.combinetool import combine_da_to_df
        if has_xesmf:
            from ..util.combinetool import combine_da_to_df_xesmf
        da = self._dataset_to_monet(self._obj)
        if isinstance(data, pd.DataFrame):
            if has_pyresample and pyresample:
                return combine_da_to_df(da, data, **kwargs)
            else:  # xesmf resample
                return combine_da_to_df_xesmf(da, data, suffix=suffix, **kwargs)
        else:
            print("`data` must be a pandas.DataFrame")

    def remap_nearest_parallel(self, data, radius_of_influence=1e6, n_processes=None, **kwargs):
        """Remap data using nearest neighbor interpolation with parallel processing.

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            Data to remap.
        radius_of_influence : float, default: 1e6
            Search radius in meters.
        n_processes : int, optional
            Number of processes to use. If None, uses all available cores.
        **kwargs : dict
            Additional keyword arguments for regridding.

        Returns
        -------
        xarray.DataArray
            Remapped data array.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")

        from ..util import resample

        source_data = self._dataset_to_monet(data)
        target_data = self._dataset_to_monet(self._obj)
        target = self._get_CoordinateDefinition(target_data)

        result = resample.resample_pyresample_parallel(
            source_data,
            target,
            radius_of_influence=radius_of_influence,
            n_processes=n_processes,
            **kwargs,
        )
        return result

    def combine_point_esmf(self, point_df, method="bilinear", **kwargs):
        """Combine this DataArray with point data using ESMF LocStream.

        Parameters
        ----------
        point_df : pandas.DataFrame
            DataFrame containing point observations with latitude, longitude, and siteid columns.
        method : str, default: 'bilinear'
            Regridding method to use.
        **kwargs : dict
            Additional keyword arguments for ESMF regridding.

        Returns
        -------
        pandas.DataFrame
            Combined dataframe with grid data interpolated to point locations.
        """
        from ..util.combinetool import combine_grid_to_point_esmf

        grid_data = self._dataset_to_monet(self._obj)

        return combine_grid_to_point_esmf(grid_data, point_df, method=method, **kwargs)

    def to_area_def(self, projection="platea", resolution=None, area_id=None):
        """Convert the dataarray's coordinates to a pyresample AreaDefinition.

        Parameters
        ----------
        projection : str, default: 'platea'
            Projection name. Options include:
            - 'platea': Plate Carrée (equidistant cylindrical)
            - 'lcc': Lambert Conformal Conic
            - 'merc': Mercator
            - 'stere': Stereographic
            - 'gnom': Gnomonic (used by UFS SRW)
            - 'auto': Try to determine from data attributes
        resolution : float, optional
            Resolution in meters. If None, calculated from data.
        area_id : str, optional
            Identifier for the area.

        Returns
        -------
        pyresample.geometry.AreaDefinition
            An AreaDefinition object representing this dataarray's grid.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")

        from ..util.interp_util import guess_area_def_from_dataset

        return guess_area_def_from_dataset(
            self._obj, projection=projection, resolution=resolution, area_id=area_id
        )

    def to_swath_def(self):
        """Convert the dataarray's coordinates to a pyresample SwathDefinition.

        This is particularly useful for unstructured or irregular grids.

        Returns
        -------
        pyresample.geometry.SwathDefinition
            A SwathDefinition object representing this dataarray's grid.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")

        # Process as UGRID if it has mesh topology
        for var in self._obj.coords:
            if hasattr(self._obj[var], "cf_role") and self._obj[var].cf_role == "mesh_topology":
                from ..util.interp_util import ugrid_to_swath_definition

                return ugrid_to_swath_definition(self._obj)

        # Otherwise use standard methods
        da = self._dataset_to_monet(self._obj)
        return self._get_CoordinateDefinition(da)

    def compare(
        self,
        other,
        stat="diff",
        plot=True,
        plot_method="quick_map",
        stat_kwargs=None,
        plot_kwargs=None,
    ):
        """
        Compute and optionally plot a statistic between this DataArray and another,
        leveraging MONET's util.stats metrics.

        Parameters
        ----------
        other : xarray.DataArray
            The other DataArray to compare with.
        stat : str or callable, default: "diff"
            Statistic to compute. Can be any metric name from monet.util.stats
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
        import importlib

        import numpy as np

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
                # Try to get the function from monet.util.stats
                try:
                    stats_mod = importlib.import_module("monet.util.stats")
                    func = getattr(stats_mod, stat)
                    stat_da = func(da1, da2, **stat_kwargs)
                except (ImportError, AttributeError) as e:
                    # fallback to built-in
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
        stat_da.name = stat if isinstance(stat, str) else getattr(stat, "__name__", "statistic")
        if plot:
            plot_func = getattr(stat_da.monet, plot_method)
            return plot_func(**plot_kwargs)
        else:
            return stat_da

    def quick_facet_time_map(
        self,
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
        Create a facet grid of map plots for each time slice in a DataArray using Cartopy.

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

        da = self._dataset_to_monet(self._obj)
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
