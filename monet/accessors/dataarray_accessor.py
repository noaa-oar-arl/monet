"""DataArray accessor for MONET functionality."""

import numpy as np
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

    def structure_for_monet(self, lat_name="lat", lon_name="lon", return_obj=True):
        """Structure the DataArray for use with MONET functions.

        Parameters
        ----------
        lat_name : str, default: "lat"
            Name of the latitude coordinate.
        lon_name : str, default: "lon"
            Name of the longitude coordinate.
        return_obj : bool, default: True
            Whether to return the restructured object.

        Returns
        -------
        xarray.DataArray or None
            Restructured DataArray if return_obj is True, otherwise None.
        """
        if return_obj:
            return self._dataset_to_monet(self._obj, lat_name=lat_name, lon_name=lon_name)
        else:
            self._obj = self._dataset_to_monet(self._obj, lat_name=lat_name, lon_name=lon_name)

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

    def window(self, lat_min=None, lon_min=None, lat_max=None, lon_max=None, rectilinear=False):
        """Extract a spatial window from the data.

        Parameters
        ----------
        lat_min : float, optional
            Minimum latitude.
        lon_min : float, optional
            Minimum longitude.
        lat_max : float, optional
            Maximum latitude.
        lon_max : float, optional
            Maximum longitude.
        rectilinear : bool, default: False
            Whether the grid is rectilinear.

        Returns
        -------
        xarray.DataArray
            Windowed DataArray.
        """
        try:
            if rectilinear:
                dset = self._dataset_to_monet(self._obj)
                lat = dset.latitude.isel(x=0).values
                lon = dset.longitude.isel(y=0).values
                dset["x"] = lon
                dset["y"] = lat
                # check if latitude is in the correct order
                if dset.latitude.isel(x=0).values[0] > dset.latitude.isel(x=0).values[-1]:
                    lat_min_copy = lat_min
                    lat_min = lat_max
                    lat_max = lat_min_copy
                d = dset.sel(x=slice(lon_min, lon_max), y=slice(lat_min, lat_max))
                return d
            elif has_pyresample:
                from numpy import concatenate
                from pyresample import utils

                dset = self._dataset_to_monet(self._obj)
                lons, lats = utils.check_and_wrap(dset.longitude.values, dset.latitude.values)
                x_ll, y_ll = dset.monet.nearest_ij(lat=float(lat_min), lon=float(lon_min))
                x_ur, y_ur = dset.monet.nearest_ij(lat=float(lat_max), lon=float(lon_max))
                if x_ur < x_ll:
                    x1 = dset.x.where(dset.x >= x_ll, drop=True).values
                    x2 = dset.x.where(dset.x <= x_ur, drop=True).values
                    xrange = concatenate([x1, x2]).astype(int)
                    dset["longitude"][:] = utils.wrap_longitudes(dset.longitude.values)
                else:
                    xrange = slice(x_ll, x_ur)
                if y_ur < y_ll:
                    y1 = dset.y.where(dset.y >= y_ll, drop=True).values
                    y2 = dset.y.where(dset.y <= y_ur, drop=True).values
                    yrange = concatenate([y1, y2]).astype(int)
                else:
                    yrange = slice(y_ll, y_ur)
                return dset.isel(x=xrange, y=yrange)
            else:
                raise ImportError
        except ImportError:
            print(
                """If this is a rectilinear grid and you don't have pyresample
                  please add the rectilinear=True to the call.  Otherwise the window
                  functionality is unavailable without pyresample"""
            )

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
            return result.isel(y=0)
        elif has_xesmf:
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
        if has_xesmf:
            from ..util.interp_util import constant_1d_xesmf
            from ..util.resample import resample_xesmf
        from numpy import asarray, linspace, ones

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
            elif has_xesmf:
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
        pswath = npsd(longitude=float(lon), latitude=float(lat))
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
                from ..util.interp_util import lonlat_to_swathdefinition as llsd
                from ..util.interp_util import nearest_point_swathdefinition as npsd
            except ImportError:
                raise ImportError("requires pyresample to be installed")

            lons, lats = utils.check_and_wrap(d.longitude.values, d.latitude.values)
            swath = self._get_CoordinateDefinition(d)
            pswath = npsd(longitude=float(lon), latitude=float(lat))
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

    def quick_imshow(self, map_kws=None, roll_dateline=False, **kwargs):
        """Create a quick imshow plot of the data.

        Parameters
        ----------
        map_kws : dict, optional
            Keyword arguments for map creation.
        roll_dateline : bool, default: False
            Whether to roll the dateline for proper global visualization.
        **kwargs : dict
            Additional keyword arguments for imshow.

        Returns
        -------
        matplotlib.axes.Axes
            The plot axes.
        """
        import cartopy.crs as ccrs
        import matplotlib.pyplot as plt
        import seaborn as sns
        from cartopy.mpl.geoaxes import GeoAxes

        from ..plots import _dynamic_fig_size, _set_outline_patch_alpha
        from ..plots.mapgen import draw_map

        if map_kws is None:
            map_kws = {}

        da = self._dataset_to_monet(self._obj)
        da = self._monet_to_latlon(da)
        crs_p = ccrs.PlateCarree()
        if "crs" not in map_kws:
            map_kws["crs"] = crs_p
        if "figsize" in kwargs:
            map_kws["figsize"] = kwargs["figsize"]
            kwargs.pop("figsize", None)
        else:
            figsize = _dynamic_fig_size(da)
            map_kws["figsize"] = figsize
        if "transform" not in kwargs:
            transform = crs_p
        else:
            transform = kwargs["transform"]
            kwargs.pop("transform", None)
        with sns.plotting_context("notebook", font_scale=1.2):
            if "ax" not in kwargs:
                ax = draw_map(**map_kws)
            else:
                ax = kwargs.pop("ax", None)
                if not isinstance(ax, GeoAxes):
                    raise TypeError("`ax` should be a Cartopy GeoAxes instance")
            _set_outline_patch_alpha(ax)
            if roll_dateline:
                _ = (
                    da.squeeze()
                    .roll(lon=int(len(da.lon) / 2), roll_coords=True)
                    .plot.imshow(ax=ax, transform=transform, **kwargs)
                )
            else:
                _ = da.squeeze().plot.imshow(ax=ax, transform=transform, **kwargs)
            plt.tight_layout()

        return ax

    def quick_map(self, map_kws=None, roll_dateline=False, **kwargs):
        """Create a quick map plot of the data.

        Parameters
        ----------
        map_kws : dict, optional
            Keyword arguments for map creation.
        roll_dateline : bool, default: False
            Whether to roll the dateline for proper global visualization.
        **kwargs : dict
            Additional keyword arguments for plotting.

        Returns
        -------
        matplotlib.axes.Axes
            The plot axes.
        """
        import cartopy.crs as ccrs
        import matplotlib.pyplot as plt
        import seaborn as sns
        from cartopy.mpl.geoaxes import GeoAxes

        from ..plots import _dynamic_fig_size, _set_outline_patch_alpha
        from ..plots.mapgen import draw_map

        if map_kws is None:
            map_kws = {}

        da = self._dataset_to_monet(self._obj)
        crs_p = ccrs.PlateCarree()
        if "crs" not in map_kws:
            map_kws["crs"] = crs_p
        if "figsize" in kwargs:
            map_kws["figsize"] = kwargs["figsize"]
            kwargs.pop("figsize", None)
        else:
            figsize = _dynamic_fig_size(da)
            map_kws["figsize"] = figsize
        transform = kwargs.pop("transform", crs_p)
        with sns.plotting_context("notebook"):
            if "ax" not in kwargs:
                ax = draw_map(**map_kws)
            else:
                ax = kwargs.pop("ax", None)
                if not isinstance(ax, GeoAxes):
                    raise TypeError("`ax` should be a Cartopy GeoAxes instance")
            _set_outline_patch_alpha(ax)
            if roll_dateline:
                _ = da.roll(x=int(len(da.x) / 2), roll_coords=True).plot(
                    x="longitude", y="latitude", ax=ax, transform=transform, **kwargs
                )
            else:
                _ = da.plot(x="longitude", y="latitude", ax=ax, transform=transform, **kwargs)
            plt.tight_layout()

        return ax

    def quick_contourf(self, map_kws=None, roll_dateline=False, **kwargs):
        """Create a quick filled contour plot of the data.

        Parameters
        ----------
        map_kws : dict, optional
            Keyword arguments for map creation.
        roll_dateline : bool, default: False
            Whether to roll the dateline for proper global visualization.
        **kwargs : dict
            Additional keyword arguments for contourf.

        Returns
        -------
        matplotlib.axes.Axes
            The plot axes.
        """
        import cartopy.crs as ccrs
        import matplotlib.pyplot as plt
        import seaborn as sns
        from cartopy.mpl.geoaxes import GeoAxes

        from ..plots import _dynamic_fig_size, _set_outline_patch_alpha
        from ..plots.mapgen import draw_map

        if map_kws is None:
            map_kws = {}

        da = self._dataset_to_monet(self._obj)
        dlon = da.longitude.diff("x")
        if not ((dlon >= 0).all() or (dlon <= 0).all()):  # monotonic
            da["longitude"] = da.longitude % 360  # unwrap longitudes
        crs_p = ccrs.PlateCarree()
        if "crs" not in map_kws:
            map_kws["crs"] = crs_p
        if "figsize" in kwargs:
            map_kws["figsize"] = kwargs["figsize"]
            kwargs.pop("figsize", None)
        else:
            figsize = _dynamic_fig_size(da)
            map_kws["figsize"] = figsize
        if "transform" not in kwargs:
            transform = crs_p
        else:
            transform = kwargs["transform"]
            kwargs.pop("transform", None)
        with sns.plotting_context("notebook"):
            if "ax" not in kwargs:
                ax = draw_map(**map_kws)
            else:
                ax = kwargs.pop("ax", None)
                if not isinstance(ax, GeoAxes):
                    raise TypeError("`ax` should be a Cartopy GeoAxes instance")
            _set_outline_patch_alpha(ax)
            if roll_dateline:
                _ = da.roll(x=int(len(da.x) / 2), roll_coords=True).plot.contourf(
                    x="longitude", y="latitude", ax=ax, transform=transform, **kwargs
                )
            else:
                _ = da.plot.contourf(
                    x="longitude", y="latitude", ax=ax, transform=transform, **kwargs
                )
            plt.tight_layout()

        return ax

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

    def remap_nearest(self, data, radius_of_influence=1e6, **kwargs):
        """Remap data using nearest neighbor interpolation.

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            Data to remap.
        radius_of_influence : float, default: 1e6
            Search radius in meters.
        **kwargs : dict
            Additional keyword arguments for regridding.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Remapped data.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")

        from pyresample import kd_tree

        source_data = self._dataset_to_monet(data)
        target_data = self._dataset_to_monet(self._obj)
        source = self._get_CoordinateDefinition(source_data)
        target = self._get_CoordinateDefinition(target_data)
        r = kd_tree.XArrayResamplerNN(
            source, target, radius_of_influence=radius_of_influence, **kwargs
        )
        r.get_neighbour_info()
        if isinstance(source_data, xr.DataArray):
            result = r.get_sample_from_neighbour_info(source_data)
            result.name = source_data.name
            result["latitude"] = target_data.latitude
            result["longitude"] = target_data.longitude

        elif isinstance(source_data, xr.Dataset):
            results = {}
            for i in source_data.data_vars.keys():
                results[i] = r.get_sample_from_neighbour_info(source_data[i])
            result = xr.Dataset(results)
            if bool(source_data.attrs):
                result.attrs = source_data.attrs
            result.coords["latitude"] = target_data.latitude
            result.coords["longitude"] = target_data.longitude

        return result

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
