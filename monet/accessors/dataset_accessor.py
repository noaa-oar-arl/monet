"""Dataset accessor for MONET functionality."""

import numpy as np
import pandas as pd
import xarray as xr

from .base import BaseAccessor, has_pyresample, has_xesmf


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
        """Remap data using xESMF regridding with optional parallelization.

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            Data to remap.
        parallel : bool, default: True
            Whether to use parallel processing via dask.
        n_workers : int, optional
            Number of dask workers to use. If None, uses all available cores.
        **kwargs : dict
            Keyword arguments for xESMF regridding.

        Returns
        -------
        xarray.Dataset
            Remapped dataset.
        """
        if not has_xesmf:
            raise ImportError("xesmf is required for this functionality")

        try:
            from ..util import resample

            kwargs["method"] = kwargs.get("method", "bilinear")
            target = self._rename_latlon(self._obj)
            source = self._rename_latlon(data)

            out = resample.resample_xesmf(
                source, target, parallel=parallel, n_workers=n_workers, **kwargs
            )

            return self._rename_to_monet_latlon(out)
        except Exception as e:
            print(f"Error remapping with xESMF: {e}")

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
        xarray.Dataset
            Remapped dataset.
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

        # Ensure coordinates are properly set
        if isinstance(result, xr.Dataset):
            result.coords["latitude"] = target_data.latitude
            result.coords["longitude"] = target_data.longitude

        return result

    def combine_point_esmf(self, point_df, method="bilinear", **kwargs):
        """Combine this dataset with point data using ESMF LocStream.

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

    def _remap_xesmf_dataset(self, dset, filename="monet_xesmf_regrid_file.nc", **kwargs):
        """Remap dataset using xESMF.

        Parameters
        ----------
        dset : xarray.Dataset
            Dataset to remap.
        filename : str, default: "monet_xesmf_regrid_file.nc"
            Name of temporary file for regridding weights.
        **kwargs : dict
            Keyword arguments for xESMF regridding.

        Returns
        -------
        xarray.Dataset
            Remapped dataset.
        """
        skip_keys = ["lat", "lon", "time", "TFLAG"]
        vars = pd.Series(list(dset.variables))
        loop_vars = vars.loc[~vars.isin(skip_keys)]
        dataarray = dset[loop_vars[0]]
        da = self._remap_xesmf_dataarray(dataarray, filename=filename, **kwargs)
        # Ensure da is a DataArray with a name
        das = {}
        if hasattr(da, "name") and da.name is not None:
            das[da.name] = da
        else:
            das["var0"] = da
        for idx, i in enumerate(loop_vars[1:], start=1):
            dataarray = dset[i]
            tmp = self._remap_xesmf_dataarray(
                dataarray, filename=filename, reuse_weights=True, **kwargs
            )
            key = tmp.name if hasattr(tmp, "name") and tmp.name is not None else f"var{idx}"
            das[key] = tmp.copy()
        return xr.Dataset(das)

    def _remap_xesmf_dataarray(
        self, dataarray, method="bilinear", filename="monet_xesmf_regrid_file.nc", **kwargs
    ):
        """Remap DataArray using xESMF.

        Parameters
        ----------
        dataarray : xarray.DataArray
            DataArray to remap.
        method : str, default: "bilinear"
            Regridding method.
        filename : str, default: "monet_xesmf_regrid_file.nc"
            Name of temporary file for regridding weights.
        **kwargs : dict
            Keyword arguments for xESMF regridding.

        Returns
        -------
        xarray.DataArray
            Remapped DataArray.
        """
        from ..util import resample

        target = self._obj
        out = resample.resample_xesmf(dataarray, target, method=method, filename=filename, **kwargs)
        if out.name in self._obj.variables:
            out.name = out.name + "_y"
        self._obj[out.name] = out
        return out

    def remap(self, data, method="nearest", radius_of_influence=1e6, **kwargs):
        """Remap data using pyresample (nearest or bilinear) or xESMF if requested.

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            Data to remap.
        method : str, default: 'nearest'
            Resampling method: 'nearest', 'bilinear', or 'xesmf'.
        radius_of_influence : float, default: 1e6
            Search radius in meters (for pyresample methods).
        **kwargs : dict
            Additional keyword arguments for the resampler.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Remapped data.
        """
        from ..util import resample
        import xarray as xr

        # Always use xESMF for Dask-backed arrays if available
        is_dask = hasattr(self._obj, "chunks") and self._obj.chunks is not None
        # Only use xESMF for Dask-backed arrays when target shape differs from source
        target_shape = None
        if hasattr(data, "shape"):
            target_shape = data.shape
        source_shape = self._obj.shape if hasattr(self._obj, "shape") else None
        # For Dask-backed arrays, always use xESMF if target shape differs from source
        if is_dask and target_shape is not None and target_shape != source_shape:
            if not has_xesmf:
                raise ImportError("xesmf is required for Dask-backed remapping to different-shaped grid")
            xesmf_method_map = {
                "nearest": "nearest_s2d",
                "bilinear": "bilinear",
                "xesmf": kwargs.get("xesmf_method", "bilinear"),
            }
            xesmf_method = xesmf_method_map.get(method, "bilinear")
            source = self._dataset_to_monet(self._obj)
            target = self._dataset_to_monet(data)
            from ..util.interp_util import lonlat_to_xesmf
            lat = target.latitude.values if hasattr(target, 'latitude') else target.lat.values
            lon = target.longitude.values if hasattr(target, 'longitude') else target.lon.values
            target_xesmf = lonlat_to_xesmf(longitude=lon, latitude=lat)
            source = source.chunk()
            target_xesmf = target_xesmf.chunk()
            out = resample.resample_xesmf(source, target_xesmf, method=xesmf_method, **kwargs)
            return self._rename_to_monet_latlon(out)
        # Otherwise, use xESMF if requested, or pyresample for same-shaped grids
        use_xesmf = (method == "xesmf") or (has_xesmf and is_dask and target_shape == source_shape)
        if use_xesmf:
            if not has_xesmf:
                raise ImportError("xesmf is required for this functionality")
            xesmf_method_map = {
                "nearest": "nearest_s2d",
                "bilinear": "bilinear",
                "xesmf": kwargs.get("xesmf_method", "bilinear"),
            }
            xesmf_method = xesmf_method_map.get(method, "bilinear")
            source = self._dataset_to_monet(self._obj)
            target = self._dataset_to_monet(data)
            from ..util.interp_util import lonlat_to_xesmf
            lat = target.latitude.values if hasattr(target, 'latitude') else target.lat.values
            lon = target.longitude.values if hasattr(target, 'longitude') else target.lon.values
            target_xesmf = lonlat_to_xesmf(longitude=lon, latitude=lat)
            source = source.chunk()
            target_xesmf = target_xesmf.chunk()
            out = resample.resample_xesmf(source, target_xesmf, method=xesmf_method, **kwargs)
            # xESMF output should already match target grid shape; do not transpose
            return self._rename_to_monet_latlon(out)

        # Otherwise, use pyresample
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")
        source_data = self._dataset_to_monet(data)
        target_data = self._dataset_to_monet(self._obj)
        target = self._get_CoordinateDefinition(target_data)
        result = resample.resample(
            source_data, target, method=method, radius_of_influence=radius_of_influence, **kwargs
        )
        # Ensure coordinates are properly set
        if isinstance(result, xr.DataArray):
            # Remove any old latitude/longitude coordinates to avoid conflicts
            for coord in ["latitude", "longitude"]:
                if coord in result.coords:
                    result = result.drop_vars(coord)
            # Assign latitude/longitude from the target grid, matching dims
            if hasattr(target_data, "latitude") and hasattr(target_data, "longitude"):
                lat = target_data.latitude
                lon = target_data.longitude
                lat_data = getattr(lat, 'data', getattr(lat, 'values', lat))
                lon_data = getattr(lon, 'data', getattr(lon, 'values', lon))
                if lat.shape == result.shape[-2:] and lon.shape == result.shape[-2:]:
                    y_dim, x_dim = result.dims[-2], result.dims[-1]
                    result = result.assign_coords({
                        "latitude": (y_dim, lat_data[:,0] if lat.ndim==2 else lat_data),
                        "longitude": (x_dim, lon_data[0,:] if lon.ndim==2 else lon_data)
                    })
                else:
                    if lat.ndim == 1 and lat.shape[0] == result.shape[-2]:
                        y_dim = result.dims[-2]
                        result = result.assign_coords({"latitude": (y_dim, lat_data)})
                    if lon.ndim == 1 and lon.shape[0] == result.shape[-1]:
                        x_dim = result.dims[-1]
                        result = result.assign_coords({"longitude": (x_dim, lon_data)})
            result.name = source_data.name
        elif isinstance(result, xr.Dataset):
            for coord in ["latitude", "longitude"]:
                if coord in result.coords:
                    result = result.drop_vars(coord)
            if hasattr(target_data, "latitude") and hasattr(target_data, "longitude"):
                lat = target_data.latitude
                lon = target_data.longitude
                lat_data = getattr(lat, 'data', getattr(lat, 'values', lat))
                lon_data = getattr(lon, 'data', getattr(lon, 'values', lon))
                if lat.ndim == 2 and lon.ndim == 2 and lat.shape == result[list(result.data_vars)[0]].shape[-2:]:
                    y_dim, x_dim = result[list(result.data_vars)[0]].dims[-2], result[list(result.data_vars)[0]].dims[-1]
                    result = result.assign_coords({
                        "latitude": (y_dim, lat_data[:,0] if lat.ndim==2 else lat_data),
                        "longitude": (x_dim, lon_data[0,:] if lon.ndim==2 else lon_data)
                    })
                else:
                    if lat.ndim == 1 and lat.shape[0] == result[list(result.data_vars)[0]].shape[-2]:
                        y_dim = result[list(result.data_vars)[0]].dims[-2]
                        result = result.assign_coords({"latitude": (y_dim, lat_data)})
                    if lon.ndim == 1 and lon.shape[0] == result[list(result.data_vars)[0]].shape[-1]:
                        x_dim = result[list(result.data_vars)[0]].dims[-1]
                        result = result.assign_coords({"longitude": (x_dim, lon_data)})
        return result

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
        xarray.Dataset
            Remapped dataset.
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

    def remap_nearest_unstructured(self, data):
        """Remap unstructured grid data using nearest neighbor interpolation.

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            Unstructured grid data to remap.

        Returns
        -------
        xarray.Dataset
            Remapped dataset.
        """
        try:
            check_error = False
            if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
                check_error = False
            else:
                check_error = True
            if check_error:
                raise TypeError
        except TypeError:
            print("data must be either an xarray.DataArray or xarray.Dataset")

        model_data = data
        obs_data = self._obj

        site_indices = []
        site_latitudes = obs_data["latitude"].values[0, :]
        site_longitudes = obs_data["longitude"].values[0, :]
        model_latitudes = model_data["latitude"].values
        model_longitudes = model_data["longitude"].values

        for siteii in np.arange(len(obs_data["siteid"][0])):
            site_indices.append(
                np.argmin(
                    np.abs(site_latitudes[siteii] - model_latitudes)
                    + np.abs(site_longitudes[siteii] - model_longitudes)
                )
            )

        dict_data = {}
        for dvar in model_data.data_vars:
            if dvar in ["latitude", "longitude"]:
                continue
            else:
                dict_data[dvar] = (
                    ["time", "z", "y", "x"],
                    model_data[dvar][:, 0, np.array(site_indices)].values.reshape(
                        len(model_data["time"]), 1, 1, len(site_indices)
                    ),
                )

        dict_coords = {
            "time": (["time"], model_data["time"].values),
            "x": (["x"], np.arange(len(site_indices))),
            "longitude": (
                ["y", "x"],
                model_longitudes[np.array(site_indices)].reshape(1, len(site_indices)),
            ),
            "latitude": (
                ["y", "x"],
                model_latitudes[np.array(site_indices)].reshape(1, len(site_indices)),
            ),
        }

        result = xr.Dataset(data_vars=dict_data, coords=dict_coords)

        return result

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
            raise ImportError("pyresample is required for this functionality")

        try:
            from pyresample import utils

            from ..util.interp_util import lonlat_to_swathdefinition as llsd
            from ..util.interp_util import nearest_point_swathdefinition as npsd
        except ImportError:
            raise ImportError("pyresample is required for this functionality")

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
        xarray.Dataset
            Dataset at nearest point(s).
        """
        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print("Must provide latitude and longitude")

        if has_pyresample:
            try:
                from pyresample import utils

                from ..util.interp_util import lonlat_to_swathdefinition as llsd
                from ..util.interp_util import nearest_point_swathdefinition as npsd
            except ImportError:
                raise ImportError("pyresample is required for this functionality")

            dset = self._dataset_to_monet(self._obj)
            lons, lats = utils.check_and_wrap(dset.longitude.values, dset.latitude.values)
            swath = llsd(longitude=lons, latitude=lats)
            pswath = npsd(longitude=float(lon), latitude=float(lat))
            row, col = utils.generate_nearest_neighbour_linesample_arrays(swath, pswath, float(1e6))
            y, x = row[0][0], col[0][0]
            return dset.isel(x=x, y=y)
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
        else:
            raise ImportError("Either pyresample or xesmf is required for this functionality")

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
            from ..util.interp_util import constant_1d_xesmf
            from ..util.resample import resample_xesmf

            output = constant_1d_xesmf(latitude=latitude, longitude=longitude)
            out = resample_xesmf(self._obj, output, **kwargs)
            return self._rename_latlon(out)
        else:
            raise ImportError("Either pyresample or xesmf is required for this functionality")

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
            from ..util.interp_util import constant_1d_xesmf
            from ..util.resample import resample_xesmf

            output = constant_1d_xesmf(latitude=latitude, longitude=longitude)
            out = resample_xesmf(self._obj, output, **kwargs)
            return self._rename_latlon(out)
        else:
            raise ImportError("Either pyresample or xesmf is required for this functionality")

    def stratify(self, levels, vertical, axis=1):
        """Vertically interpolate data to specified levels.

        Parameters
        ----------
        levels : array-like
            Target vertical levels.
        vertical : xarray.DataArray or str
            Vertical coordinate values or name of the vertical coordinate.
        axis : int, default: 1
            Axis along which to interpolate.

        Returns
        -------
        xarray.Dataset
            Vertically interpolated dataset.
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
            raise ValueError(
                "No variables found with vertical dimension matching the provided coordinate"
            )

        from ..util.resample import resample_stratify

        orig = resample_stratify(self._obj[loop_vars[0]], levels, vertical, axis=axis)
        dset = orig.to_dataset(name=loop_vars[0])
        dset.attrs = self._obj.attrs.copy()

        for vn in loop_vars[1:]:
            dset[vn] = resample_stratify(self._obj[vn], levels, vertical, axis=axis)

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
        if has_pyresample:
            try:
                from numpy import concatenate
                from pyresample import utils

                from ..util.interp_util import lonlat_to_swathdefinition as llsd
                from ..util.interp_util import nearest_point_swathdefinition as npsd

                dset = self._dataset_to_monet(self._obj)
                lons, lats = utils.check_and_wrap(dset.longitude.values, dset.latitude.values)
                swath = llsd(longitude=lons, latitude=lats)
                pswath_ll = npsd(longitude=float(lon_min), latitude=float(lat_min))
                pswath_ur = npsd(longitude=float(lon_max), latitude=float(lat_max))

                row, col = utils.generate_nearest_neighbour_linesample_arrays(
                    swath, pswath_ll, float(1e6)
                )
                y_ll, x_ll = row[0][0], col[0][0]

                row, col = utils.generate_nearest_neighbour_linesample_arrays(
                    swath, pswath_ur, float(1e6)
                )
                y_ur, x_ur = row[0][0], col[0][0]

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
            except ImportError:
                raise ImportError("pyresample is required for this functionality")
        else:
            raise ImportError("Window functionality is unavailable without pyresample")

    def combine_point(self, data, suffix=None, pyresample=True, **kwargs):
        """Combine point data with this Dataset.

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
        if not isinstance(data, pd.DataFrame):
            raise TypeError("`data` must be a pandas.DataFrame")

        if has_pyresample and pyresample:
            from ..util.combinetool import combine_da_to_df

            da = self._dataset_to_monet(self._obj)
            return combine_da_to_df(da, data, **kwargs)
        elif has_xesmf:
            from ..util.combinetool import combine_da_to_df_xesmf

            da = self._dataset_to_monet(self._obj)
            return combine_da_to_df_xesmf(da, data, suffix=suffix, **kwargs)
        else:
            raise ImportError("Either pyresample or xesmf is required for this functionality")

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
        """Convert the dataset's coordinates to a pyresample AreaDefinition.

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
            An AreaDefinition object representing this dataset's grid.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")

        from ..util.interp_util import guess_area_def_from_dataset

        return guess_area_def_from_dataset(
            self._obj, projection=projection, resolution=resolution, area_id=area_id
        )

    def to_swath_def(self):
        """Convert the dataset's coordinates to a pyresample SwathDefinition.

        This is particularly useful for unstructured or irregular grids.

        Returns
        -------
        pyresample.geometry.SwathDefinition
            A SwathDefinition object representing this dataset's grid.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")

        # Process as UGRID if it has mesh topology
        for var in self._obj.variables:
            if hasattr(self._obj[var], "cf_role") and self._obj[var].cf_role == "mesh_topology":
                from ..util.interp_util import ugrid_to_swath_definition

                return ugrid_to_swath_definition(self._obj)

        # Otherwise use standard methods
        ds = self._dataset_to_monet(self._obj)
        return self._get_CoordinateDefinition(ds)

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
