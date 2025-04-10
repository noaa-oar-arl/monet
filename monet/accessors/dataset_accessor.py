"""Dataset accessor for MONET functionality."""

import numpy as np
import pandas as pd
import xarray as xr

from .base import BaseAccessor, has_pyresample, has_xesmf

@xr.register_dataset_accessor("monet")
class MONETAccessorDataset(BaseAccessor):
    """Dataset accessor for MONET functionality."""

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
        xarray.Dataset
            Remapped dataset.
        """
        if not has_xesmf:
            raise ImportError("xesmf is required for this functionality")

        try:
            if isinstance(data, xr.DataArray):
                data = self._rename_latlon(data)
                return self._remap_xesmf_dataarray(data, **kwargs)
            elif isinstance(data, xr.Dataset):
                data = self._rename_latlon(data)
                return self._remap_xesmf_dataset(data, **kwargs)
            else:
                raise TypeError("data must be an xarray.DataArray or xarray.Dataset")
        except Exception as e:
            print(f"Error remapping with xESMF: {e}")

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
        self._obj[da.name] = da
        das = {}
        das[da.name] = da
        for i in loop_vars[1:]:
            dataarray = dset[i]
            tmp = self._remap_xesmf_dataarray(
                dataarray, filename=filename, reuse_weights=True, **kwargs
            )
            das[tmp.name] = tmp.copy()
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
        loop_vars = [
            vn
            for vn in self._obj.variables
            if "z" in self._obj[vn].dims
            and vn != vertical.name
            and len(self._obj[vn].shape) >= len(vertical_shape)
            and self._obj[vn].shape[-len(vertical_shape):] == vertical_shape
        ]

        if not loop_vars:
            raise ValueError("No variables found with vertical dimension matching the provided coordinate")

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

                row, col = utils.generate_nearest_neighbour_linesample_arrays(swath, pswath_ll, float(1e6))
                y_ll, x_ll = row[0][0], col[0][0]

                row, col = utils.generate_nearest_neighbour_linesample_arrays(swath, pswath_ur, float(1e6))
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
