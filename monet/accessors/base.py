"""Base accessor implementation for MONET"""

import typing as t

import xarray as xr

from ..util.resample import has_monet_regrid, has_xregrid


def wrap_longitudes(lons):
    """For longitudes that may be in [0, 360) format, return in [-180, 180) format."""
    return (lons + 180) % 360 - 180


LAT_NAMES = [
    "latitude",
    "lat",
    "Latitude",
    "LATITUDE",
    "LAT",
    "y",
    "Lat",
    "XLAT",
    "XLAT_M",
    "grid_yt",
    "nav_lat",
    "NY",
    "lat_b",
    "lat_centers",
    "node_lat",
    "face_lat",
    "edge_lat",
]
LON_NAMES = [
    "longitude",
    "lon",
    "Longitude",
    "LONGITUDE",
    "LON",
    "x",
    "Long",
    "XLONG",
    "XLONG_M",
    "grid_xt",
    "nav_lon",
    "NX",
    "lon_b",
    "lon_centers",
    "node_lon",
    "face_lon",
    "edge_lon",
]


class BaseAccessor:
    """Base class for MONET accessors with common utility methods."""

    @property
    def lat(self) -> t.Any:
        """Detected latitude coordinate or variable.
        Uses convention-aware detection without renaming.
        Supports Xarray and Pandas.
        """
        if hasattr(self._obj, "coords") or hasattr(self._obj, "variables"):  # Xarray
            from ..util.conventions import find_coords

            return find_coords(self._obj, "latitude")
        elif hasattr(self._obj, "columns"):  # Pandas
            lat_names = ["latitude", "lat", "Latitude", "Lat", "LAT"]
            lat_col = next((c for c in lat_names if c in self._obj.columns), None)
            if lat_col:
                return self._obj[lat_col]
        return None

    @property
    def lon(self) -> t.Any:
        """Detected longitude coordinate or variable.
        Uses convention-aware detection without renaming.
        Supports Xarray and Pandas.
        """
        if hasattr(self._obj, "coords") or hasattr(self._obj, "variables"):  # Xarray
            from ..util.conventions import find_coords

            return find_coords(self._obj, "longitude")
        elif hasattr(self._obj, "columns"):  # Pandas
            lon_names = ["longitude", "lon", "Longitude", "Lon", "LON"]
            lon_col = next((c for c in lon_names if c in self._obj.columns), None)
            if lon_col:
                return self._obj[lon_col]
        return None

    @staticmethod
    def safe_import(module_name, error_msg=None):
        """Import a module with a clear error message if not found."""
        import importlib

        try:
            return importlib.import_module(module_name)
        except ImportError:
            if error_msg is None:
                error_msg = f"Module '{module_name}' is required but not installed."
            raise ImportError(error_msg)

    @staticmethod
    def _check_kwargs_and_set_defaults(**kwargs):
        """Set default values for regrid/remap kwargs if not provided.

        Parameters
        ----------
        kwargs : dict
            Regrid/remap kwargs dict.

        Returns
        -------
        dict
            With defaults added if not already set.
        """
        defaults = {
            "reuse_weights": False,
            "method": "bilinear",
        }
        return {**defaults, **kwargs}

    @staticmethod
    def _rename_latlon(ds):
        """Rename latitude/longitude variants to ``lat``/``lon``.

        Parameters
        ----------
        ds : xarray.DataArray or xarray.Dataset
            Dataset with latitude/longitude coordinates to rename.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Dataset with renamed coordinates.
        """
        if "latitude" in ds.coords:
            return ds.rename({"latitude": "lat", "longitude": "lon"})
        elif "Latitude" in ds.coords:
            return ds.rename({"Latitude": "lat", "Longitude": "lon"})
        elif "Lat" in ds.coords:
            return ds.rename({"Lat": "lat", "Lon": "lon"})
        else:
            return ds

    @staticmethod
    def _rename_to_monet_latlon(ds):
        """Rename latitude/longitude variants to ``latitude``/``longitude``.

        Parameters
        ----------
        ds : xarray.DataArray or xarray.Dataset
            Dataset with latitude/longitude coordinates to rename.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Dataset with renamed coordinates.
        """
        from ..util.conventions import detect_grid_type

        # To consider unstructured grid
        if detect_grid_type(ds) == "unstructured":
            check_list = ds.variables
        else:
            check_list = ds.coords

        if "lat" in check_list:
            return ds.rename({"lat": "latitude", "lon": "longitude"})
        elif "Latitude" in check_list:
            return ds.rename({"Latitude": "latitude", "Longitude": "longitude"})
        elif "Lat" in check_list:
            return ds.rename({"Lat": "latitude", "Lon": "longitude"})
        elif "XLAT_M" in check_list:
            return ds.rename({"XLAT_M": "latitude", "XLONG_M": "longitude"})
        elif "XLAT" in check_list:
            return ds.rename({"XLAT": "latitude", "XLONG": "longitude"})
        else:
            return ds.copy()

    @staticmethod
    def _detect_ugrid(ds):
        """Detect UGRID mesh topology in a dataset.

        Parameters
        ----------
        ds : xarray.Dataset or xarray.DataArray
            The input xarray object to check.

        Returns
        -------
        str or None
            The name of the mesh topology variable if found, otherwise None.
        """
        from ..util.conventions import get_ugrid_info

        info = get_ugrid_info(ds)
        return info.get("mesh_name")

    @staticmethod
    def _detect_latlon_names(ds):
        """Detect possible latitude/longitude coordinate names in COARDS/CF/UGRID datasets.

        Parameters
        ----------
        ds : xarray.DataArray or xarray.Dataset
            The input xarray object to check

        Returns
        -------
        tuple
            (lat_name, lon_name) if found, otherwise (None, None)
        """
        from ..util.conventions import find_coords

        lat_da = find_coords(ds, "latitude")
        lon_da = find_coords(ds, "longitude")

        lat_name = getattr(lat_da, "name", None) if lat_da is not None else None
        lon_name = getattr(lon_da, "name", None) if lon_da is not None else None

        return lat_name, lon_name

    @staticmethod
    def _dataset_to_monet(
        dset: xr.DataArray | xr.Dataset,
        lat_name: str = "latitude",
        lon_name: str = "longitude",
        latlon2d: bool | None = None,
        lon180: bool | None = None,
        coards_compliant: bool = False,
    ) -> xr.DataArray | xr.Dataset:
        """Rename xarray DataArray or Dataset coordinate variables for use with monet functions.

        Parameters
        ----------
        dset : xarray.DataArray or xarray.Dataset
            A given data obj to be renamed for monet.
        lat_name : str, default: "latitude"
            Name of the latitude array.
        lon_name : str, default: "longitude"
            Name of the longitude array.
        latlon2d : bool, optional
            Whether the latitude and longitude data is two-dimensional.
            If unset (``None``), guess based on dim count.
        lon180 : bool, optional
            Whether the longitude values are in the range [-180, 180) already.
            If true, longitude wrapping/normalization,
            which can introduce small floating point errors, will be skipped.
            If unset (``None``), compute min/max to determine.
        coards_compliant : bool, default: False
            Whether to make the output COARDS/CF compliant.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            The input with coordinates renamed for MONET compatibility.

        Raises
        ------
        TypeError
            If dset is not an xarray DataArray or Dataset.
        """
        if not isinstance(dset, xr.DataArray | xr.Dataset):
            raise TypeError("dset must be an xarray.DataArray or xarray.Dataset")

        # Auto-detect UGRID
        mesh_var = BaseAccessor._detect_ugrid(dset)
        if mesh_var:
            dset.attrs["mio_has_ugrid"] = True
            dset.attrs["ugrid_mesh"] = mesh_var

        # Auto-detect lat/lon names for COARDS/CF formatted data
        detected_lat, detected_lon = BaseAccessor._detect_latlon_names(dset)
        if detected_lat and detected_lon:
            lat_name, lon_name = detected_lat, detected_lon

        # Handle grid_xt dimension in UFS files
        if "grid_xt" in dset.dims:  # UFS
            if isinstance(dset, xr.DataArray):
                dset = BaseAccessor._dataarray_coards_to_netcdf(dset, lat_name="grid_yt", lon_name="grid_xt")
            elif isinstance(dset, xr.Dataset):
                dset = BaseAccessor._coards_to_netcdf(dset, lat_name="grid_yt", lon_name="grid_xt")

        # Handle WRF dimensions
        if "south_north" in dset.dims:  # WRF WPS file
            dset = dset.rename(dict(south_north="y", west_east="x"))
            if isinstance(dset, xr.Dataset):
                if "XLAT_M" in dset.data_vars:
                    dset["XLAT_M"] = dset.XLAT_M.squeeze()
                    dset["XLONG_M"] = dset.XLONG_M.squeeze()
                    dset = dset.set_coords(["XLAT_M", "XLONG_M"])
                elif "XLAT" in dset.data_vars:
                    dset["XLAT"] = dset.XLAT.squeeze()
                    dset["XLONG"] = dset.XLONG.squeeze()
                    dset = dset.set_coords(["XLAT", "XLONG"])
            elif isinstance(dset, xr.DataArray):
                if "XLAT_M" in dset.coords:
                    dset["XLAT_M"] = dset.XLAT_M.squeeze()
                    dset["XLONG_M"] = dset.XLONG_M.squeeze()
                elif "XLAT" in dset.coords:
                    dset["XLAT"] = dset.XLAT.squeeze()
                    dset["XLONG"] = dset.XLONG.squeeze()

        # Check for Climate and Forecast (CF) convention attributes
        if isinstance(dset, xr.Dataset):
            for var in dset.variables:
                if "standard_name" in dset[var].attrs:
                    if dset[var].attrs["standard_name"] in [
                        "latitude",
                        "grid_latitude",
                    ]:
                        lat_name = var
                    elif dset[var].attrs["standard_name"] in [
                        "longitude",
                        "grid_longitude",
                    ]:
                        lon_name = var

        # Rename lat/lon coordinates to 'latitude'/'longitude'
        dset = BaseAccessor._rename_to_monet_latlon(dset)  # common cases

        # Determine if we still need to rename based on detection or defaults
        needs_lat_rename = "latitude" not in (dset.variables if isinstance(dset, xr.Dataset) else dset.coords)
        needs_lon_rename = "longitude" not in (dset.variables if isinstance(dset, xr.Dataset) else dset.coords)

        rename_dict = {}
        if needs_lat_rename and lat_name in (dset.variables if isinstance(dset, xr.Dataset) else dset.coords):
            rename_dict[lat_name] = "latitude"
        if needs_lon_rename and lon_name in (dset.variables if isinstance(dset, xr.Dataset) else dset.coords):
            rename_dict[lon_name] = "longitude"

        if rename_dict:
            dset = dset.rename(rename_dict)

        # Maybe wrap longitudes
        if lon180 is None:
            # Idempotent wrapping is safer than forcing computation to check range
            lon180 = False

        # Explicitly check coordinates or variables without triggering data loading
        check_obj = dset.variables if isinstance(dset, xr.Dataset) else dset.coords
        if not lon180 and "longitude" in check_obj:
            dset["longitude"] = wrap_longitudes(dset["longitude"])

        # lat & lon are not coordinate variables in unstructured grid, so we're done
        from ..util.conventions import detect_grid_type

        if detect_grid_type(dset) == "unstructured":
            return dset

        # Maybe convert 1-D lat/lon coords to 2-D
        if latlon2d is None:
            latlon2d = dset["latitude"].ndim >= 2 if "latitude" in dset.coords else False

        if not latlon2d:
            try:
                if isinstance(dset, xr.DataArray):
                    dset = BaseAccessor._dataarray_coards_to_netcdf(dset, lat_name="latitude", lon_name="longitude")
                elif isinstance(dset, xr.Dataset):
                    dset = BaseAccessor._coards_to_netcdf(dset, lat_name="latitude", lon_name="longitude")
            except Exception as e:
                # If conversion fails, log error and return original dataset
                print(f"Error converting COARDS format: {e}")
                return dset

        # Make COARDS compliant if requested
        if coards_compliant:
            from ..util.coards_tools import add_cf_standard_names, monet_to_coards

            dset = monet_to_coards(dset)
            dset = add_cf_standard_names(dset)

        return dset

    @staticmethod
    def _monet_to_latlon(da):
        """Convert from MONET-style coordinates to standard lat/lon coordinates.

        Parameters
        ----------
        da : xarray.DataArray
            DataArray with MONET-style coordinates.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            DataArray or Dataset with standard lat/lon coordinates.
        """
        if isinstance(da, xr.DataArray):
            dset = da.to_dataset()
        else:
            dset = da.copy()
        dset["x"] = da.longitude[0, :]
        dset["y"] = da.latitude[:, 0]
        dset = dset.drop_vars(["latitude", "longitude"])
        dset = dset.set_coords(["x", "y"])
        dset = dset.rename({"x": "lon", "y": "lat"})
        if isinstance(da, xr.DataArray):
            return dset[da.name]
        else:
            return dset

    @staticmethod
    def _coards_to_netcdf(dset, lat_name="lat", lon_name="lon"):
        """Convert 1-D lat/lon coords to x/y convention, with
        lat/lon as 2-D variables with (y, x) dimensions.

        Handles both COARDS and CF convention datasets.

        Parameters
        ----------
        dset : xarray.Dataset
            Dataset with 1D lat/lon coordinates.
        lat_name : str, default: "lat"
            Name of the latitude coordinate.
        lon_name : str, default: "lon"
            Name of the longitude coordinate.

        Returns
        -------
        xarray.Dataset
            Dataset with 2D lat/lon coordinates.
        """
        import numpy as np

        from ..util.conventions import find_coords, update_history

        # Check if lat/lon coordinate names exist, otherwise try to detect them
        if lat_name not in dset.variables or lon_name not in dset.variables:
            lat_da = find_coords(dset, "latitude")
            lon_da = find_coords(dset, "longitude")
            if lat_da is not None and lon_da is not None:
                lat_name, lon_name = lat_da.name, lon_da.name

        # Extract coordinates and handle reversed coordinates if needed
        lon = dset[lon_name]
        lat = dset[lat_name]

        lat_dim = lat.dims[0]
        lon_dim = lon.dims[0]

        # Check for monotonicity and handle reversed coordinates
        # Note: Indexing for metadata check is acceptable.
        lat_decreasing = bool(lat[0] > lat[-1]) if lat.size > 1 else False
        lon_decreasing = bool(lon[0] > lon[-1]) if lon.size > 1 else False

        if lat_decreasing:
            lat = lat[::-1]
        if lon_decreasing:
            lon = lon[::-1]

        # Create 2D meshgrid lazily via broadcast
        lons, lats = xr.broadcast(lon, lat)

        # Standardize dimension order to (lat_dim, lon_dim) -> (y, x)
        lons = lons.transpose(lat_dim, lon_dim)
        lats = lats.transpose(lat_dim, lon_dim)

        # Create new coordinates
        x = np.arange(len(lon))
        y = np.arange(len(lat))

        # Create new dataset with renamed dimensions
        result = dset.rename({lon_dim: "x", lat_dim: "y"})

        # Add 2D latitude/longitude arrays and 1D coords
        result.coords["longitude"] = lons.rename({lon_dim: "x", lat_dim: "y"})
        result.coords["latitude"] = lats.rename({lon_dim: "x", lat_dim: "y"})
        result["x"] = x
        result["y"] = y

        # Set as coordinates
        result = result.set_coords(["latitude", "longitude"])

        # If coordinates were reversed, make sure data is properly oriented
        if lat_decreasing:
            result = result.sel(y=slice(None, None, -1))
        if lon_decreasing:
            result = result.sel(x=slice(None, None, -1))

        update_history(result, f"Converted 1D {lat_name}/{lon_name} to 2D latitude/longitude (Lazy).")

        return result

    @staticmethod
    def _dataarray_coards_to_netcdf(dset, lat_name="lat", lon_name="lon"):
        """Convert 1-D lat/lon coords to x/y convention, with
        lat/lon as 2-D variables with (y, x) dimensions for a DataArray.

        Handles both COARDS and CF convention datasets.

        Parameters
        ----------
        dset : xarray.DataArray
            DataArray with 1D lat/lon coordinates.
        lat_name : str, default: "lat"
            Name of the latitude coordinate.
        lon_name : str, default: "lon"
            Name of the longitude coordinate.

        Returns
        -------
        xarray.DataArray
            DataArray with 2D lat/lon coordinates.
        """
        import numpy as np

        from ..util.conventions import find_coords, update_history

        # Check if lat/lon coordinate names exist, otherwise try to detect them
        if lat_name not in dset.coords or lon_name not in dset.coords:
            lat_da = find_coords(dset, "latitude")
            lon_da = find_coords(dset, "longitude")
            if lat_da is not None and lon_da is not None:
                lat_name, lon_name = lat_da.name, lon_da.name

        # Extract coordinates and handle reversed coordinates if needed
        lon = dset[lon_name]
        lat = dset[lat_name]

        lat_dim = lat.dims[0]
        lon_dim = lon.dims[0]

        # Check for monotonicity and handle reversed coordinates
        lat_decreasing = bool(lat[0] > lat[-1]) if lat.size > 1 else False
        lon_decreasing = bool(lon[0] > lon[-1]) if lon.size > 1 else False

        if lat_decreasing:
            lat = lat[::-1]
        if lon_decreasing:
            lon = lon[::-1]

        # Create 2D meshgrid lazily via broadcast
        lons, lats = xr.broadcast(lon, lat)

        # Standardize dimension order to (lat_dim, lon_dim) -> (y, x)
        lons = lons.transpose(lat_dim, lon_dim)
        lats = lats.transpose(lat_dim, lon_dim)

        # Create new coordinates
        x = np.arange(len(lon))
        y = np.arange(len(lat))

        # Create new dataset with renamed dimensions
        result = dset.rename({lon_dim: "x", lat_dim: "y"})

        # Add 2D latitude/longitude arrays and 1D coords
        result.coords["latitude"] = lats.rename({lon_dim: "x", lat_dim: "y"})
        result.coords["longitude"] = lons.rename({lon_dim: "x", lat_dim: "y"})
        result["x"] = x
        result["y"] = y

        # If coordinates were reversed, make sure data is properly oriented
        if lat_decreasing:
            result = result.sel(y=slice(None, None, -1))
        if lon_decreasing:
            result = result.sel(x=slice(None, None, -1))

        update_history(result, f"Converted 1D {lat_name}/{lon_name} to 2D latitude/longitude (Lazy).")

        return result

    def structure_for_monet(
        self,
        lat_name: str = "lat",
        lon_name: str = "lon",
        return_obj: bool = True,
        coards_compliant: bool = False,
    ) -> xr.DataArray | xr.Dataset | None:
        """Structure the object for use with MONET functions.
        Deprecated in favor of convention-aware processing, but preserved for
        explicit dimension renaming to 'x'/'y'.

        Parameters
        ----------
        lat_name : str, default: "lat"
            Name of the latitude coordinate.
        lon_name : str, default: "lon"
            Name of the longitude coordinate.
        return_obj : bool, default: True
            Whether to return the restructured object.
        coards_compliant : bool, default: False
            Whether to make the output COARDS/CF compliant.

        Returns
        -------
        xarray.DataArray, xarray.Dataset, or None
            Restructured object if return_obj is True, otherwise None.
        """
        import warnings

        warnings.warn(
            "structure_for_monet is deprecated. Most MONET functions are now convention-aware "
            "and do not require dimension renaming.",
            DeprecationWarning,
            stacklevel=2,
        )

        res = self._dataset_to_monet(
            self._obj,
            lat_name=lat_name,
            lon_name=lon_name,
            coards_compliant=coards_compliant,
        )

        if return_obj:
            return res
        else:
            self._obj = res
            return None

    def standardize(self) -> xr.DataArray | xr.Dataset:
        """Standardize the object coordinates and attributes without renaming dimensions.
        Convention-aware: adds standard_name attributes and ensures longitudes are wrapped.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            The standardized object.
        """
        from ..util.conventions import update_history

        obj = self._obj.copy()

        # Wrap longitudes if present
        lat_name, lon_name = self._detect_latlon_names(obj)
        if lon_name:
            obj[lon_name] = (obj[lon_name] + 180) % 360 - 180
            if "standard_name" not in obj[lon_name].attrs:
                obj[lon_name].attrs["standard_name"] = "longitude"
            if "units" not in obj[lon_name].attrs:
                obj[lon_name].attrs["units"] = "degrees_east"

        if lat_name:
            if "standard_name" not in obj[lat_name].attrs:
                obj[lat_name].attrs["standard_name"] = "latitude"
            if "units" not in obj[lat_name].attrs:
                obj[lat_name].attrs["units"] = "degrees_north"

        update_history(obj, "Standardized coordinates and attributes.")

        return obj

    def is_land(self, return_xarray: bool = False) -> xr.DataArray | xr.Dataset | t.Any:
        """Check if points are on land.
        Supports both Eager (NumPy) and Lazy (Dask) backends via ``xarray.apply_ufunc``.
        Convention-aware: works with CF/COARDS and UGRID without forced renaming.
        Also supports Pandas DataFrames.

        Parameters
        ----------
        return_xarray : bool, default: False
            If True, return results as masked object.
            Otherwise, return the boolean mask.

        Returns
        -------
        xarray.DataArray, xarray.Dataset, or any
            If return_xarray is True, returns the object masked by land.
            Otherwise, returns a boolean mask.
        """
        return self._mask_land_ocean(mask_type="land", return_xarray=return_xarray)

    def is_ocean(self, return_xarray: bool = False) -> xr.DataArray | xr.Dataset | t.Any:
        """Check if points are on ocean.
        Supports both Eager (NumPy) and Lazy (Dask) backends via ``xarray.apply_ufunc``.
        Convention-aware: works with CF/COARDS and UGRID without forced renaming.
        Also supports Pandas DataFrames.

        Parameters
        ----------
        return_xarray : bool, default: False
            If True, return results as masked object.
            Otherwise, return the boolean mask.

        Returns
        -------
        xarray.DataArray, xarray.Dataset, or any
            If return_xarray is True, returns the object masked by ocean.
            Otherwise, returns a boolean mask.
        """
        return self._mask_land_ocean(mask_type="ocean", return_xarray=return_xarray)

    def _mask_land_ocean(self, mask_type: str = "land", return_xarray: bool = False) -> xr.DataArray | xr.Dataset | t.Any:
        """Helper method to compute land/ocean mask.

        Parameters
        ----------
        mask_type : str, default: 'land'
            Type of mask to compute: 'land' or 'ocean'.
        return_xarray : bool, default: False
            If True, return results as xarray (masked object).
            Otherwise, return the boolean mask as a DataArray.

        Returns
        -------
        xarray.DataArray, xarray.Dataset, or t.Any
            If return_xarray is True, returns the object masked.
            Otherwise, returns the boolean mask.
        """
        from ..util.mask import get_mask

        mask = get_mask("land")

        lat = self.lat
        lon = self.lon
        if lat is None or lon is None:
            raise ValueError("Could not detect latitude and longitude coordinates.")

        if hasattr(self._obj, "coords") or hasattr(self._obj, "variables"):  # Xarray
            # Use apply_ufunc to be backend-agnostic (handles Dask automatically if parallelized=True)
            # We align inputs and use a template to preserve coordinates and dimensions
            res = xr.apply_ufunc(
                mask.query,
                lat,
                lon,
                dask="parallelized",
                output_dtypes=[object],
                keep_attrs=True,
            )

            # Ensure coordinates are preserved (xr.apply_ufunc with multiple inputs and different dims
            # might not automatically combine them into the result as we expect here)
            if mask_type == "land":
                is_type = res == "land"
            else:
                is_type = res != "land"

            is_type.name = f"is_{mask_type}"

            # Re-assign coordinates from the original object to ensure they are preserved
            # especially for the lazy case where they might have been dropped
            for coord in self._obj.coords:
                if set(self._obj.coords[coord].dims).issubset(is_type.dims):
                    is_type = is_type.assign_coords({coord: self._obj.coords[coord]})

            if return_xarray:
                is_type = self._obj.where(is_type)

            # Update history for provenance
            from ..util.conventions import update_history

            update_history(is_type, f"Computed {mask_type} mask via monet.is_{mask_type}")

            return is_type
        else:
            # Assume Pandas
            import numpy as np
            import pandas as pd

            res = mask.query(np.asarray(lat), np.asarray(lon))
            if mask_type == "land":
                is_type = res == "land"
            else:
                is_type = res != "land"

            if return_xarray:
                # For pandas, where() with a Series will align on index if axis=0
                mask_series = pd.Series(is_type, index=self._obj.index)
                return self._obj.where(mask_series, axis=0)
            return is_type

    def get_region(self, mask_name: str, resolution: float = 0.05, new_var: str | None = None) -> xr.DataArray | xr.Dataset | t.Any:
        """Add region information to the object using a pre-computed mask.

        Parameters
        ----------
        mask_name : str
            Name of the mask (e.g., 'giorgi', 'ipcc_ar6', 'epa_eco', 'timezones', 'epa_admin').
        resolution : float, default: 0.05
            Resolution of the mask in degrees.
        new_var : str, optional
            Name of the new variable/column to create. Defaults to mask_name.

        Returns
        -------
        xarray.DataArray, xarray.Dataset, or pandas.DataFrame
            The object with the added region information.
        """
        from ..util.mask import query_mask

        return query_mask(self._obj, mask_name, resolution=resolution, new_var=new_var)

    def wrap_longitudes(self, lon_name: str | None = None) -> xr.DataArray | xr.Dataset:
        """Wrap longitude values to [-180, 180).
        Convention-aware: auto-detects longitude if lon_name is None.

        Parameters
        ----------
        lon_name : str, optional
            Name of the longitude coordinate. If None, auto-detects.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Object with wrapped longitudes.
        """
        if lon_name is None:
            _, lon_name = self._detect_latlon_names(self._obj)
            if lon_name is None:
                raise ValueError("Could not detect longitude coordinate.")

        obj = self._obj.copy()
        obj[lon_name] = (obj[lon_name] + 180) % 360 - 180

        # Update history for provenance
        from ..util.conventions import update_history

        update_history(obj, f"Wrapped longitudes ({lon_name}) via monet.wrap_longitudes")

        return obj

    def tidy(self, lon_name: str | None = None) -> xr.DataArray | xr.Dataset:
        """Apply tidying operations to the data.
        Wraps longitudes and sorts by longitude.
        Convention-aware: auto-detects longitude if lon_name is None.

        Parameters
        ----------
        lon_name : str, optional
            Name of the longitude coordinate. If None, auto-detects.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Tidied object.
        """
        if lon_name is None:
            _, lon_name = self._detect_latlon_names(self._obj)
            if lon_name is None:
                raise ValueError("Could not detect longitude coordinate.")

        wd = self.wrap_longitudes(lon_name=lon_name)
        wdl = wd.sortby(wd[lon_name])

        # Update history for provenance
        from ..util.conventions import update_history

        update_history(wdl, f"Tidied via monet.tidy (lon_name={lon_name})")

        return wdl

    def cftime_to_datetime64(self, name: str | None = None) -> xr.DataArray | xr.Dataset:
        """Convert cftime coordinates to numpy datetime64.
        Preserves Dask laziness if the time coordinate is Dask-backed.

        Parameters
        ----------
        name : str, optional
            Name of the coordinate to convert. If None, tries to detect the time coordinate.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            Object with converted time coordinate.
        """
        import pandas as pd
        from numpy import vectorize

        obj = self._obj.copy()

        def cf_to_dt64(x):
            try:
                return pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S"))
            except AttributeError:
                return x

        if name is None:  # assume 'time' is the column name to transform
            name = "time"

        if hasattr(obj, "coords") and name in obj.coords and isinstance(obj[name].to_index(), xr.CFTimeIndex):
            obj[name] = xr.apply_ufunc(
                vectorize(cf_to_dt64),
                obj[name],
                dask="parallelized",
                output_dtypes=["datetime64[ns]"],
            )

            # Update history
            from ..util.conventions import update_history

            update_history(obj, f"Converted {name} from cftime to datetime64")

        return obj

    def interp_constant_lat(self, lat: float | None = None, **kwargs: t.Any) -> xr.DataArray | xr.Dataset:
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
        xarray.DataArray or xarray.Dataset
            Interpolated object.
        """
        from numpy import asarray, linspace, ones

        if lat is None:
            raise ValueError("Must provide a latitude value ('lat')")

        obj = self._obj.copy()
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

        out = resample(obj, target, **kwargs)

        # Update history
        from ..util.conventions import update_history

        update_history(out, f"Interpolated to constant latitude: {lat}")

        return out

    def interp_constant_lon(self, lon: float | None = None, **kwargs: t.Any) -> xr.DataArray | xr.Dataset:
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
        xarray.DataArray or xarray.Dataset
            Interpolated object.
        """
        from numpy import asarray, linspace, ones

        if lon is None:
            raise ValueError("Must provide a longitude value ('lon')")

        obj = self._obj.copy()
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

        out = resample(obj, target, **kwargs)

        # Update history
        from ..util.conventions import update_history

        update_history(out, f"Interpolated to constant longitude: {lon}")

        return out

    def nearest_latlon(
        self,
        lat: float | t.Sequence[float] | None = None,
        lon: float | t.Sequence[float] | None = None,
        cleanup: bool = True,
        esmf: bool = False,
        **kwargs: t.Any,
    ) -> xr.DataArray | xr.Dataset:
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
        xarray.DataArray or xarray.Dataset
            Object at nearest point(s).
        """
        if lat is None or lon is None:
            raise ValueError("Must provide latitude and longitude")

        obj = self._obj.copy()

        # Use xregrid via resample
        from ..util.interp_util import points_to_dataset
        from ..util.resample import resample

        # Create target grid
        target = points_to_dataset(latitude=lat, longitude=lon)
        output = resample(obj, target, method="nearest", **kwargs)

        res = output.squeeze()

        # Update history
        from ..util.conventions import update_history

        update_history(res, "Extracted nearest lat/lon points")

        return res

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
        """
        if not has_xregrid and not has_monet_regrid:
            raise ImportError("xregrid (with esmpy) or monet-regrid is required for this functionality")

        from ..util.resample import resample

        # Ensure consistent behavior: always remap the argument 'data' to 'self._obj' grid.
        # This resolves issues where dask-backed targets caused an erroneous swap of source and target.
        source = data
        target = self._obj

        out = resample(source, target, method=method, **kwargs)

        # Update history
        from ..util.conventions import update_history

        update_history(out, f"Remapped via monet.remap (method={method})")

        return out

    def pair(self, obs: t.Any, **kwargs: t.Any) -> t.Any:
        """Pair this object with observation data.

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

    def combine_point(self, data: t.Any, suffix: str | None = None, **kwargs: t.Any) -> t.Any:
        """Combine point data with this object.

        Note: This is a backward compatibility wrapper for `pair`.

        Parameters
        ----------
        data : pandas.DataFrame or t.Any
            Point data to combine.
        suffix : str, optional
            Suffix to add to variable names.
        **kwargs : dict
            Additional keyword arguments for regridding.

        Returns
        -------
        combined data
            Combined data.
        """
        return self.pair(data, suffix=suffix, **kwargs)
