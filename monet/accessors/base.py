"""Base accessor implementation for MONET"""

import xarray as xr

try:
    import esmpy  # noqa: F401
    import xregrid  # noqa: F401

    has_xregrid = True
except ImportError:
    try:
        import ESMF as esmpy  # noqa: F401
        import xregrid  # noqa: F401

        has_xregrid = True
    except ImportError:
        has_xregrid = False

try:
    import monet_regrid  # noqa: F401

    has_monet_regrid = True
except ImportError:
    has_monet_regrid = False


def wrap_longitudes(lons):
    """For longitudes that may be in [0, 360) format, return in [-180, 180) format."""
    return (lons + 180) % 360 - 180


class BaseAccessor:
    """Base class for MONET accessors with common utility methods."""

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
            "periodic": False,
            "filename": "monet_xesmf_regrid_file.nc",
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
        # To consider unstructured grid
        if ds.attrs.get("mio_has_unstructured_grid", False):
            check_list = ds.data_vars
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
    def _detect_latlon_names(ds):
        """Detect possible latitude/longitude coordinate names in COARDS/CF datasets.

        Parameters
        ----------
        ds : xarray.DataArray or xarray.Dataset
            The input xarray object to check

        Returns
        -------
        tuple
            (lat_name, lon_name) if found, otherwise (None, None)
        """
        # Common latitude/longitude naming patterns, including non-rectilinear grid names
        lat_names = [
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
        ]
        lon_names = [
            "longitude",
            "lon",
            "Longitude",
            "LONGITUDE",
            "LON",
            "x",
            "Long",
            "Lon",
            "XLONG",
            "XLONG_M",
            "grid_xt",
            "nav_lon",
            "NX",
            "lon_b",
            "lon_centers",
        ]

        # First check in coordinates
        for lat, lon in zip(lat_names, lon_names):
            if lat in ds.coords and lon in ds.coords:
                return lat, lon

        # Then check in variables if it's a Dataset
        if isinstance(ds, xr.Dataset):
            for lat, lon in zip(lat_names, lon_names):
                if lat in ds.variables and lon in ds.variables:
                    return lat, lon

        # Look for variables with standard_name attribute
        lat_name = None
        lon_name = None

        if isinstance(ds, xr.Dataset):
            for var in ds.variables:
                if "standard_name" in ds[var].attrs:
                    if ds[var].attrs["standard_name"] in ["latitude", "grid_latitude"]:
                        lat_name = var
                    elif ds[var].attrs["standard_name"] in [
                        "longitude",
                        "grid_longitude",
                    ]:
                        lon_name = var

            if lat_name is not None and lon_name is not None:
                return lat_name, lon_name

        return None, None

    @staticmethod
    def _dataset_to_monet(
        dset,
        lat_name="latitude",
        lon_name="longitude",
        latlon2d=None,
        lon180=None,
        coards_compliant=False,
    ):
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
        if (isinstance(dset, xr.Dataset) and not {"latitude", "longitude"} <= set(dset.variables)) or (
            isinstance(dset, xr.DataArray) and not {"latitude", "longitude"} <= set(dset.coords)
        ):
            dset = dset.rename({lat_name: "latitude", lon_name: "longitude"})

        # Maybe wrap longitudes
        if lon180 is None:
            try:
                lon180 = dset["longitude"].min() >= -180 and dset["longitude"].max() < 180
            except (ValueError, TypeError):
                # Handle case where longitude might be multidimensional
                if dset["longitude"].ndim > 1:
                    lon_values = dset["longitude"].values
                    lon180 = lon_values.min() >= -180 and lon_values.max() < 180
                else:
                    lon180 = True  # Default to avoiding unnecessary wrapping

        if not lon180:
            dset["longitude"] = wrap_longitudes(dset["longitude"])

        # lat & lon are not coordinate variables in unstructured grid, so we're done
        if dset.attrs.get("mio_has_unstructured_grid", False):
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
        dset["x"] = da.longitude[0, :].values
        dset["y"] = da.latitude[:, 0].values
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
        from numpy import arange, meshgrid

        # Check if lat/lon coordinate names exist, otherwise try to detect them
        if lat_name not in dset.variables or lon_name not in dset.variables:
            detected_lat, detected_lon = BaseAccessor._detect_latlon_names(dset)
            if detected_lat and detected_lon:
                lat_name, lon_name = detected_lat, detected_lon

        # Extract coordinates and handle reversed coordinates if needed
        lon = dset[lon_name]
        lat = dset[lat_name]

        # Check for monotonicity and handle reversed coordinates
        lat_decreasing = lat[0] > lat[-1] if len(lat) > 1 else False
        lon_decreasing = lon[0] > lon[-1] if len(lon) > 1 else False

        if lat_decreasing:
            lat = lat[::-1]
        if lon_decreasing:
            lon = lon[::-1]

        # Create 2D meshgrid
        lons, lats = meshgrid(lon, lat)

        # Create new coordinates
        x = arange(len(lon))
        y = arange(len(lat))

        # Create new dataset with renamed coordinates
        result = dset.rename({lon_name: "x", lat_name: "y"})

        # Add 2D latitude/longitude arrays
        result.coords["longitude"] = (("y", "x"), lons)
        result.coords["latitude"] = (("y", "x"), lats)

        # Add 1D coordinate arrays
        result["x"] = x
        result["y"] = y

        # Set as coordinates
        result = result.set_coords(["latitude", "longitude"])

        # If coordinates were reversed, make sure data is properly oriented
        if lat_decreasing:
            result = result.sel(y=slice(None, None, -1))
        if lon_decreasing:
            result = result.sel(x=slice(None, None, -1))

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
        from numpy import arange, meshgrid

        # Check if lat/lon coordinate names exist, otherwise try to detect them
        if lat_name not in dset.coords or lon_name not in dset.coords:
            detected_lat, detected_lon = BaseAccessor._detect_latlon_names(dset)
            if detected_lat and detected_lon:
                lat_name, lon_name = detected_lat, detected_lon

        # Extract coordinates and handle reversed coordinates if needed
        lon = dset[lon_name]
        lat = dset[lat_name]

        # Check for monotonicity and handle reversed coordinates
        lat_decreasing = lat[0] > lat[-1] if len(lat) > 1 else False
        lon_decreasing = lon[0] > lon[-1] if len(lon) > 1 else False

        if lat_decreasing:
            lat = lat[::-1]
        if lon_decreasing:
            lon = lon[::-1]

        # Create 2D meshgrid
        lons, lats = meshgrid(lon, lat)

        # Create new coordinates
        x = arange(len(lon))
        y = arange(len(lat))

        # Create new dataset with renamed coordinates
        result = dset.rename({lon_name: "x", lat_name: "y"})

        # Add 2D latitude/longitude arrays
        result.coords["latitude"] = (("y", "x"), lats)
        result.coords["longitude"] = (("y", "x"), lons)

        # Add 1D coordinate arrays
        result["x"] = x
        result["y"] = y

        # If coordinates were reversed, make sure data is properly oriented
        if lat_decreasing:
            result = result.sel(y=slice(None, None, -1))
        if lon_decreasing:
            result = result.sel(x=slice(None, None, -1))

        return result

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
            Whether to make the output COARDS/CF compliant.

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
