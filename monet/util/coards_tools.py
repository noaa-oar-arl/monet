"""Utilities for working with COARDS and CF convention data in MONET."""

import typing as t

import numpy as np
import pandas as pd
import xarray as xr


def is_ugrid_compliant(ds: xr.Dataset | xr.DataArray) -> bool:
    """Check if a dataset appears to follow UGRID conventions.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        Dataset to check for UGRID compliance.

    Returns
    -------
    bool
        True if the dataset appears to follow UGRID conventions.
    """
    from .conventions import detect_grid_type

    return detect_grid_type(ds) == "unstructured"


def is_coards_compliant(ds: xr.Dataset | xr.DataArray) -> bool:
    """Check if a dataset appears to follow COARDS or CF conventions.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        Dataset to check for COARDS/CF compliance.

    Returns
    -------
    bool
        True if the dataset appears to follow COARDS or CF conventions.
    """
    from .conventions import find_coords

    lat = find_coords(ds, "latitude")
    lon = find_coords(ds, "longitude")
    return lat is not None and lon is not None


def extract_latlon_dataarray(ds: xr.DataArray, return_names: bool = False) -> t.Any:
    """Extract latitude/longitude arrays from a COARDS or CF-compliant DataArray.

    Parameters
    ----------
    ds : xarray.DataArray
        DataArray to extract latitude/longitude arrays from.
    return_names : bool, default: False
        If True, also return the names of the coordinates.

    Returns
    -------
    tuple
        If return_names is False: (latitude_array, longitude_array)
        If return_names is True: (latitude_array, longitude_array, lat_name, lon_name)
    """
    from .conventions import find_coords

    lat_coord = find_coords(ds, "latitude")
    lon_coord = find_coords(ds, "longitude")

    if lat_coord is None or lon_coord is None:
        raise ValueError("Could not find latitude/longitude coordinates in DataArray")

    if return_names:
        return lat_coord, lon_coord, lat_coord.name, lon_coord.name
    else:
        return lat_coord, lon_coord


def extract_latlon_dataset(ds: xr.Dataset, return_names: bool = False) -> t.Any:
    """Extract latitude/longitude arrays from a COARDS or CF-compliant Dataset.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to extract latitude/longitude arrays from.
    return_names : bool, default: False
        If True, also return the names of the variables.

    Returns
    -------
    tuple
        If return_names is False: (latitude_array, longitude_array)
        If return_names is True: (latitude_array, longitude_array, lat_name, lon_name)
    """
    from .conventions import find_coords

    lat_var = find_coords(ds, "latitude")
    lon_var = find_coords(ds, "longitude")

    if lat_var is None or lon_var is None:
        raise ValueError("Could not find latitude/longitude variables in Dataset")

    if return_names:
        return lat_var, lon_var, lat_var.name, lon_var.name
    else:
        return lat_var, lon_var


def is_curvilinear_grid(ds: xr.Dataset | xr.DataArray) -> bool:
    """Detect if a dataset uses a curvilinear (non-rectilinear) grid.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        Dataset to check for curvilinear grid.

    Returns
    -------
    bool
        True if the dataset appears to use a curvilinear grid.
    """
    # Check for explicit grid_mapping attribute
    if isinstance(ds, xr.Dataset):
        for var in ds.variables:
            if "grid_mapping" in ds[var].attrs:
                return True

    # Check if lat/lon coordinates are 2D and have different shapes from the dimensions
    lat_var, lon_var = None, None
    try:
        if isinstance(ds, xr.Dataset):
            result = extract_latlon_dataset(ds, return_names=True)
            if len(result) == 4:
                lat_var, lon_var, _, _ = result
            else:
                lat_var, lon_var = result
        else:
            result = extract_latlon_dataarray(ds, return_names=True)
            if len(result) == 4:
                lat_var, lon_var, _, _ = result
            else:
                lat_var, lon_var = result
    except ValueError:
        return False

    # If lat/lon are 2D, and their values are not strictly monotonic along rows and columns, it's curvilinear
    if lat_var is not None and lon_var is not None and lat_var.ndim == 2 and lon_var.ndim == 2:
        # Use lazy check for rectilinear vs curvilinear
        # A grid is rectilinear if lat only varies with y and lon only varies with x.
        # We check this by comparing the array with its first row/column.
        lat_dim_x = lat_var.dims[1]
        lon_dim_y = lon_var.dims[0]

        # lat(y, x) == lat(y, 0) for all x
        # Note: Indexing for metadata check is acceptable.
        lat_diff = abs(lat_var - lat_var.isel({lat_dim_x: 0})).max()
        lon_diff = abs(lon_var - lon_var.isel({lon_dim_y: 0})).max()

        # Explicit compute for boolean check is intentional for metadata logic
        is_rect = bool(lat_diff < 1e-5) and bool(lon_diff < 1e-5)

        if not is_rect:
            return True

    # Check for dimensions like 'nx', 'ny' that are common in curvilinear grids
    if ("nx" in ds.dims and "ny" in ds.dims) or ("NX" in ds.dims and "NY" in ds.dims):
        return True

    return False


def convert_coards_to_monet_format(ds):
    """Convert a COARDS/CF-compliant Dataset or DataArray to MONET format.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        A dataset or data array to convert

    Returns
    -------
    xarray.Dataset or xarray.DataArray
        The converted dataset or data array
    """
    from ..accessors.base import BaseAccessor

    if isinstance(ds, xr.DataArray):
        result = extract_latlon_dataarray(ds, return_names=True)
        if len(result) == 4:
            lat, lon, lat_name, lon_name = result
        else:
            lat, lon = result
            lat_name = getattr(lat, "name", None) or "latitude"
            lon_name = getattr(lon, "name", None) or "longitude"
        if lat_name is None:
            lat_name = "latitude"
        if lon_name is None:
            lon_name = "longitude"
        return BaseAccessor._dataset_to_monet(ds, lat_name=lat_name, lon_name=lon_name)
    else:
        result = extract_latlon_dataset(ds, return_names=True)
        if len(result) == 4:
            lat, lon, lat_name, lon_name = result
        else:
            lat, lon = result
            lat_name = getattr(lat, "name", None) or "latitude"
            lon_name = getattr(lon, "name", None) or "longitude"
        if lat_name is None:
            lat_name = "latitude"
        if lon_name is None:
            lon_name = "longitude"
        return BaseAccessor._dataset_to_monet(ds, lat_name=lat_name, lon_name=lon_name)


def add_cf_attributes(ds, **kwargs):
    """Add CF convention attributes to a Dataset or DataArray.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        The dataset or data array to add attributes to
    **kwargs : dict
        Additional global attributes to add

    Returns
    -------
    xarray.Dataset or xarray.DataArray
        The dataset or data array with added attributes
    """
    from .conventions import update_history

    result = ds.copy()

    # Add CF Convention global attributes
    result.attrs["Conventions"] = "CF-1.8"
    update_history(result, "Created by MONET")

    # Add additional attributes from kwargs
    for key, value in kwargs.items():
        result.attrs[key] = value

    # Add standard_name attributes to lat/lon coordinates if they don't have them
    if isinstance(result, xr.Dataset):
        if "latitude" in result.coords and "standard_name" not in result.latitude.attrs:
            result.latitude.attrs["standard_name"] = "latitude"
            result.latitude.attrs["units"] = "degrees_north"

        if "longitude" in result.coords and "standard_name" not in result.longitude.attrs:
            result.longitude.attrs["standard_name"] = "longitude"
            result.longitude.attrs["units"] = "degrees_east"

    elif isinstance(result, xr.DataArray):
        if "latitude" in result.coords and "standard_name" not in result.coords["latitude"].attrs:
            result.coords["latitude"].attrs["standard_name"] = "latitude"
            result.coords["latitude"].attrs["units"] = "degrees_north"

        if "longitude" in result.coords and "standard_name" not in result.coords["longitude"].attrs:
            result.coords["longitude"].attrs["standard_name"] = "longitude"
            result.coords["longitude"].attrs["units"] = "degrees_east"

    return result


def monet_to_coards(
    ds: xr.Dataset | xr.DataArray, add_bounds: bool = True, add_metadata: bool = True, version: str = "CF-1.8"
) -> xr.Dataset | xr.DataArray:
    """Convert a MONET-formatted Dataset or DataArray to COARDS/CF compliant format.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        MONET-formatted dataset to convert to COARDS/CF format.
    add_bounds : bool, default: True
        Whether to add cell bounds for coordinate variables.
    add_metadata : bool, default: True
        Whether to add recommended global metadata attributes.
    version : str, default: "CF-1.8"
        CF Convention version to comply with.

    Returns
    -------
    xarray.Dataset or xarray.DataArray
        COARDS/CF compliant dataset.
    """
    result = ds.copy()

    # Add convention attribute
    result.attrs["Conventions"] = version

    # Add standard metadata if requested
    if add_metadata:
        if "history" not in result.attrs:
            from .conventions import update_history

            update_history(result, "Created by MONET")
        if "institution" not in result.attrs:
            result.attrs["institution"] = "Generated by MONET"
        if "source" not in result.attrs:
            result.attrs["source"] = "MONET Python Package"
        if "references" not in result.attrs:
            result.attrs["references"] = "https://github.com/noaa-oar-arl/monet"

    # Check if this is a curvilinear grid
    is_curvilinear = is_curvilinear_grid(result)

    # Convert 2D lat/lon to 1D coordinates for compatibility (only for rectilinear grids)
    if "x" in result.dims and "y" in result.dims and not is_curvilinear:
        if "latitude" in result.coords and "longitude" in result.coords:
            # Check if coordinates are 2D (MONET format)
            if result["latitude"].ndim == 2 and result["longitude"].ndim == 2:
                # Extract values for 1D coordinates
                if result["latitude"].shape[1] > 1 and result["longitude"].shape[0] > 1:
                    try:
                        # Make sure we're dealing with a rectilinear grid
                        lat_1d = result["latitude"][:, 0]
                        lon_1d = result["longitude"][0, :]

                        # Verify the grid is truly rectilinear by checking if lat/lon are constant along rows/columns
                        # Use lazy check: take difference and see if it's all zero
                        # Explicit compute for metadata check is intentional.
                        lat_diff = abs(result["latitude"] - result["latitude"].isel(x=0)).max()
                        lon_diff = abs(result["longitude"] - result["longitude"].isel(y=0)).max()
                        is_rect = bool(lat_diff < 1e-5) and bool(lon_diff < 1e-5)

                        if is_rect:
                            # Create new 1D coordinate variables with CF attributes
                            # Use .data to preserve laziness if possible (DataArray will handle it)
                            result.coords["lat"] = ("y", lat_1d.data)
                            result.coords["lon"] = ("x", lon_1d.data)

                            # Add CF standard attributes to coordinate variables
                            result["lat"].attrs["standard_name"] = "latitude"
                            result["lat"].attrs["units"] = "degrees_north"
                            result["lat"].attrs["axis"] = "Y"

                            result["lon"].attrs["standard_name"] = "longitude"
                            result["lon"].attrs["units"] = "degrees_east"
                            result["lon"].attrs["axis"] = "X"

                            # Add bounds if requested
                            if add_bounds:
                                # Add bounds variables
                                # For metadata resolution, we compute eagerly
                                dlat = float(abs(lat_1d.diff("y").mean())) if len(lat_1d) > 1 else 0.5
                                dlon = float(abs(lon_1d.diff("x").mean())) if len(lon_1d) > 1 else 0.5

                                # Generate bounds lazily
                                lat_bounds = xr.concat([lat_1d - dlat, lat_1d + dlat], dim="bounds").transpose("y", "bounds")
                                lon_bounds = xr.concat([lon_1d - dlon, lon_1d + dlon], dim="bounds").transpose("x", "bounds")

                                result["lat_bounds"] = lat_bounds
                                result["lon_bounds"] = lon_bounds

                                result["lat"].attrs["bounds"] = "lat_bounds"
                                result["lon"].attrs["bounds"] = "lon_bounds"

                            # Keep 2D lat/lon in the dataset, but rename to comply with CF
                            if isinstance(result, xr.Dataset):
                                result = result.rename(
                                    {
                                        "latitude": "latitude_2d",
                                        "longitude": "longitude_2d",
                                    }
                                )

                                # Add attributes to 2D coordinates
                                result["latitude_2d"].attrs["standard_name"] = "latitude"
                                result["latitude_2d"].attrs["units"] = "degrees_north"
                                result["longitude_2d"].attrs["standard_name"] = "longitude"
                                result["longitude_2d"].attrs["units"] = "degrees_east"
                    except Exception as e:
                        print(f"Warning: Could not convert to 1D coordinates: {e}")

    # For curvilinear grids, ensure 2D lat/lon have proper attributes
    if is_curvilinear and "latitude" in result.coords and "longitude" in result.coords:
        if "standard_name" not in result["latitude"].attrs:
            result["latitude"].attrs["standard_name"] = "latitude"
            result["latitude"].attrs["units"] = "degrees_north"

        if "standard_name" not in result["longitude"].attrs:
            result["longitude"].attrs["standard_name"] = "longitude"
            result["longitude"].attrs["units"] = "degrees_east"

        # Add grid_mapping attribute if needed
        if isinstance(result, xr.Dataset):
            for var_name, var in result.data_vars.items():
                if "grid_mapping" not in var.attrs:
                    var.attrs["coordinates"] = "latitude longitude"

    # If there's a time coordinate, make sure it has CF attributes
    if "time" in result.coords:
        result["time"].attrs["standard_name"] = "time"
        if "units" not in result["time"].attrs:
            # Try to determine time units based on time values
            try:
                # Get reference time close to start time
                t0 = result["time"].values[0]
                ref_date = pd.Timestamp(t0).replace(hour=0, minute=0, second=0, microsecond=0)
                result["time"].attrs["units"] = f"seconds since {ref_date.strftime('%Y-%m-%d %H:%M:%S')}"
            except (IndexError, AttributeError, ValueError, TypeError):
                # Default to standard reference date
                result["time"].attrs["units"] = "seconds since 1970-01-01 00:00:00"

        result["time"].attrs["calendar"] = "standard"
        result["time"].attrs["axis"] = "T"

    # If there's a vertical coordinate, ensure it has proper attributes
    vertical_coords = ["lev", "level", "height", "altitude", "depth", "z"]
    for vc in vertical_coords:
        if vc in result.coords:
            if "standard_name" not in result[vc].attrs:
                if vc in ["height", "altitude", "z"]:
                    result[vc].attrs["standard_name"] = "height"
                    result[vc].attrs["units"] = "m"  # default to meters
                elif vc in ["depth"]:
                    result[vc].attrs["standard_name"] = "depth"
                    result[vc].attrs["units"] = "m"  # default to meters
                else:
                    result[vc].attrs["standard_name"] = "air_pressure"
                    result[vc].attrs["units"] = "hPa"  # default to hPa

            result[vc].attrs["axis"] = "Z"

            # Add bounds if requested and not already present
            if add_bounds and "bounds" not in result[vc].attrs:
                try:
                    vc_da = result[vc]
                    vc_dim = vc_da.dims[0]
                    if len(vc_da) > 1:
                        # Compute mean diff eagerly for metadata
                        dz = float(np.abs(vc_da.diff(vc_dim)).mean()) / 2

                        # Create bounds (approximated for non-uniform grids if necessary)
                        # but keep it lazy
                        vc_lower = vc_da - dz
                        vc_upper = vc_da + dz
                        vc_bounds = xr.concat([vc_lower, vc_upper], dim="bounds").transpose(vc_dim, "bounds")

                        result[f"{vc}_bounds"] = vc_bounds
                        result[vc].attrs["bounds"] = f"{vc}_bounds"
                except Exception as e:
                    print(f"Warning: Could not create bounds for {vc}: {e}")

    # Add variable attributes
    if isinstance(result, xr.Dataset):
        for var_name, var in result.data_vars.items():
            # Skip coordinates
            if var_name in result.coords:
                continue

            # Ensure each variable has at least basic attributes
            if "units" not in var.attrs:
                var.attrs["units"] = "1"  # Dimensionless

            # Variables should have a description
            if "long_name" not in var.attrs and "standard_name" not in var.attrs:
                var.attrs["long_name"] = var_name

    return result


def get_standard_name_mapping():
    """Return a dictionary of common variable names to their CF standard_name.

    Returns
    -------
    dict
        Dictionary mapping variable names to CF standard_names
    """
    return {
        "temp": "air_temperature",
        "temperature": "air_temperature",
        "rh": "relative_humidity",
        "rel_hum": "relative_humidity",
        "relative_humidity": "relative_humidity",
        "ws": "wind_speed",
        "wind_speed": "wind_speed",
        "wd": "wind_from_direction",
        "wind_direction": "wind_from_direction",
        "pres": "air_pressure",
        "pressure": "air_pressure",
        "o3": "mole_fraction_of_ozone_in_air",
        "ozone": "mole_fraction_of_ozone_in_air",
        "co": "mole_fraction_of_carbon_monoxide_in_air",
        "no": "mole_fraction_of_nitrogen_monoxide_in_air",
        "no2": "mole_fraction_of_nitrogen_dioxide_in_air",
        "pm25": "mass_concentration_of_pm2p5_ambient_aerosol_in_air",
        "pm10": "mass_concentration_of_pm10_ambient_aerosol_in_air",
        "aod": "atmosphere_optical_thickness_due_to_ambient_aerosol",
        "aod_550nm": "atmosphere_optical_thickness_due_to_ambient_aerosol",
        "precip": "precipitation_amount",
        "precipitation": "precipitation_amount",
        "height": "height",
        "alt": "height_above_reference_ellipsoid",
        "altitude": "height_above_reference_ellipsoid",
        "elev": "surface_altitude",
        "elevation": "surface_altitude",
    }


def add_cf_standard_names(ds, auto_detect=True, name_mapping=None):
    """Add CF standard_name attributes to variables in a Dataset.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to add standard_names to
    auto_detect : bool, default: True
        Whether to attempt to auto-detect standard names based on variable names
    name_mapping : dict, optional
        Custom mapping of variable names to CF standard_names

    Returns
    -------
    xarray.Dataset
        Dataset with standard_name attributes added
    """
    result = ds.copy()

    # Get standard name mapping
    std_names = get_standard_name_mapping()

    # If custom mapping is provided, update standard mapping
    if name_mapping is not None:
        std_names.update(name_mapping)

    # Assign standard_name for Datasets
    if isinstance(result, xr.Dataset):
        for var_name, var in result.data_vars.items():
            if var_name is None:
                continue
            if "standard_name" in var.attrs:
                continue
            var_name_str = str(var_name)
            assigned = False
            if auto_detect and var_name_str in std_names:
                var.attrs["standard_name"] = std_names[var_name_str]
                assigned = True
            elif auto_detect:
                for pattern, std_name in std_names.items():
                    if str(pattern).lower() == var_name_str.lower():
                        var.attrs["standard_name"] = std_name
                        assigned = True
                        break
            if not assigned:
                var.attrs["standard_name"] = var_name_str
    # Assign standard_name for DataArray
    elif isinstance(result, xr.DataArray):
        var_name = result.name
        if var_name is not None and "standard_name" not in result.attrs:
            var_name_str = str(var_name)
            assigned = False
            if auto_detect and var_name_str in std_names:
                result.attrs["standard_name"] = std_names[var_name_str]
                assigned = True
            elif auto_detect:
                for pattern, std_name in std_names.items():
                    if str(pattern).lower() == var_name_str.lower():
                        result.attrs["standard_name"] = std_name
                        assigned = True
                        break
            if not assigned:
                result.attrs["standard_name"] = var_name_str
    return result
