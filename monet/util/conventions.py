"""Convention-aware coordinate and grid detection for MONET.

This module provides utilities to identify spatial coordinates (latitude, longitude)
and grid types (rectilinear, curvilinear, unstructured) across various metadata
conventions including CF, COARDS, and UGRID.
"""

import datetime
import typing as t

import numpy as np
import xarray as xr

try:
    import cf_xarray  # noqa: F401

    HAS_CF = True
except ImportError:
    HAS_CF = False


def get_non_spatial_dims(obj: xr.Dataset | xr.DataArray) -> set[str]:
    """Identify dimensions that are likely not spatial (Time, Vertical).

    Parameters
    ----------
    obj : xarray.Dataset or xarray.DataArray
        The object to inspect.

    Returns
    -------
    set of str
        Names of non-spatial dimensions.
    """
    non_spatial_dims = set()

    # 1. Use cf-xarray axes if available
    if HAS_CF:
        try:
            if "T" in obj.cf.axes:
                non_spatial_dims.update(obj.cf.axes["T"])
            if "Z" in obj.cf.axes:
                non_spatial_dims.update(obj.cf.axes["Z"])
        except (KeyError, AttributeError):
            pass

    # 2. Heuristics based on dimension names
    time_names = ["time", "t", "tden", "time_counter", "t_step", "Time"]
    vert_names = [
        "lev",
        "level",
        "depth",
        "pressure",
        "sigma",
        "pres",
        "height",
        "altitude",
        "z",
        "Vertical",
        "layer",
        "bottom_top",
    ]

    for dim in obj.dims:
        dim_lower = str(dim).lower()
        if dim_lower in time_names or dim_lower in vert_names:
            non_spatial_dims.add(str(dim))

        # 3. Dtype check for time
        if dim in obj.coords:
            dtype = obj[dim].dtype
            if np.issubdtype(dtype, np.datetime64) or np.issubdtype(dtype, np.timedelta64):
                non_spatial_dims.add(str(dim))
            # Check for cftime
            try:
                if isinstance(obj[dim].to_index(), xr.CFTimeIndex):
                    non_spatial_dims.add(str(dim))
            except (ImportError, TypeError, AttributeError):
                pass

    return non_spatial_dims


def find_coords(obj: xr.Dataset | xr.DataArray, key: str) -> xr.DataArray | None:
    """Find a coordinate in an xarray object by CF standard name or common name.

    Parameters
    ----------
    obj : xarray.Dataset or xarray.DataArray
        The object to search.
    key : str
        The coordinate type ('latitude' or 'longitude').

    Returns
    -------
    xarray.DataArray or None
        The found coordinate DataArray, or None.
    """
    if HAS_CF:
        try:
            return obj.cf[key]
        except (KeyError, AttributeError):
            try:
                matches = obj.cf.coordinates.get(key, [])
                if not matches:
                    matches = obj.cf.axes.get(key, [])

                if matches:
                    # Prefer one that matches dimensions
                    if isinstance(obj, xr.DataArray):
                        for m in matches:
                            if set(obj[m].dims).issubset(set(obj.dims)):
                                return obj[m]
                    elif isinstance(obj, xr.Dataset) and len(obj.data_vars) > 0:
                        # Try to find coord that matches one of the data variables
                        for var_name in obj.data_vars:
                            da = obj[var_name]
                            if da.attrs.get("cf_role") not in ["mesh_topology"]:
                                for m in matches:
                                    if set(obj[m].dims).issubset(set(da.dims)):
                                        return obj[m]
                    return obj[matches[0]]
            except Exception:
                pass

    # 2. UGRID detection for Dataset
    if isinstance(obj, xr.Dataset):
        info = get_ugrid_info(obj)
        if info:
            # Check node, face, then edge coordinates
            for attr in ["node_coordinates", "face_coordinates", "edge_coordinates"]:
                coords_names = info.get(attr, [])
                if len(coords_names) >= 2:
                    c1_name, c2_name = coords_names[0], coords_names[1]
                    if c1_name in obj and c2_name in obj:
                        c1, c2 = obj[c1_name], obj[c2_name]
                        if key == "latitude":
                            if any(x in c1_name.lower() for x in ["lat", "y"]) or c1.attrs.get("standard_name") == "latitude":
                                return c1
                            if any(x in c2_name.lower() for x in ["lat", "y"]) or c2.attrs.get("standard_name") == "latitude":
                                return c2
                        else:
                            if any(x in c1_name.lower() for x in ["lon", "x"]) or c1.attrs.get("standard_name") == "longitude":
                                return c1
                            if any(x in c2_name.lower() for x in ["lon", "x"]) or c2.attrs.get("standard_name") == "longitude":
                                return c2

    # 3. Check for units (legacy support and fallback)
    check_obj = obj.variables if hasattr(obj, "variables") else obj.coords
    for var in check_obj:
        attrs = obj[var].attrs
        if "units" in attrs:
            u = str(attrs["units"]).lower()
            if key == "latitude":
                if any(x in u for x in ["degrees_north", "degree_north", "degree_n", "degrees_n"]):
                    return obj[var]
            elif key == "longitude":
                if any(x in u for x in ["degrees_east", "degree_east", "degree_e", "degrees_e"]):
                    return obj[var]

    # 4. Fallback to common names
    names = {
        "latitude": [
            "latitude",
            "lat",
            "Latitude",
            "LATITUDE",
            "LAT",
            "y",
            "XLAT",
            "XLAT_M",
            "grid_yt",
            "nav_lat",
            "NY",
            "lat_b",
            "lat_centers",
            "latCell",
            "lat_face",
            "lat_node",
            "node_lat",
            "face_lat",
            "edge_lat",
        ],
        "longitude": [
            "longitude",
            "lon",
            "Longitude",
            "LONGITUDE",
            "LON",
            "x",
            "XLONG",
            "XLONG_M",
            "grid_xt",
            "nav_lon",
            "NX",
            "lon_b",
            "lon_centers",
            "lonCell",
            "lon_face",
            "lon_node",
            "node_lon",
            "face_lon",
            "edge_lon",
        ],
    }

    check_list = names.get(key, [])

    # Check coords first
    for name in check_list:
        if name in obj.coords:
            return obj[name]

    # Check data_vars if Dataset
    if isinstance(obj, xr.Dataset):
        for name in check_list:
            if name in obj.data_vars:
                return obj[name]

    # Case insensitive search
    for c in obj.coords:
        if str(c).lower() in [n.lower() for n in check_list]:
            return obj[c]

    return None


def detect_grid_type(obj: xr.Dataset | xr.DataArray) -> str:
    """Detect the grid type of an xarray object.

    Parameters
    ----------
    obj : xarray.Dataset or xarray.DataArray
        The object to check.

    Returns
    -------
    str
        'rectilinear', 'curvilinear', 'unstructured', or 'unknown'.
    """
    # Detect UGRID
    is_ugrid = False
    if isinstance(obj, xr.Dataset):
        for var in obj.variables:
            if obj[var].attrs.get("cf_role") == "mesh_topology":
                is_ugrid = True
                break

    # Detect via attributes on DataArray
    if not is_ugrid and "mesh" in obj.attrs:
        is_ugrid = True

    if is_ugrid:
        return "unstructured"

    lat = find_coords(obj, "latitude")
    lon = find_coords(obj, "longitude")

    if lat is None or lon is None:
        return "unknown"

    # Filter non-spatial dims for dimension check
    non_spatial = get_non_spatial_dims(obj)
    lat_spatial_dims = [d for d in lat.dims if d not in non_spatial]

    # Handle UXarray objects if present
    if hasattr(obj, "uxgrid"):
        return "unstructured"

    if lat.ndim == 1:
        if lat.dims == lon.dims or is_ugrid:
            return "unstructured"
        else:
            return "rectilinear"
    elif lat.ndim == 2:
        # Check if it's actually rectilinear but stored as 2D
        if len(lat_spatial_dims) == 2:
            return "curvilinear"

    # Higher dimensions or other cases
    if lat.ndim > 2:
        return "curvilinear"

    return "unknown"


def update_history(obj: xr.Dataset | xr.DataArray, msg: str) -> xr.Dataset | xr.DataArray:
    """Update the history attribute of an xarray object with a timestamp.

    Parameters
    ----------
    obj : xarray.Dataset or xarray.DataArray
        The object to update.
    msg : str
        The message to add to the history.

    Returns
    -------
    xarray.Dataset or xarray.DataArray
        The object with updated history.
    """
    curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history = obj.attrs.get("history", "")
    new_history = (history + f"\n{curr_time} > {msg}").strip()
    obj.attrs["history"] = new_history
    return obj


def get_ugrid_info(obj: xr.Dataset | xr.DataArray) -> dict[str, t.Any]:
    """Extract UGRID mesh topology information.

    Parameters
    ----------
    obj : xarray.Dataset or xarray.DataArray
        The object to inspect.

    Returns
    -------
    dict
        Mesh topology metadata including mesh name, dimension, and coordinate names.
    """
    if isinstance(obj, xr.DataArray):
        # Check for 'mesh' attribute
        mesh_name = obj.attrs.get("mesh")
        if not mesh_name:
            # Fallback to searching coordinates for cf_role
            for coord in obj.coords:
                if obj[coord].attrs.get("cf_role") == "mesh_topology":
                    mesh_name = coord
                    break
        if not mesh_name:
            return {}
        # We can't easily get the topology variable if it's not in the DataArray's coords,
        # but often the DataArray's own coords have the necessary info.
        return {"mesh_name": mesh_name}

    topology_vars = [v for v in obj.variables if obj[v].attrs.get("cf_role") == "mesh_topology"]
    if not topology_vars:
        return {}

    topo_var = topology_vars[0]
    attrs = obj[topo_var].attrs

    info = {
        "mesh_name": topo_var,
        "topology_dimension": attrs.get("topology_dimension"),
        "node_coordinates": attrs.get("node_coordinates", "").split(),
        "face_node_connectivity": attrs.get("face_node_connectivity"),
        "face_coordinates": attrs.get("face_coordinates", "").split(),
        "edge_coordinates": attrs.get("edge_coordinates", "").split(),
    }
    return info


def get_ugrid_coords(obj: xr.Dataset, da: xr.DataArray) -> tuple[xr.DataArray | None, xr.DataArray | None]:
    """Get latitude and longitude coordinates for a UGRID DataArray.

    Parameters
    ----------
    obj : xarray.Dataset
        The parent dataset containing coordinate variables.
    da : xarray.DataArray
        The data array belonging to a mesh.

    Returns
    -------
    tuple
        (latitude, longitude) DataArrays if found, otherwise (None, None).
    """
    info = get_ugrid_info(obj)
    if not info:
        return None, None

    location = da.attrs.get("location", "node")  # Default to node per UGRID spec

    if location == "node":
        coords_names = info.get("node_coordinates", [])
    elif location == "face":
        coords_names = info.get("face_coordinates", [])
    elif location == "edge":
        coords_names = info.get("edge_coordinates", [])
    else:
        coords_names = []

    if len(coords_names) >= 2:
        # Identify lat/lon by name heuristics
        c1_name, c2_name = coords_names[0], coords_names[1]
        c1, c2 = obj[c1_name], obj[c2_name]

        if any(x in c1_name.lower() for x in ["lat", "y"]):
            return c1, c2
        elif any(x in c2_name.lower() for x in ["lat", "y"]):
            return c2, c1
        else:
            # Check standard_name
            if c1.attrs.get("standard_name") == "latitude":
                return c1, c2
            if c2.attrs.get("standard_name") == "latitude":
                return c2, c1
            # Default to (c2, c1) as UGRID often uses (lon, lat) order in attrs
            return c2, c1

    return None, None
