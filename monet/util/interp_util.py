"""Interpolation utility functions for MONET"""

import numpy as np
import xarray as xr


def latlon_xarray_to_CoordinateDefinition(longitude=None, latitude=None):
    """Deprecated: Create pyresample SwathDefinition from xarray object.

    This function was part of the pyresample dependency and is deprecated.
    """
    raise NotImplementedError(
        "This function relies on pyresample which has been removed."
    )


def lonlat_to_xesmf(longitude=None, latitude=None):
    """Deprecated: Create an empty xarray.Dataset with longitude and latitude coordinates.

    This function was part of the xesmf dependency and is deprecated.
    """
    from numpy import asarray, meshgrid

    lat = asarray(latitude)
    lon = asarray(longitude)

    # Handle scalar values
    if lat.ndim == 0:
        lat = lat[None]
    if lon.ndim == 0:
        lon = lon[None]

    # If both are 1D, create a 2D meshgrid
    if lat.ndim == 1 and lon.ndim == 1:
        lon_2d, lat_2d = meshgrid(lon, lat)
    # If both are already 2D with same shape, use them directly
    elif lat.ndim == 2 and lon.ndim == 2 and lat.shape == lon.shape:
        lon_2d, lat_2d = lon, lat
    # If they have different shapes or dimensions, create meshgrid
    else:
        if lat.ndim > 1:
            lat = lat.flatten()
        if lon.ndim > 1:
            lon = lon.flatten()
        lon_2d, lat_2d = meshgrid(lon, lat)

    dset = xr.Dataset(coords={"lon": (["y", "x"], lon_2d), "lat": (["y", "x"], lat_2d)})
    return dset


def lonlat_to_swathdefinition(longitude=None, latitude=None):
    """Deprecated: Create a pyresample SwathDefinition from longitude and latitude arrays."""
    raise NotImplementedError(
        "This function relies on pyresample which has been removed."
    )


def nearest_point_swathdefinition(longitude=None, latitude=None):
    """Deprecated: Create a SwathDefinition for a single point."""
    raise NotImplementedError(
        "This function relies on pyresample which has been removed."
    )


def constant_1d_xesmf(longitude=None, latitude=None):
    """Create a dataset with a constant latitude along a longitude array.

    Parameters
    ----------
    longitude : array-like
        Array of longitude values.
    latitude : float or array-like
        Latitude value(s) to use as a constant.

    Returns
    -------
    xarray.Dataset
        A dataset with coordinates suitable for regridding, where longitude varies
        but latitude is constant.
    """
    from numpy import asarray

    lat = asarray(latitude)
    lon = asarray(longitude)
    if lat.ndim == 0:
        lat = lat[None]
    if lon.ndim == 0:
        lon = lon[None]
    s = lat.shape[0]
    dset = xr.Dataset(
        coords={
            "lon": (["x", "y"], lon.reshape(s, 1)),
            "lat": (["x", "y"], lat.reshape(s, 1)),
        }
    )
    return dset


def constant_lat_swathdefition(longitude=None, latitude=None):
    """Deprecated: Create a SwathDefinition with constant latitude along a longitude array."""
    raise NotImplementedError(
        "This function relies on pyresample which has been removed."
    )


def constant_lon_swathdefition(longitude=None, latitude=None):
    """Deprecated: Create a SwathDefinition with constant longitude along a latitude array
    Parameters
    ----------
    longitude : float
        Constant longitude value to use for all points.
    latitude : array-like
        Array of latitude values, 1D or 2D.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        A SwathDefinition with constant longitude.
    """
    from numpy import meshgrid
    from pyresample import geometry

    latitude = np.asarray(latitude)
    if latitude.ndim == 1:
        lats, lons = meshgrid(latitude, np.array([longitude]))
        lons = lons.T
        lats = lats.T
    else:
        lats = latitude
        lons = np.full_like(lats, longitude)
    return geometry.SwathDefinition(lons=lons, lats=lats)


def create_area_def_from_latlon(
    lat, lon, projection="platea", resolution=None, area_id=None
):
    """Create a pyresample AreaDefinition from latitude and longitude arrays.

    Parameters
    ----------
    lat : numpy.ndarray
        2D latitude array or 1D latitude coordinate
    lon : numpy.ndarray
        2D longitude array or 1D longitude coordinate
    projection : str, default: 'platea'
        Projection name. Options include:
        - 'platea': Plate Carrée (equidistant cylindrical)
        - 'lcc': Lambert Conformal Conic
        - 'merc': Mercator
        - 'stere': Stereographic
        - 'gnom': Gnomonic (used by UFS SRW)
    resolution : float, optional
        Resolution in meters. If None, calculate from data.
    area_id : str, optional
        Identifier for the area. Default is 'generated_area'.

    Returns
    -------
    pyresample.geometry.AreaDefinition
        An AreaDefinition object representing the data's grid.

    Notes
    -----
    For non-regular grids, SwathDefinition might be more appropriate than AreaDefinition.
    """
    import pyproj
    from pyresample.geometry import AreaDefinition

    # Determine dimensions and boundaries
    # Optimization: avoid creating 2D meshgrid if inputs are 1D, as we only need min/max and shape
    if lat.ndim == 1 and lon.ndim == 1:
        height = lat.size
        width = lon.size
        lat_min = lat.min()
        lat_max = lat.max()
        lon_min = lon.min()
        lon_max = lon.max()
    else:
        lat_2d, lon_2d = lat, lon
        height, width = lat_2d.shape
        lat_min = lat_2d.min()
        lat_max = lat_2d.max()
        lon_min = lon_2d.min()
        lon_max = lon_2d.max()

    # Set the area_id
    if area_id is None:
        area_id = "generated_area"

    # Setup projection based on the input data
    if projection == "platea":
        # Plate Carrée projection (equidistant cylindrical)
        proj_dict = {
            "proj": "eqc",
            "lat_ts": 0,
            "lat_0": 0,
            "lon_0": 0,
            "x_0": 0,
            "y_0": 0,
            "ellps": "WGS84",
        }

        # Convert lat/lon to projection coordinates
        p = pyproj.Proj(proj_dict)
        x_ll, y_ll = p(lon_min, lat_min)
        x_ur, y_ur = p(lon_max, lat_max)
        area_extent = (x_ll, y_ll, x_ur, y_ur)

    elif projection == "lcc":
        # Lambert Conformal Conic projection
        center_lat = (lat_min + lat_max) / 2
        center_lon = (lon_min + lon_max) / 2
        lat_1 = center_lat - (center_lat - lat_min) * 0.33
        lat_2 = center_lat + (lat_max - center_lat) * 0.33

        proj_dict = {
            "proj": "lcc",
            "lat_0": center_lat,
            "lon_0": center_lon,
            "lat_1": lat_1,
            "lat_2": lat_2,
            "ellps": "WGS84",
        }

        # Convert lat/lon to projection coordinates
        p = pyproj.Proj(proj_dict)
        x_ll, y_ll = p(lon_min, lat_min)
        x_ur, y_ur = p(lon_max, lat_max)
        area_extent = (x_ll, y_ll, x_ur, y_ur)

    elif projection == "merc":
        # Mercator projection
        proj_dict = {"proj": "merc", "lat_ts": 0, "ellps": "WGS84"}

        # Convert lat/lon to projection coordinates
        p = pyproj.Proj(proj_dict)
        x_ll, y_ll = p(lon_min, lat_min)
        x_ur, y_ur = p(lon_max, lat_max)
        area_extent = (x_ll, y_ll, x_ur, y_ur)

    elif projection == "stere":
        # Stereographic projection
        center_lat = (lat_min + lat_max) / 2
        center_lon = (lon_min + lon_max) / 2

        proj_dict = {
            "proj": "stere",
            "lat_0": center_lat,
            "lon_0": center_lon,
            "lat_ts": center_lat,
            "ellps": "WGS84",
        }

        # Convert lat/lon to projection coordinates
        p = pyproj.Proj(proj_dict)
        x_ll, y_ll = p(lon_min, lat_min)
        x_ur, y_ur = p(lon_max, lat_max)
        area_extent = (x_ll, y_ll, x_ur, y_ur)

    elif projection == "gnom" or projection == "gnomonic":
        # Gnomonic projection (used by UFS SRW)
        center_lat = (lat_min + lat_max) / 2
        center_lon = (lon_min + lon_max) / 2

        proj_dict = {
            "proj": "gnom",
            "lat_0": center_lat,
            "lon_0": center_lon,
            "ellps": "WGS84",
        }

        # Convert lat/lon to projection coordinates
        p = pyproj.Proj(proj_dict)
        x_ll, y_ll = p(lon_min, lat_min)
        x_ur, y_ur = p(lon_max, lat_max)
        area_extent = (x_ll, y_ll, x_ur, y_ur)

    else:
        raise ValueError(f"Unsupported projection: {projection}")

    # Create the AreaDefinition
    description = f"Generated area definition ({projection})"
    proj_id = projection

    return AreaDefinition(
        area_id, description, proj_id, proj_dict, width, height, area_extent
    )


def create_area_def_from_dataset(
    dataset, projection="platea", resolution=None, area_id=None
):
    """Create an AreaDefinition from an xarray Dataset or DataArray.

    Parameters
    ----------
    dataset : xarray.Dataset or xarray.DataArray
        Dataset or DataArray containing latitude and longitude coordinates
    projection : str, default: 'platea'
        Projection name. Options include:
        - 'platea': Plate Carrée (equidistant cylindrical)
        - 'lcc': Lambert Conformal Conic
        - 'merc': Mercator
        - 'stere': Stereographic
        - 'gnom': Gnomonic (used by UFS SRW)
    resolution : float, optional
        Resolution in meters. If None, calculate from data.
    area_id : str, optional
        Identifier for the area. Default is 'generated_area'.

    Returns
    -------
    pyresample.geometry.AreaDefinition
        An AreaDefinition object representing the dataset's grid.
    """
    from ..accessors.base import BaseAccessor

    # Get lat/lon coordinates
    dataset = BaseAccessor._dataset_to_monet(dataset)

    return create_area_def_from_latlon(
        dataset.latitude.values,
        dataset.longitude.values,
        projection=projection,
        resolution=resolution,
        area_id=area_id,
    )


def get_grid_area_def(
    lat_min,
    lat_max,
    lon_min,
    lon_max,
    resolution=0.1,
    projection="platea",
    area_id=None,
):
    """Create an AreaDefinition for a regular grid based on bounds and resolution.

    Parameters
    ----------
    lat_min : float
        Minimum latitude
    lat_max : float
        Maximum latitude
    lon_min : float
        Minimum longitude
    lon_max : float
        Maximum longitude
    resolution : float, default: 0.1
        Resolution in degrees
    projection : str, default: 'platea'
        Projection name. Options include:
        - 'platea': Plate Carrée (equidistant cylindrical)
        - 'lcc': Lambert Conformal Conic
        - 'merc': Mercator
        - 'stere': Stereographic
        - 'gnom': Gnomonic (used by UFS SRW)
    area_id : str, optional
        Identifier for the area. Default is 'regular_grid'.

    Returns
    -------
    pyresample.geometry.AreaDefinition
        An AreaDefinition object representing the regular grid.
    """
    import numpy as np

    # Create regular grid
    lat = np.arange(lat_min, lat_max + resolution, resolution)
    lon = np.arange(lon_min, lon_max + resolution, resolution)

    lon_2d, lat_2d = np.meshgrid(lon, lat)

    return create_area_def_from_latlon(
        lat_2d, lon_2d, projection=projection, area_id=area_id
    )


def create_area_def_from_esmf_mesh(
    mesh, projection="platea", resolution=None, area_id=None
):
    """Create a pyresample AreaDefinition from an ESMF Mesh.

    Parameters
    ----------
    mesh : ESMF.Mesh
        ESMF Mesh object containing unstructured grid information
    projection : str, default: 'platea'
        Projection name. Options include:
        - 'platea': Plate Carrée (equidistant cylindrical)
        - 'lcc': Lambert Conformal Conic
        - 'merc': Mercator
        - 'stere': Stereographic
        - 'gnom': Gnomonic (used by UFS SRW)
    resolution : float, optional
        Resolution in meters. If None, calculate from data.
    area_id : str, optional
        Identifier for the area. Default is 'esmf_mesh_area'.

    Returns
    -------
    pyresample.geometry.AreaDefinition
        An AreaDefinition object representing the mesh's grid bounds.

    Notes
    -----
    This creates a regular-grid approximation of the unstructured mesh.
    For accurate operations on unstructured grids, consider using
    pyresample's SwathDefinition or ESMF's native capabilities.
    """
    try:
        import ESMF
    except ImportError:
        try:
            import esmpy as ESMF
        except ImportError:
            raise ImportError("ESMF is required for this functionality")

    # Extract node coordinates from the mesh
    node_coords = mesh.get_coords()

    if mesh.coord_sys == ESMF.CoordSys.SPH_DEG:
        # Coordinates are in degrees (longitude/latitude)
        lons = node_coords[0]
        lats = node_coords[1]
    else:
        # For other coordinate systems, try to convert or raise an error
        raise ValueError("Only spherical degree coordinate system is supported")

    # Create AreaDefinition from extracted coordinates
    if area_id is None:
        area_id = "esmf_mesh_area"

    return create_area_def_from_latlon(lats, lons, projection, resolution, area_id)


def create_area_def_from_ugrid(
    ugrid_dataset, projection="platea", resolution=None, area_id=None
):
    """Create a pyresample AreaDefinition from a UGRID-compliant dataset.

    Parameters
    ----------
    ugrid_dataset : xarray.Dataset
        Dataset following the UGRID conventions with mesh topology
    projection : str, default: 'platea'
        Projection name. Options include:
        - 'platea': Plate Carrée (equidistant cylindrical)
        - 'lcc': Lambert Conformal Conic
        - 'merc': Mercator
        - 'stere': Stereographic
        - 'gnom': Gnomonic (used by UFS SRW)
    resolution : float, optional
        Resolution in meters. If None, calculate from data.
    area_id : str, optional
        Identifier for the area. Default is 'ugrid_area'.

    Returns
    -------
    pyresample.geometry.AreaDefinition
        An AreaDefinition object representing the UGRID mesh bounds.

    Notes
    -----
    This creates a regular-grid approximation of the unstructured mesh.
    For accurate operations on unstructured grids, consider using
    pyresample's SwathDefinition or xESMF's ESMF-based regridding.
    """

    # First, identify the mesh topology variable
    mesh_topology_var = None
    for var in ugrid_dataset.variables:
        if (
            hasattr(ugrid_dataset[var], "cf_role")
            and ugrid_dataset[var].cf_role == "mesh_topology"
        ):
            mesh_topology_var = var
            break

    if mesh_topology_var is None:
        raise ValueError("No mesh_topology variable found in the dataset")

    # Find node coordinates
    topology = ugrid_dataset[mesh_topology_var]
    if hasattr(topology, "node_coordinates"):
        node_coords = topology.node_coordinates.split()
        if len(node_coords) >= 2:
            lon_var, lat_var = node_coords[0], node_coords[1]
            lons = ugrid_dataset[lon_var].values
            lats = ugrid_dataset[lat_var].values
        else:
            raise ValueError("Not enough node coordinates specified")
    else:
        # Try to find by standard names
        lon_var = None
        lat_var = None
        for var in ugrid_dataset.variables:
            if hasattr(ugrid_dataset[var], "standard_name"):
                if ugrid_dataset[var].standard_name == "longitude":
                    lon_var = var
                elif ugrid_dataset[var].standard_name == "latitude":
                    lat_var = var

        if lon_var is None or lat_var is None:
            raise ValueError("Could not identify latitude and longitude variables")

        lons = ugrid_dataset[lon_var].values
        lats = ugrid_dataset[lat_var].values

    # Create AreaDefinition from extracted coordinates
    if area_id is None:
        area_id = "ugrid_area"

    return create_area_def_from_latlon(lats, lons, projection, resolution, area_id)


def mesh_to_swath_definition(mesh):
    """Convert an ESMF Mesh to a pyresample SwathDefinition.

    Parameters
    ----------
    mesh : ESMF.Mesh
        ESMF Mesh object containing unstructured grid information

    Returns
    -------
    pyresample.geometry.SwathDefinition
        A SwathDefinition object representing the mesh nodes

    Notes
    -----
    This is more appropriate than AreaDefinition for unstructured grids.
    """
    raise NotImplementedError(
        "This function relies on pyresample which has been removed."
    )
