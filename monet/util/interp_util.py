"""Interpolation functions"""


def latlon_xarray_to_CoordinateDefinition(longitude=None, latitude=None):
    """Create pyresample SwathDefinition from xarray object.

    Converts xarray latitude and longitude coordinate arrays into a
    pyresample CoordinateDefinition suitable for spatial interpolation.

    Parameters
    ----------
    longitude : 2D xarray.DataArray
        Longitude array -> must be from -180 -> 180 and monotonically increasing.
    latitude : 2D xarray.DataArray
        Latitude array -> must be from -90 -> 90 and monotonically increasing.

    Returns
    -------
    pyresample.geometry.CoordinateDefinition
        A coordinate definition object that can be used with pyresample.
    """
    from pyresample import geometry

    return geometry.CoordinateDefinition(lats=latitude, lons=longitude)


def lonlat_to_xesmf(longitude=None, latitude=None):
    """Create an empty xarray.Dataset with longitude and latitude coordinates.

    Creates a minimal xarray Dataset with the provided coordinates to be used
    as a target grid for xESMF regridding.

    Parameters
    ----------
    longitude : array-like
        Longitude value(s).
    latitude : array-like
        Latitude value(s).

    Returns
    -------
    xarray.Dataset
        A dataset with lon/lat coordinates suitable for use with xesmf.
    """
    import xarray as xr
    from numpy import asarray

    lat = asarray(latitude)
    lon = asarray(longitude)
    dset = xr.Dataset(
        coords={"lon": (["x", "y"], lon.reshape(1, 1)), "lat": (["x", "y"], lat.reshape(1, 1))}
    )
    return dset


def lonlat_to_swathdefinition(longitude=None, latitude=None):
    """Create a pyresample SwathDefinition from longitude and latitude arrays.

    Parameters
    ----------
    longitude : array-like
        Longitude values, either 1D or 2D.
    latitude : array-like
        Latitude values, either 1D or 2D.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        A SwathDefinition object for the provided coordinates.
    """
    from numpy import vstack
    from pyresample.geometry import SwathDefinition

    if len(longitude.shape) < 2:
        lons = vstack(longitude)
        lats = vstack(latitude)
    else:
        lons = longitude
        lats = latitude
    return SwathDefinition(lons=lons, lats=lats)


def nearest_point_swathdefinition(longitude=None, latitude=None):
    """Create a SwathDefinition for a single point.

    Used for nearest neighbor point-to-point interpolation.

    Parameters
    ----------
    longitude : float
        Longitude of the point.
    latitude : float
        Latitude of the point.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        A SwathDefinition representing a single point.
    """
    from numpy import vstack
    from pyresample.geometry import SwathDefinition

    lons = vstack([longitude])
    lats = vstack([latitude])
    return SwathDefinition(lons=lons, lats=lats)


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
        A dataset with coordinates suitable for xesmf, where longitude varies
        but latitude is constant.
    """
    import xarray as xr
    from numpy import asarray

    lat = asarray(latitude)
    lon = asarray(longitude)
    s = lat.shape[0]
    dset = xr.Dataset(
        coords={"lon": (["x", "y"], lon.reshape(s, 1)), "lat": (["x", "y"], lat.reshape(s, 1))}
    )
    return dset


def constant_lat_swathdefition(longitude=None, latitude=None):
    """Create a SwathDefinition with constant latitude along a longitude array.

    Parameters
    ----------
    longitude : array-like
        Array of longitude values, 1D or 2D.
    latitude : float
        Constant latitude value to use for all points.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        A SwathDefinition with constant latitude.
    """
    from numpy import vstack
    from pyresample import geometry
    from xarray import DataArray

    if len(longitude.shape) < 2:
        lons = vstack(longitude)
    else:
        lons = longitude
    lats = lons * 0.0 + latitude
    if isinstance(lats, DataArray):
        lats.name = "lats"
    return geometry.SwathDefinition(lons=lons, lats=lats)


def constant_lon_swathdefition(longitude=None, latitude=None):
    """Create a SwathDefinition with constant longitude along a latitude array.

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
    from numpy import vstack
    from pyresample import geometry
    from xarray import DataArray

    if len(latitude.shape) < 2:
        lats = vstack(latitude)
    else:
        lats = latitude
    lons = lats * 0.0 + longitude
    if isinstance(lats, DataArray):
        lons.name = "lons"
    return geometry.SwathDefinition(lons=lons, lats=lats)
