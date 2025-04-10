"""Interpolation utility functions for MONET"""

import xarray as xr
import numpy as np

def latlon_xarray_to_CoordinateDefinition(longitude=None, latitude=None):
    """Create a pyresample CoordinateDefinition from xarray DataArrays.

    Parameters
    ----------
    longitude : xarray.DataArray
        2D longitude array. Must be monotonically increasing in range -180 to 180.
    latitude : xarray.DataArray
        2D latitude array. Must be monotonically increasing in range -90 to 90.

    Returns
    -------
    pyresample.geometry.CoordinateDefinition
        CoordinateDefinition object created from the given lat/lon arrays.
    """
    from pyresample import geometry

    return geometry.CoordinateDefinition(lats=latitude, lons=longitude)


def lonlat_to_xesmf(longitude=None, latitude=None):
    """Create an empty xarray.Dataset with longitude and latitude coordinates.

    Parameters
    ----------
    longitude : float or array-like
        Longitude value(s).
    latitude : float or array-like
        Latitude value(s).

    Returns
    -------
    xarray.Dataset
        An empty dataset with the given longitude and latitude as coordinates.
    """
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
        Longitude values. Can be 1D or 2D.
    latitude : array-like
        Latitude values. Can be 1D or 2D.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        SwathDefinition object created from the given lon/lat arrays.
    """
    from numpy import vstack, meshgrid
    from pyresample.geometry import SwathDefinition

    if len(longitude.shape) < 2:
        lons, lats = meshgrid(longitude, latitude)
    else:
        lons, lats = longitude, latitude

    return SwathDefinition(lons=lons, lats=lats)


def nearest_point_swathdefinition(longitude=None, latitude=None):
    """Create a SwathDefinition for a single point.

    Parameters
    ----------
    longitude : float
        Longitude of the point.
    latitude : float
        Latitude of the point.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        SwathDefinition object representing a single point.
    """
    from numpy import vstack
    from pyresample.geometry import SwathDefinition

    lons = vstack([longitude])
    lats = vstack([latitude])
    return SwathDefinition(lons=lons, lats=lats)


def constant_1d_xesmf(longitude=None, latitude=None):
    """Create an xESMF compatible dataset with a constant latitude.

    Parameters
    ----------
    longitude : array-like
        Longitude values. Can be 1D or 2D.
    latitude : array-like
        Latitude values to use as constant.

    Returns
    -------
    xarray.Dataset
        Dataset suitable for xESMF with lon/lat coordinates.
    """
    from numpy import asarray

    lat = asarray(latitude)
    lon = asarray(longitude)
    s = lat.shape[0]
    dset = xr.Dataset(
        coords={"lon": (["x", "y"], lon.reshape(s, 1)), "lat": (["x", "y"], lat.reshape(s, 1))}
    )
    return dset


def constant_lat_swathdefition(longitude=None, latitude=None):
    """Create a SwathDefinition with constant latitude.

    Creates a grid where all points have the same latitude value.

    Parameters
    ----------
    longitude : array-like
        Longitude values. Can be 1D or 2D.
    latitude : float
        Constant latitude value to use for all points.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        SwathDefinition with constant latitude.
    """
    from numpy import vstack, meshgrid
    from pyresample import geometry
    from xarray import DataArray

    if len(longitude.shape) < 2:
        lons, lats = meshgrid(longitude, latitude)
    else:
        lons = longitude
        lats = lons * 0.0 + latitude

    return geometry.SwathDefinition(lons=lons, lats=lats)


def constant_lon_swathdefition(longitude=None, latitude=None):
    """Create a SwathDefinition with constant longitude.

    Creates a grid where all points have the same longitude value.

    Parameters
    ----------
    longitude : float
        Constant longitude value to use for all points.
    latitude : array-like
        Latitude values. Can be 1D or 2D.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        SwathDefinition with constant longitude.
    """
    from numpy import vstack, meshgrid
    from pyresample import geometry
    from xarray import DataArray

    if len(latitude.shape) < 2:
        lats, lons = meshgrid(latitude, longitude)
        lons = lons.T
        lats = lats.T
    else:
        lats = latitude
        lons = lats * 0.0 + longitude

    return geometry.SwathDefinition(lons=lons, lats=lats)
