"""Interpolation utility functions for MONET"""

import typing as t

import numpy as np
import xarray as xr


def lonlat_to_dataset(longitude: t.Sequence[float] | np.ndarray, latitude: t.Sequence[float] | np.ndarray) -> xr.Dataset:
    """Create a Dataset with longitude and latitude coordinates.
    Supports both 1D coordinates (creates a meshgrid) and 2D arrays.

    Parameters
    ----------
    longitude : array-like
        Longitude values.
    latitude : array-like
        Latitude values.

    Returns
    -------
    xarray.Dataset
        Dataset with 'lon' and 'lat' coordinates on (y, x) dimensions.
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

    dset = xr.Dataset(
        coords={
            "longitude": (["y", "x"], lon_2d, {"standard_name": "longitude", "units": "degrees_east"}),
            "latitude": (["y", "x"], lat_2d, {"standard_name": "latitude", "units": "degrees_north"}),
            "lon": (["y", "x"], lon_2d, {"standard_name": "longitude", "units": "degrees_east"}),
            "lat": (["y", "x"], lat_2d, {"standard_name": "latitude", "units": "degrees_north"}),
        }
    )
    return dset


def points_to_dataset(longitude: t.Sequence[float] | np.ndarray, latitude: t.Sequence[float] | np.ndarray) -> xr.Dataset:
    """Create a dataset for a set of points (1D).

    Parameters
    ----------
    longitude : array-like
        Array of longitude values.
    latitude : array-like
        Array of latitude values.

    Returns
    -------
    xarray.Dataset
        A dataset with coordinates suitable for regridding.
    """
    from numpy import asarray

    lat = asarray(latitude)
    lon = asarray(longitude)
    if lat.ndim == 0:
        lat = lat[None]
    if lon.ndim == 0:
        lon = lon[None]

    # Ensure they are the same length
    if lat.shape != lon.shape:
        # If one is scalar and other is array, broadcast
        if lat.size == 1:
            lat = np.full_like(lon, lat[0])
        elif lon.size == 1:
            lon = np.full_like(lat, lon[0])
        else:
            raise ValueError("Latitude and longitude must have the same shape or one must be scalar.")

    s = lat.shape[0]
    dset = xr.Dataset(
        coords={
            "lon": (["x", "y"], lon.reshape(s, 1)),
            "lat": (["x", "y"], lat.reshape(s, 1)),
        }
    )
    return dset


def create_area_def_from_latlon(*args, **kwargs):
    """Deprecated: Part of removed pyresample dependency."""
    raise NotImplementedError("This function relies on pyresample which has been removed.")


def create_area_def_from_dataset(*args, **kwargs):
    """Deprecated: Part of removed pyresample dependency."""
    raise NotImplementedError("This function relies on pyresample which has been removed.")


def get_grid_area_def(*args, **kwargs):
    """Deprecated: Part of removed pyresample dependency."""
    raise NotImplementedError("This function relies on pyresample which has been removed.")


def create_area_def_from_esmf_mesh(*args, **kwargs):
    """Deprecated: Part of removed pyresample dependency."""
    raise NotImplementedError("This function relies on pyresample which has been removed.")


def create_area_def_from_ugrid(*args, **kwargs):
    """Deprecated: Part of removed pyresample dependency."""
    raise NotImplementedError("This function relies on pyresample which has been removed.")


# Backward compatibility aliases
def lonlat_to_xesmf(longitude=None, latitude=None):
    """Alias for lonlat_to_dataset."""
    return lonlat_to_dataset(longitude, latitude)


def constant_1d_xesmf(longitude=None, latitude=None):
    """Alias for points_to_dataset."""
    return points_to_dataset(longitude, latitude)
