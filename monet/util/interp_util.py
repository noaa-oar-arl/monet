"""Interpolation utility functions for MONET"""

import typing as t

import numpy as np
import xarray as xr


def lonlat_to_dataset(
    longitude: t.Sequence[float] | np.ndarray | xr.DataArray, latitude: t.Sequence[float] | np.ndarray | xr.DataArray
) -> xr.Dataset:
    """Create a Dataset with longitude and latitude coordinates.
    Supports both 1D coordinates (creates a meshgrid) and 2D arrays.

    This implementation is backend-agnostic and preserves Dask laziness
    by using xarray broadcasting instead of numpy meshgrid for xarray inputs.

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
    if isinstance(longitude, xr.DataArray) and isinstance(latitude, xr.DataArray):
        # Use xarray broadcasting to preserve Dask laziness
        # We want (y, x) ordering to match numpy meshgrid default
        # First ensure they have the expected dimension names if they are 1D
        if longitude.ndim == 1 and latitude.ndim == 1:
            if longitude.dims[0] != "x":
                longitude = longitude.rename({longitude.dims[0]: "x"})
            if latitude.dims[0] != "y":
                latitude = latitude.rename({latitude.dims[0]: "y"})

        lon_2d, lat_2d = xr.broadcast(longitude, latitude)

        # Force transpose to (y, x) to match expectations
        if "y" in lon_2d.dims and "x" in lon_2d.dims:
            lon_2d = lon_2d.transpose("y", "x")
            lat_2d = lat_2d.transpose("y", "x")
    else:
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

    dims = ["y", "x"]
    if hasattr(lon_2d, "dims") and len(lon_2d.dims) == 2:
        dims = lon_2d.dims

    dset = xr.Dataset(
        coords={
            "longitude": (
                dims,
                lon_2d.data if hasattr(lon_2d, "data") else lon_2d,
                {"standard_name": "longitude", "units": "degrees_east"},
            ),
            "latitude": (
                dims,
                lat_2d.data if hasattr(lat_2d, "data") else lat_2d,
                {"standard_name": "latitude", "units": "degrees_north"},
            ),
            "lon": (
                dims,
                lon_2d.data if hasattr(lon_2d, "data") else lon_2d,
                {"standard_name": "longitude", "units": "degrees_east"},
            ),
            "lat": (
                dims,
                lat_2d.data if hasattr(lat_2d, "data") else lat_2d,
                {"standard_name": "latitude", "units": "degrees_north"},
            ),
        }
    )
    return dset


def points_to_dataset(
    longitude: t.Sequence[float] | np.ndarray | xr.DataArray, latitude: t.Sequence[float] | np.ndarray | xr.DataArray
) -> xr.Dataset:
    """Create a dataset for a set of points (1D).

    This implementation is backend-agnostic and preserves Dask laziness.

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
    if isinstance(longitude, xr.DataArray) and isinstance(latitude, xr.DataArray):
        # Ensure they are aligned if possible, or assume they are point pairs
        lon = longitude
        lat = latitude
    else:
        from numpy import asanyarray

        lat = asanyarray(latitude)
        lon = asanyarray(longitude)
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

    # Reshape to (N, 1) for regridding compatibility
    # If xarray, we use expand_dims to keep it lazy
    if isinstance(lon, xr.DataArray):
        lon_out = lon.expand_dims("y", axis=-1)
        lat_out = lat.expand_dims("y", axis=-1)
        dims = lon_out.dims
    else:
        s = lat.shape[0]
        lon_out = lon.reshape(s, 1)
        lat_out = lat.reshape(s, 1)
        dims = ["x", "y"]

    dset = xr.Dataset(
        coords={
            "lon": (
                dims,
                lon_out.data if hasattr(lon_out, "data") else lon_out,
                {"standard_name": "longitude", "units": "degrees_east"},
            ),
            "lat": (
                dims,
                lat_out.data if hasattr(lat_out, "data") else lat_out,
                {"standard_name": "latitude", "units": "degrees_north"},
            ),
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
