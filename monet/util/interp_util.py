""" Interpolation functions """


def latlon_xarray_to_CoordinateDefinition(longitude=None, latitude=None):
    """Create pyresample SwathDefinition from xarray object.

    Parameters
    ----------
    longitude : 2d xarray.DataArray
        Longitude -> must be from -180 -> 180 and monotonically increasing
    latitude : 2d xarray.DataArray
        Latitude -> must be from -90 -> 90 and monotonically increasing

    Returns
    -------
    pyresample.CoordinateDefinition

    """
    from pyresample import geometry

    return geometry.CoordinateDefinition(lats=latitude, lons=longitude)


def lonlat_to_xesmf(longitude=None, latitude=None):
    """Creates an empty xarray.Dataset with the coordinate (longitude, latitude).

    Parameters
    ----------
    longitude : type
        Description of parameter `longitude`.
    latitude : type
        Description of parameter `latitude`.

    Returns
    -------
    type
        Description of returned object.

    """
    import xarray as xr
    from numpy import asarray

    lat = asarray(latitude)
    lon = asarray(longitude)
    dset = xr.Dataset(coords={"lon": (["x", "y"], lon.reshape(1, 1)), "lat": (["x", "y"], lat.reshape(1, 1))})
    return dset


def lonlat_to_swathdefinition(longitude=None, latitude=None):
    """Short summary.

    Parameters
    ----------
    longitude : type
        Description of parameter `longitude`.
    latitude : type
        Description of parameter `latitude`.

    Returns
    -------
    type
        Description of returned object.

    """
    from pyresample.geometry import SwathDefinition
    from numpy import vstack

    if len(longitude.shape) < 2:
        lons = vstack(longitude)
        lats = vstack(latitude)
    else:
        lons = longitude
        lats = latitude
    return SwathDefinition(lons=lons, lats=lats)


def nearest_point_swathdefinition(longitude=None, latitude=None):
    """Creates a pyreample.geometry.SwathDefinition for a single point.

    Parameters
    ----------
    longitude : float
        longitude.
    latitude : float
        latitude.

    Returns
    -------
    pyreample.geometry.SwathDefinition


    """
    from pyresample.geometry import SwathDefinition
    from numpy import vstack

    lons = vstack([longitude])
    lats = vstack([latitude])
    return SwathDefinition(lons=lons, lats=lats)


def constant_1d_xesmf(longitude=None, latitude=None):
    """Creates a pyreample.geometry.SwathDefinition with a constant latitude along
    the longitude array.  Longitude can be a 1d or 2d np.array or xr.DataArray

    Parameters
    ----------
    longitude : numpy.array or xarray.DataArray
        Array of longitude values
    latitude : float
        latitude for constant

    Returns
    -------
    pyreample.geometry.SwathDefinition

    """
    import xarray as xr
    from numpy import asarray

    lat = asarray(latitude)
    lon = asarray(longitude)
    s = lat.shape[0]
    dset = xr.Dataset(coords={"lon": (["x", "y"], lon.reshape(s, 1)), "lat": (["x", "y"], lat.reshape(s, 1))})
    return dset


def constant_lat_swathdefition(longitude=None, latitude=None):
    """Creates a pyreample.geometry.SwathDefinition with a constant latitude along
    the longitude array.  Longitude can be a 1d or 2d np.array or xr.DataArray

    Parameters
    ----------
    longitude : numpy.array or xarray.DataArray
        Array of longitude values
    latitude : float
        latitude for constant

    Returns
    -------
    pyreample.geometry.SwathDefinition

    """
    from pyresample import geometry
    from xarray import DataArray
    from numpy import vstack

    if len(longitude.shape) < 2:
        lons = vstack(longitude)
    else:
        lons = longitude
    lats = lons * 0.0 + latitude
    if isinstance(lats, DataArray):
        lats.name = "lats"
    return geometry.SwathDefinition(lons=lons, lats=lats)


def constant_lon_swathdefition(longitude=None, latitude=None):
    """Creates a pyreample.geometry.SwathDefinition with a constant longitude along
    the latitude array.  latitude can be a 1d or 2d np.array or xr.DataArray

    Parameters
    ----------
    longitude :
        latitude for constant
    latitude : numpy.array or xarray.DataArray
        Array of longitude values

    Returns
    -------
    pyreample.geometry.SwathDefinition

    """
    from pyresample import geometry
    from xarray import DataArray
    from numpy import vstack

    if len(latitude.shape) < 2:
        lats = vstack(latitude)
    else:
        lats = latitude
    lons = lats * 0.0 + longitude
    if isinstance(lats, DataArray):
        lons.name = "lons"
    return geometry.SwathDefinition(lons=lons, lats=lats)
