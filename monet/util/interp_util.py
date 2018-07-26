from __future__ import print_function

from builtins import str, zip
try:
    from functools import reduce
except ImportError:
    pass


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
    from pyreample.geometry import SwathDefinition
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
    lats = lons * 0. + latitude
    if isinstance(lats, DataArray):
        lats.name = 'lats'
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
    lons = lats * 0. + longitude
    if isinstance(lats, DataArray):
        lons.name = 'lons'
    return geometry.SwathDefinition(lons=lons, lats=lats)


def get_smops_area_def(nx=1440, ny=720):
    """Short summary.

    Parameters
    ----------
    nx : type
        Description of parameter `nx` (the default is 1440).
    ny : type
        Description of parameter `ny` (the default is 720).

    Returns
    -------
    type
        Description of returned object.

    """
    p = Proj(
        proj='eqc',
        lat_ts=0.,
        lat_0=0.,
        lon_0=0.,
        x_0=0.,
        y_0=0.,
        a=6378137,
        b=6378137,
        units='m')
    proj4_args = p.srs
    area_name = 'Global .25 degree SMOPS Grid'
    area_id = 'smops'
    proj_id = area_id
    aa = p([-180, 180], [-90, 90])
    area_extent = (aa[0][0], aa[1][0], aa[0][1], aa[1][1])
    area_def = utils.get_area_def(area_id, area_name, proj_id, proj4_args, nx,
                                  ny, area_extent)
    return area_def


def get_gfs_area_def(nx=1440, ny=721):
    """Short summary.

    Parameters
    ----------
    nx : type
        Description of parameter `nx` (the default is 1440).
    ny : type
        Description of parameter `ny` (the default is 721).

    Returns
    -------
    type
        Description of returned object.

    """
    # proj4_args = '+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m'
    p = Proj(
        proj='eqc',
        lat_ts=0.,
        lat_0=0.,
        lon_0=0.,
        x_0=0.,
        y_0=0.,
        a=6378137,
        b=6378137,
        units='m')
    proj4_args = p.srs
    area_name = 'Global .25 degree SMOPS Grid'
    area_id = 'smops'
    proj_id = area_id
    aa = p([0, 360 - .25], [-90, 90.])
    area_extent = (aa[0][0], aa[1][0], aa[0][1], aa[1][1])
    area_def = utils.get_area_def(area_id, area_name, proj_id, proj4_args, nx,
                                  ny, area_extent)
    return area_def


def geotiff_meta_to_areadef(meta):
    """
    Transform (Rasterio) geotiff meta dictionary to pyresample area definition
    Arguments:
     meta (dictionary) : dictionary containing projection and image geometry
                         information (formed by Rasterio)
    Returns:
         area_def (pyresample.geometry.AreaDefinition) : Area definition object
    """
    import pyresample
    area_id = ""
    name = ""
    proj_id = "Generated from GeoTIFF"
    proj_dict = meta['crs']
    proj_dict_with_string_values = dict(
        list(
            zip([str(key) for key in list(proj_dict.keys())],
                [str(value) for value in list(proj_dict.values())])))
    x_size = meta['width']
    x_res = meta['transform'][0]
    y_res = meta['transform'][4] * -1
    y_size = meta['height']
    x_ll = meta['transform'][2]
    y_ur = meta['transform'][5]
    y_ll = y_ur - y_size * y_res
    x_ur = x_ll + x_size * x_res
    area_extent = [x_ll, y_ll, x_ur, y_ur]
    print(area_extent, x_size, y_size, x_res, y_res)

    area_def = pyresample.geometry.AreaDefinition(
        area_id, name, proj_id, proj_dict_with_string_values, x_size, y_size,
        area_extent)
    print(area_extent, xsize, ysize)
    return area_def


def geotiff_meta_to_areadef2(meta):
    """
    Transform (Rasterio) geotiff meta dictionary to pyresample area definition
    Arguments:
     meta (dictionary) : dictionary containing projection and image geometry
                         information (formed by Rasterio)
    Returns:
         area_def (pyresample.geometry.AreaDefinition) : Area definition object
    """
    import pyresample
    area_id = ""
    name = ""
    proj_id = "Generated from GeoTIFF"
    proj_dict = meta['crs']
    proj_dict_with_string_values = dict(
        list(
            zip([str(key) for key in list(proj_dict.keys())],
                [str(value) for value in list(proj_dict.values())])))
    x_size = meta['width']
    x_res = 50000.
    y_res = 50000.
    y_size = meta['height']
    x_ll = meta['transform'][2]
    y_ur = meta['transform'][5]
    y_ll = y_ur - y_size * y_res
    x_ur = x_ll + x_size * x_res
    area_extent = [x_ll, y_ll, x_ur, y_ur]
    print(area_extent, x_size, y_size, x_res, y_res)

    area_def = pyresample.geometry.AreaDefinition(
        area_id, name, proj_id, proj_dict_with_string_values, x_size, y_size,
        area_extent)
    return area_def
    """Short summary.

    Parameters
    ----------
    meta : type
        Description of parameter `meta`.

    Returns
    -------
    type
        Description of returned object.

    """
