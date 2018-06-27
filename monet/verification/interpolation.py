from __future__ import print_function

from builtins import str, zip
try:
    from functools import reduce
except ImportError:
    pass


def interp_latlon(var, lat, lon, radius=12000.):
    """Short summary.

    Parameters
    ----------
    var : type
        Description of parameter `var`.
    lat : type
        Description of parameter `lat`.
    lon : type
        Description of parameter `lon`.
    radius : type
        Description of parameter `radius` (the default is 12000.).

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import NaN, vstack, dstack, meshgrid
    from pyresample import geometry, image
    if len(lat.shape) < 2:  # need to vstack latitudes
        lat = vstack(lat)
        lon = vstack(lon)
    if len(var.longitude.shape
           ) < 2:  # need to meshgrid variables to create grid
        varlons, varlats = meshgrid(var.longitude, var.latitude)
    else:
        varlons, varlats = var.longitude.values, var.latitude.values
    grid1 = geometry.SwathDefinition(lons=varlons, lats=varlats)
    grid2 = geometry.SwathDefinition(lons=lon, lats=lat)

    # useful flatten for tuples

    def flatten(lst):        return reduce(lambda l, i: l + flatten(i) if isinstance(i, (list, tuple)) else l + [i], lst, [])

    from pandas import Series
    if len(var.dims) > 3:  # this has 4 dimensions need to dstack
        varlays = []
        # temporary fix for hysplit
        if Series(var.dims).isin(['levels']).max():
            for i in var.levels:
                varlays.append(
                    var.sel(levels=i).squeeze().transpose(
                        'latitude', 'longitude', 'time').values)
            data = dstack(varlays)
            datashape = flatten((grid2.shape, var.levels.shape[0],
                                 var.time.shape[0]))
        else:
            for i in var.z:
                varlays.append(
                    var.isel(z=i).squeeze().transpose('y', 'x', 'time').values)
            data = dstack(varlays)
            datashape = flatten((grid2.shape, var.z.shape[0],
                                 var.time.shape[0]))
    elif len(var.dims) > 2:
        if Series(var.dims).isin(['y']).max():
            data = var.transpose('y', 'x', 'time').values
            datashape = flatten(
                (grid2.shape,
                 var.time.shape[0]))  # var.transpose('y', 'x', 'time').shape
        else:
            data = var.transpose('latitude', 'longitude', 'time').values
            datashape = flatten((grid2.shape, var.time.shape[0]))
    else:  # assume 2d space
        if Series(var.dims).isin(['y']).max():
            data = var.transpose('y', 'x').values
            datashape = grid2.shape  # var.transpose('y', 'x')grid2.shape
        else:
            data = var.transpose('latitude', 'longitude').values
            datashape = grid2.shape
    icon = image.ImageContainerNearest(
        data, grid1, radius_of_influence=radius, fill_value=NaN)
    # resample
    ii = icon.resample(grid2).image_data.reshape(datashape).squeeze()
    return ii


def find_nearest_latlon_xarray(arr, lat=37.102400, lon=-76.392900,
                               radius=12e3):
    """Short summary.

    Parameters
    ----------
    arr : type
        Description of parameter `arr`.
    lat : type
        Description of parameter `lat` (the default is 37.102400).
    lon : type
        Description of parameter `lon` (the default is -76.392900).
    radius : type
        Description of parameter `radius` (the default is 12e3).

    Returns
    -------
    type
        Description of returned object.

    """
    from pyresample import utils, geometry
    from numpy import array, vstack
    grid1 = geometry.SwathDefinition(lons=arr.longitude, lats=arr.latitude)
    grid2 = geometry.SwathDefinition(lons=vstack([lon]), lats=vstack([lat]))
    row, col = utils.generate_nearest_neighbour_linesample_arrays(
        grid1, grid2, radius)
    row = row.flatten()
    col = col.flatten()
    return arr.sel(x=col).sel(y=row).squeeze()


def to_constant_latitude(arr, lat=37.102400, radius=12e3):
    """Short summary.

    Parameters
    ----------
    arr : type
        Description of parameter `arr`.
    lat : type
        Description of parameter `lat` (the default is 37.102400).
    lon : type
        Description of parameter `lon` (the default is -76.392900).
    radius : type
        Description of parameter `radius` (the default is 12e3).

    Returns
    -------
    type
        Description of returned object.

    """
    from pyresample import utils, geometry
    from numpy import arange, vstack, ones

    lons = array()
    grid1 = geometry.SwathDefinition(lons=arr.longitude, lats=arr.latitude)
    grid2 = geometry.SwathDefinition(lons=vstack([lon]), lats=vstack([lat]))
    row, col = utils.generate_nearest_neighbour_linesample_arrays(
        grid1, grid2, radius)
    row = row.flatten()
    col = col.flatten()
    return arr.sel(x=col).sel(y=row).squeeze()


# def interp_to_press_xarray()


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


def get_grid_def(lon, lat):
    """Short summary.

    Parameters
    ----------
    lon : type
        Description of parameter `lon`.
    lat : type
        Description of parameter `lat`.

    Returns
    -------
    type
        Description of returned object.

    """
    return geometry.GridDefinition(lons=lon, lats=lat)
