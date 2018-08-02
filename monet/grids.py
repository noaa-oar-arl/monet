""" This is a module that will derive the proj4 string and pyesample.geometry.AreaDefinition
    for any gridded dataset (satellite, models, etc....)

    """
import os
path = os.path.abspath(__file__)


def _get_sinu_grid_df():
    from pandas import read_csv, DataFrame
    import inspect
    """This function finds the modis grid tiles found within
        the defined grid.
        input:
        lon = gridded longitude - numpy array
        lat = gridded latitude - numpy array
        output:
        pandas dataframe of tiles
        """
    f = path[:-8] + 'data/sn_bound_10deg.txt'
    td = read_csv(f, skiprows=4, delim_whitespace=True)
    td = td.assign(ihiv='h' + td.ih.astype(str).str.zfill(2) + 'v' +
                   td.iv.astype(str).str.zfill(2))
    return td


def _sinu_grid_latlon_boundary(h, v):
    td = _get_sinu_grid_df()
    o = td.loc[(td.ih == int(h)) & (td.iv == int(v))]
    latmin = o.lat_min.iloc[0]
    lonmin = o.lon_min.iloc[0]
    latmax = o.lat_max.iloc[0]
    lonmax = o.lon_max.iloc[0]
    return lonmin, latmin, lonmax, latmax


def _get_sinu_xy(lon, lat):
    from pyproj import Proj
    sinu = Proj(
        '+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m'
    )
    return sinu(lon, lat)


def _get_sinu_latlon(x, y):
    from numpy import meshgrid
    from pyproj import Proj
    xv, yv = meshgrid(x, y)
    sinu = Proj(
        '+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m, +R=6371007.181'
    )
    return sinu(xv, yv, inverse=True)


def get_modis_latlon_from_swath_hv(h, v, dset):
    from numpy import linspace
    lonmin, latmin, lonmax, latmax = _sinu_grid_latlon_boundary(h, v)
    xmin, ymin = _get_sinu_xy(lonmin, latmin)
    xmax, ymax = _get_sinu_xy(lonmax, latmax)
    x = linspace(xmin, xmax, len(dset.x))
    y = linspace(ymin, ymax, len(dset.y))
    lon, lat = _get_sinu_latlon(x, y)
    dset.coords['longitude'] = (('x', 'y'), lon)
    dset.coords['latitude'] = (('x', 'y'), lat)
    dset.attrs['area_extent'] = (x.min(), y.min(), x.max(), y.max())
    dset.attrs[
        'proj4_srs'] = '+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m'
    return dset


def get_sinu_area_def(dset):
    from pyresample import utils
    from pyproj import Proj
    p = Proj(dset.attrs['proj4_srs'])
    proj4_args = p.srs
    area_name = 'MODIS Grid Def'
    area_id = 'modis'
    proj_id = area_id
    area_extent = dset.attrs['area_extent']
    nx, ny = dset.longitude.shape
    return utils.get_area_def(area_id, area_name, proj_id, proj4_args, nx, ny,
                              area_extent)


def get_ioapi_pyresample_area_def(ds, proj4_srs):
    from pyresample import geometry, utils
    y_size = ds.NROWS
    x_size = ds.NCOLS
    projection = utils.proj4_str_to_dict(proj4_srs)
    proj_id = 'IOAPI_Dataset'
    description = 'IOAPI area_def for pyresample'
    area_id = 'MONET_Object_Grid'
    x_ll, y_ll = ds.XORIG + ds.XCELL * .5, ds.YORIG + ds.YCELL * .5
    x_ur, y_ur = ds.XORIG + (ds.NCOLS * ds.XCELL) + .5 * ds.XCELL, ds.YORIG + (
        ds.YCELL * ds.NROWS) + .5 * ds.YCELL
    area_extent = (x_ll, y_ll, x_ur, y_ur)
    area_def = geometry.AreaDefinition(area_id, description, proj_id,
                                       projection, x_size, y_size, area_extent)
    return area_def


def _ioapi_grid_from_dataset(ds, earth_radius=6370000):
    """Get the IOAPI projection out of the file into proj4."""

    pargs = dict()
    # Normal WRF file
    cen_lon = ds.XCENT
    cen_lat = ds.YCENT
    dx = ds.XCELL
    dy = ds.YCELL
    pargs['lat_1'] = ds.P_ALP
    pargs['lat_2'] = ds.P_BET
    pargs['lat_0'] = ds.YCENT
    pargs['lon_0'] = ds.P_GAM
    pargs['center_lon'] = ds.XCENT
    pargs['x0'] = ds.XORIG
    pargs['y0'] = ds.YORIG
    pargs['r'] = earth_radius
    proj_id = ds.GDTYP
    atol = 1e-4
    if proj_id == 2:
        # Lambert
        p4 = '+proj=lcc +lat_1={lat_1} +lat_2={lat_2} ' \
             '+lat_0={lat_0} +lon_0={lon_0} ' \
             '+x_0=0 +y_0=0 +datum=WGS84 +units=m +a={r} +b={r}'
        p4 = p4.format(**pargs)
    elif proj_id == 4:
        # Polar stereo
        p4 = '+proj=stere +lat_ts={lat_1} +lon_0={lon_0} +lat_0=90.0' \
             '+x_0=0 +y_0=0 +a={r} +b={r}'
        p4 = p4.format(**pargs)
        # pyproj and WRF do not agree well close to the pole
        atol = 5e-3
    elif proj_id == 3:
        # Mercator
        p4 = '+proj=merc +lat_ts={lat_1} ' \
             '+lon_0={center_lon} ' \
             '+x_0={x0} +y_0={y0} +a={r} +b={r}'
        p4 = p4.format(**pargs)
    else:
        raise NotImplementedError('IOAPI proj not implemented yet: '
                                  '{}'.format(proj_id))
    #area_def = _get_ioapi_pyresample_area_def(ds)
    return p4  #, area_def


def grid_from_dataset(ds, earth_radius=6370000):
    """Find out if the dataset contains enough info for Salem to understand.

    ``ds`` can be an xarray dataset

    Returns a :py:string:`proj4_string` if successful, ``None`` otherwise
    """
    # maybe its an IOAPI file
    if hasattr(ds, 'IOAPI_VERSION') or hasattr(ds, 'P_ALP'):
        # IOAPI_VERSION
        return _ioapi_grid_from_dataset(ds, earth_radius=earth_radius)

    # Try out platte carree
    return _lonlat_grid_from_dataset(ds)
