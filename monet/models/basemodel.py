from __future__ import absolute_import, division, print_function

# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1
from builtins import object, zip
from gc import collect

import pandas as pd
import xarray as xr
import numpy as np
from dask.diagnostics import ProgressBar
try:
    from osgeo import osr
    has_gdal = True
except ImportError:
    has_gdal = False


def check_crs(crs):
    """Checks if the crs represents a valid grid, projection or ESPG string.

    Examples
    --------
    >>> p = check_crs('+units=m +init=epsg:26915')
    >>> p.srs
    '+units=m +init=epsg:26915 '
    >>> p = check_crs('wrong')
    >>> p is None
    True

    Returns
    -------
    A valid crs if possible, otherwise None
    """
    import pyproj
    try:
        out = pyproj.Proj(crs)
    except RuntimeError:
        try:
            out = pyproj.Proj(init=crs)
        except RuntimeError:
            out = None

    return out


def proj_to_cartopy(proj):
    """Converts a pyproj.Proj to a cartopy.crs.Projection

    Parameters
    ----------
    proj: pyproj.Proj
        the projection to convert

    Returns
    -------
    a cartopy.crs.Projection object

    """

    import cartopy
    import cartopy.crs as ccrs

    proj = check_crs(proj)

    if proj.is_latlong():
        return ccrs.PlateCarree()

    srs = proj.srs
    if has_gdal:
        # this is more robust, as srs could be anything (espg, etc.)
        from osgeo import osr
        s1 = osr.SpatialReference()
        s1.ImportFromProj4(proj.srs)
        srs = s1.ExportToProj4()

    km_proj = {
        'lon_0': 'central_longitude',
        'lat_0': 'central_latitude',
        'x_0': 'false_easting',
        'y_0': 'false_northing',
        'lat_ts': 'latitude_true_scale',
        'k': 'scale_factor',
        'zone': 'zone',
    }
    km_globe = {
        'a': 'semimajor_axis',
        'b': 'semiminor_axis',
    }
    km_std = {
        'lat_1': 'lat_1',
        'lat_2': 'lat_2',
    }
    kw_proj = dict()
    kw_globe = dict()
    kw_std = dict()
    for s in srs.split('+'):
        s = s.split('=')
        if len(s) != 2:
            continue
        k = s[0].strip()
        v = s[1].strip()
        try:
            v = float(v)
        except:
            pass
        if k == 'proj':
            if v == 'tmerc':
                cl = ccrs.TransverseMercator
            if v == 'lcc':
                cl = ccrs.LambertConformal
            if v == 'merc':
                cl = ccrs.Mercator
            if v == 'utm':
                cl = ccrs.UTM
            if v == 'stere':
                cl = ccrs.Stereographic
        if k in km_proj:
            kw_proj[km_proj[k]] = v
        if k in km_globe:
            kw_globe[km_globe[k]] = v
        if k in km_std:
            kw_std[km_std[k]] = v

    globe = None
    if kw_globe:
        globe = ccrs.Globe(ellipse='sphere', **kw_globe)
    if kw_std:
        kw_proj['standard_parallels'] = (kw_std['lat_1'], kw_std['lat_2'])

    # mercatoooor
    if cl.__name__ == 'Mercator':
        kw_proj.pop('false_easting', None)
        kw_proj.pop('false_northing', None)
        if LooseVersion(cartopy.__version__) < LooseVersion('0.15'):
            kw_proj.pop('latitude_true_scale', None)
    elif cl.__name__ == 'Stereographic':
        kw_proj.pop('scale_factor', None)
        if 'latitude_true_scale' in kw_proj:
            kw_proj['true_scale_latitude'] = kw_proj['latitude_true_scale']
            kw_proj.pop('latitude_true_scale', None)
    else:
        kw_proj.pop('latitude_true_scale', None)

    return cl(globe=globe, **kw_proj)


def _ioapi_grid_from_dataset(ds):
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
    proj_id = ds.GDTYP
    atol = 1e-4
    if proj_id == 2:
        # Lambert
        p4 = '+proj=lcc +lat_1={lat_1} +lat_2={lat_2} ' \
             '+lat_0={lat_0} +lon_0={lon_0} ' \
             '+x_0={x0} +y_0={y0} +a=6370000 +b=6370000'
        p4 = p4.format(**pargs)
    elif proj_id == 4:
        # Polar stereo
        p4 = '+proj=stere +lat_ts={lat_1} +lon_0={lon_0} +lat_0=90.0' \
             '+x_0={x0} +y_0={y0} +a=6370000 +b=6370000'
        p4 = p4.format(**pargs)
        # pyproj and WRF do not agree well close to the pole
        atol = 5e-3
    elif proj_id == 3:
        # Mercator
        p4 = '+proj=merc +lat_ts={lat_1} ' \
             '+lon_0={center_lon} ' \
             '+x_0={x0} +y_0={y0} +a=6370000 +b=6370000'
        p4 = p4.format(**pargs)
    else:
        raise NotImplementedError('IOAPI proj not implemented yet: '
                                  '{}'.format(proj_id))

    return p4


def grid_from_dataset(ds):
    """Find out if the dataset contains enough info for Salem to understand.

    ``ds`` can be an xarray dataset

    Returns a :py:string:`proj4_string` if successful, ``None`` otherwise
    """
    # maybe its an IOAPI file
    if hasattr(ds, 'IOAPI_VERSION') or hasattr(ds, 'P_ALP'):
        # IOAPI_VERSION
        return _ioapi_grid_from_dataset(ds)

    # Try out platte carree
    return _lonlat_grid_from_dataset(ds)


@xr.register_dataset_accessor('monet')
class BaseModel(object):
    def __init__(self, xray_obj):
        self.obj = xray_obj

        def interpz(self,
                    zcoord,
                    levels,
                    dim_name='',
                    fill_value=np.NaN,
                    use_multiprocessing=True):
            """Interpolates the array along the vertical dimension

            Parameters
            ----------
            zcoord: DataArray
              the z coordinates of the variable. Must be of same dimensions
            levels: 1dArray
              the levels at which to interpolate
            dim_name: str
              the name of the new dimension
            fill_value : np.NaN or 'extrapolate', optional
              how to handle levels below the topography. Default is to mark them
              as invalid, but you might want the have them extrapolated.
            use_multiprocessing: bool
              set to false if, for some reason, you don't want to use mp

            Returns
            -------
            a new DataArray with the interpolated data
            """
            print('TODO')

    def cartopy(self):
        """Returns a cartopy.crs.Projection for this dataset."""
        return proj_to_cartopy(self.obj.proj4_srs)

    def combine(d):
        #point source data
        if isinstance(d, pd.DataFrame):
            print('do pd.DataFrame thing here')
        elif isinstance(d, xr.Dataset) or isinstance(d, xr.DataArray):
            print('do spatial transform')
        else:
            print('d must be either a pd.DataFrame or xr.DataArray')


def get_times(dset):
    if Series(dset.attrs.keys()).isin(['IOAPI_VERSION']).max():
        from ioapitools import get_times
        return get_times(dset)


def open_ioapi_dataset(fname):
    from pandas import Series
    import ioapitools
    dset = xr.open_dataset(fname)
    grid = grid_from_dataset(dset)

    dset = dset.assign_attrs({'proj4_srs': grid})
    for i in dset.variables:
        dset[i] = dset[i].assign_attrs({'proj4_srs': grid})
        for j in dset[i].attrs:
            dset[i].attrs[j] = dset[i].attrs[j].strip()
    lazy_vars = Series(dir(ioapitools)).str.contains('add_lazy')
    for m in Series(dir(ioapitools)).loc[lazy_vars]:
        print(m)
        meth = getattr(ioapitools, m)
        dset = meth(dset)
    return dset
