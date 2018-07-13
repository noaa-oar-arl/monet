from __future__ import absolute_import, division, print_function

# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1
from builtins import object, zip
from gc import collect

import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar
try:
    from osgeo import osr
    has_gdal = True
except ImportError:
    has_gdal = False

ProgressBar().register()

# TO DO - Need to decide what belongs in the BaseModel. This is just a first guess.

# class BaseModel(object):
#     def __init__(self):
#         self.dset = None  # CMAQ xarray dataset object
#         self.dates = None
#         self.keys = None
#         self.indexdates = None
#         self.latitude = None
#         self.longitude = None
#         self.map = None
#
#     def check_z(self, varname):
#         if pd.Series(self.dset[varname].dims).isin(['z']).max():
#             return True
#         else:
#             return False
#
#     def get_var(self, varname, mapping_dict=None):
#         if mapping_dict is not None:
#             return self.dset[varname]
#         else:
#             return self.dset[varname]


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
    proj_id = ds.GDTYP
    atol = 1e-4
    if proj_id == 2:
        # Lambert
        p4 = '+proj=lcc +lat_1={lat_1} +lat_2={lat_2} ' \
             '+lat_0={lat_0} +lon_0={lon_0} ' \
             '+x_0=0 +y_0=0 +a=6370000 +b=6370000'
        p4 = p4.format(**pargs)
    elif proj_id == 4:
        # Polar stereo
        p4 = '+proj=stere +lat_ts={lat_1} +lon_0={lon_0} +lat_0=90.0' \
             '+x_0=0 +y_0=0 +a=6370000 +b=6370000'
        p4 = p4.format(**pargs)
        # pyproj and WRF do not agree well close to the pole
        atol = 5e-3
    elif proj_id == 3:
        # Mercator
        p4 = '+proj=merc +lat_ts={lat_1} ' \
             '+lon_0={center_lon} ' \
             '+x_0=0 +y_0=0 +a=6370000 +b=6370000'
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


@xr.register_dataarray_accessor('monet')
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


def open_ioapi_dataset(fname):
    from pandas import Series
    import ioapitools
    dset = xr.open_mfdataset(fname)
    grid = grid_from_dataset(dset)
    for i in dset.variables:
        dset[i].assign_attrs({'proj4_srs': grid})
    lazy_vars = Series(dir(ioapitools)).str.contains('add_lazy')
    for m in Series(dir(ioapitools)).loc[lazy_vars]:
        print(m)
        meth = getattr(ioapitools, m)
        dset = meth(dset)
    return dset
