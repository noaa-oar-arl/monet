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
from monet.models.combinetool import *


def get_ioapi_pyresample_area_def(ds, proj4_srs):
    from pyresample import geometry, utils
    y_size = ds.NROWS
    x_size = ds.NCOLS
    projection = utils.proj4_str_to_dict(proj4_srs)
    proj_id = 'dataset'
    description = 'IOAPI area_def for pyresample'
    area_id = 'Unknown'
    x_ll, y_ll = x_ll, y_ll = ds.XORIG, ds.YORIG
    x_ur, y_ur = ds.XORIG + (ds.NCOLS * ds.XCELL) + ds.XCELL, ds.YORIG + (
        ds.YCELL * ds.NROWS) + ds.YCELL
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

    def interp_constant_lat(self, lat=None, **kwargs):
        """Interpolates the data array to constant latitude.

        Parameters
        ----------
        lat : float
            Latitude on which to interpolate to

        Returns
        -------
        DataArray
            DataArray of at constant latitude

        """
        from ..utils.interpolation import to_constant_latitude
        try:
            if lat is None:
                raise RuntimeError
            return to_constant_latitude(self.obj, lat=lat, **kwargs)
        except RuntimeError:
            print('Must enter lat value')

    def interp_constant_lon(self, lon=None, **kwargs):
        """Interpolates the data array to constant longitude.

            Parameters
            ----------
            lon : float
                Latitude on which to interpolate to

            Returns
            -------
            DataArray
                DataArray of at constant longitude

            """
        from ..utils.interpolation import to_constant_latitude
        try:
            if lat is None:
                raise RuntimeError
            return to_constant_latitude(self.obj, lat=lat, **kwargs)
        except RuntimeError:
            print('Must enter lat value')

    def nearest_latlon(self, lat=None, lon=None, **kwargs):
        from ..utils.interpolation import find_nearest_latlon_xarray
        try:
            if lat is None or lon is None:
                raise RuntimeError

            d = find_nearest_latlon_xarray(
                self.obj, lat=lat, lon=lon, radius=radius)
            return d
        except RuntimeError:
            print('Must provide latitude and longitude')

    def cartopy(self):
        """Returns a cartopy.crs.Projection for this dataset."""
        return self.obj.area_def.to_cartopy_crs()

    def remap_nearest(grid, da, radius_of_influence=12e3):
        """remaps from another grid to the current grid of self using pyresample.
        it assumes that the dimensions are ordered in ROW,COL,CHANNEL per pyresample docs

        Parameters
        ----------
        grid : pyresample grid
            Description of parameter `grid`.
        da : ndarray or xarray DataArray
            Description of parameter `dset`.
        radius_of_influence : float or integer
            radius of influcence for pyresample in meters.

        Returns
        -------
        xarray.DataArray
            resampled object on current grid.

        """
        from pyresample import image
        #resample the data
        image_con = image.ImageContainerNearest(da,grid,radius_of_influence=radius_of_influence)
        result = image_con.resample(self.obj.area_def).image_data
        #if da is xarray retain attributes and assign new area_def and proj4_srs
        # from current dataset
        if isinstance(da,xr.DataArray):
            xr.dims = 




    def combine(self, data, col=None, radius=None):
        """Short summary.

        Parameters
        ----------
        data : type
            Description of parameter `data`.
        col : type
            Description of parameter `col`.
        radius : type
            Description of parameter `radius`.

        Returns
        -------
        type
            Description of returned object.

        """

        #point source data
        if isinstance(data, pd.DataFrame):
            try:
                if col is None:
                    raise RuntimeError
                return combine_da_to_df(self.obj, data, col=col, radius=radius)
            except RuntimeError:
                print('Must enter col ')
        elif isinstance(data, xr.Dataset) or isinstance(data, xr.DataArray):
            print('do spatial transform')
        else:
            print('d must be either a pd.DataFrame or xr.DataArray')


@xr.register_dataset_accessor('monet')
class BaseModel(object):
    def __init__(self, xray_obj):
        self.obj = xray_obj

    def cartopy(self):
        """Returns a cartopy.crs.Projection for this dataset."""
        return proj_to_cartopy(self.obj.proj4_srs)

    def combine(d, mapping_table=None, radius=None):
        #point source data
        if isinstance(d, pd.DataFrame):
            print('do pd.DataFrame thing here')
        elif isinstance(d, xr.Dataset) or isinstance(d, xr.DataArray):
            print('do spatial transform')
        else:
            print('d must be either a pd.DataFrame or xr.DataArray')
