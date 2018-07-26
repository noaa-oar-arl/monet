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
from .combinetool import combine_da_to_df


def get_ioapi_pyresample_area_def(ds, proj4_srs):
    from pyresample import geometry, utils
    y_size = ds.NROWS
    x_size = ds.NCOLS
    projection = utils.proj4_str_to_dict(proj4_srs)
    proj_id = 'dataset'
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
        from ..util.interp_util import constant_lat_swathdefition
        from ..util.resample import resample_dataset
        from numpy import linspace
        try:
            if lat is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lat value')
        longitude = linspace(self.obj.longitude.min(),
                             self.obj.longitude.max(), len(self.obj.x))
        target = constant_lat_swathdefition(longitude=longitude, latitude=lat)
        output = resample_dataset(self.obj, target).squeeze()
        return output

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
        from ..util.interp_util import constant_lon_swathdefition
        from ..util.resample import resample_dataset
        from numpy import linspace
        try:
            if lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lon value')
        latitude = linspace(self.obj.latitude.min(), self.obj.latitude.max(),
                            len(self.obj.y))
        target = constant_lon_swathdefition(longitude=lon, latitude=latitude)
        output = resample_dataset(self.obj, target).squeeze()
        return output

    def nearest_latlon(self, lat=None, lon=None, **kwargs):
        from ..util.interp_util import nearest_point_swathdefinition
        from ..util.resample import resample_dataset
        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must provide latitude and longitude')
        target = nearest_point_swathdefinition(longitude=lon, latitude=lat)
        output = resample_dataset(self.obj, target).squeeze()
        return output

    def cartopy(self):
        """Returns a cartopy.crs.Projection for this dataset."""
        return self.obj.area.to_cartopy_crs()

    def quick_map(
            self,
            map_kwarg={},
            **kwargs,
    ):
        from ..plots.mapgen import draw_map
        from matplotlib.pyplot import subplots, tight_layout
        #import cartopy.crs as ccrs
        #crs = self.obj.monet.cartopy()
        ax = draw_map(**map_kwarg)
        self.obj.plot(x='longitude', y='latitude', ax=ax, **kwargs)
        ax.outline_patch.set_alpha(0)
        tight_layout()
        return ax

    def remap_nearest(self, dataarray, grid=None, **kwargs):
        """remaps from another grid to the current grid of self using pyresample.
        it assumes that the dimensions are ordered in ROW,COL,CHANNEL per pyresample docs

        Parameters
        ----------
        grid : pyresample grid (SwathDefinition or AreaDefinition)
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
        from ..util import resample
        # check to see if grid is supplied
        target = self.obj.area
        if grid is None:  #grid is assumed to be in da.area
            source = dataarray
            out = resample.resample_dataset(dataarray.chunk(), target,
                                            **kwargs)
        else:
            dataarray.attrs['area'] = grid
            out = resample.resample_dataset(dataarray.chunk(), target,
                                            **kwargs)
        return out

    def combine(self, data, col=None, radius_of_influence=None):
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
        from .combinetool import combine_da_to_df
        #point source data
        if isinstance(data, pd.DataFrame):
            try:
                if col is None:
                    raise RuntimeError
                return combine_da_to_df(
                    self.obj, data, col=col, radius_of_influence=radius)
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

    def remap_nearest(self, data, grid=None, **kwargs):
        try:
            if isinstance(data, xr.DataArray):
                self._remap_dataarray_nearest(data, grid=grid, **kwargs)
            elif instance(data, xr.Dataset):
                self._remap_dataset_nearest(data, grid=None, **kwargs)
            else:
                raise TypeError
        except TypeError:
            print('data must be an xarray.DataArray or xarray.Dataset')

    def _remap_dataset_nearest(self, dset, grid=None, **kwargs):
        """Resample the entire xarray.Dataset to the current dataset object.

        Parameters
        ----------
        dset : xarray.Dataset
            Description of parameter `dataarray`.

        Returns
        -------
        type
            Description of returned object.

        """
        from ..util import resample
        target = self.obj.area
        skip_keys = ['latitude', 'longitude', 'time', 'TFLAG']
        vars = pd.Series(dset.variables)
        loop_vars = vars.loc[~vars.isin(skip_keys)]
        #get the first one in the loop and get the resample_cache data
        dataarray = dset[loop_vars[0]]

        da, resample_cache = self._remap_dataarray_nearest(
            dataarray, grid=grid, return_neighbor_info=True, **kwargs)
        if da.name in self.obj.variables:
            da.name = da.name + '_y'
        self.obj[da.name] = da
        for i in loop_vars[1:]:
            dataarray = dset[i]
            da, resample_cache = self._remap_dataarray_nearest(
                dataarray, grid=grid, resample_cache=resample_cache, **kwargs)
            if da.name in self.obj.variables:
                da.name = da.name + '_y'
            self.obj[da.name] = da

    def _remap_dataarray_nearest(self, dataarray, grid=None, **kwargs):
        """Resample the DataArray to the dataset object.

        Parameters
        ----------
        dataarray : type
            Description of parameter `dataarray`.

        Returns
        -------
        type
            Description of returned object.

        """
        from ..util import resample
        target = self.obj.area
        if grid is None:  #grid is assumed to be in da.area
            out = resample.resample_dataset(dataarray.chunk(), target,
                                            **kwargs)
        else:
            dataarray.attrs['area'] = grid
            out = resample.resample_dataset(dataarray.chunk(), target,
                                            **kwargs)
        if out.name in dset.variables:
            out.name = out.name + '_y'
        dset[out.name] = out

    def cartopy(self):
        """Returns a cartopy.crs.Projection for this dataset."""
        return proj_to_cartopy(self.obj.proj4_srs)

    def combine_to_df(df, mapping_table=None, radius_of_influence=None):
        """Short summary.

        Parameters
        ----------
        df : type
            Description of parameter `df`.
        mapping_table : type
            Description of parameter `mapping_table`.
        radius : type
            Description of parameter `radius`.

        Returns
        -------
        type
            Description of returned object.

        """
        from .combinetool import combine_da_to_df
        try:
            if ~isinstance(df, pd.DataFrame):
                raise TypeError
        except TypeError:
            print('df must be of type pd.DataFrame')
        for i in mapping_table:
            df = combine_da_to_df(
                self.obj[mapping_table[i]],
                df,
                col=i,
                radius=radius_of_influence)
        return df
