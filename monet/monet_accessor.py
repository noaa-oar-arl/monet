"MONET Accessor"

from __future__ import absolute_import, division, print_function
from builtins import object
import pandas as pd
import xarray as xr
import stratify


@xr.register_dataarray_accessor('monet')
class MONETAccessor(object):
    """Short summary.

    Parameters
    ----------
    xray_obj : type
        Description of parameter `xray_obj`.

    Attributes
    ----------
    obj : type
        Description of attribute `obj`.

    """

    def __init__(self, xray_obj):
        self.obj = xray_obj

    def stratify(self, levels, vertical, axis=1):
        """Short summary.

        Parameters
        ----------
        levels : type
            Description of parameter `levels`.
        vertical : type
            Description of parameter `vertical`.
        axis : type
            Description of parameter `axis`.

        Returns
        -------
        type
            Description of returned object.

        """
        result = stratify.interpolate(
            levels, vertical.chunk(), self.obj.chunk(), axis=axis)
        dims = self.obj.dims
        out = xr.DataArray(result, dims=dims)
        for i in dims:
            if i != 'z':
                out[i] = self.obj[i]
        out.attrs = self.obj.attrs.copy()
        if len(self.obj.coords) > 0:
            for i in self.obj.coords:
                out.coords[i] = self.obj.coords[i]
        return out

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
        from .util.interp_util import constant_lat_swathdefition
        from .util.resample import resample_dataset
        from numpy import linspace
        try:
            if lat is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lat value')
        longitude = linspace(self.obj.longitude.min(),
                             self.obj.longitude.max(), len(self.obj.x))
        target = constant_lat_swathdefition(longitude=longitude, latitude=lat)
        output = resample_dataset(self.obj, target, **kwargs).squeeze()
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
        from .util.interp_util import constant_lon_swathdefition
        from .util.resample import resample_dataset
        from numpy import linspace
        try:
            if lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lon value')
        latitude = linspace(self.obj.latitude.min(), self.obj.latitude.max(),
                            len(self.obj.y))
        target = constant_lon_swathdefition(longitude=lon, latitude=latitude)
        output = resample_dataset(self.obj, target, **kwargs).squeeze()
        return output

    def nearest_latlon(self, lat=None, lon=None, **kwargs):
        """Short summary.

        Parameters
        ----------
        lat : type
            Description of parameter `lat`.
        lon : type
            Description of parameter `lon`.
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        from .util.interp_util import nearest_point_swathdefinition
        from .util.resample import resample_dataset
        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must provide latitude and longitude')
        target = nearest_point_swathdefinition(longitude=lon, latitude=lat)
        output = resample_dataset(self.obj, target, **kwargs)
        return output

    def cartopy(self):
        """Short summary.

        Returns
        -------
        type
                Returns a cartopy.crs.Projection for this dataset

        """

        return self.obj.area.to_cartopy_crs()

    def quick_map(self, map_kwarg={}, **kwargs):
        """Short summary.

        Parameters
        ----------
        map_kwarg : type
            Description of parameter `map_kwarg`.
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        from .plots.mapgen import draw_map
        from matplotlib.pyplot import tight_layout
        import cartopy.crs as ccrs

        if self._check_swath_def(self.obj.area):
            crs = ccrs.PlateCarree()
        else:
            crs = self.obj.monet.cartopy()
        ax = draw_map(crs=crs, **map_kwarg)
        self.obj.plot(
            x='longitude',
            y='latitude',
            ax=ax,
            transform=ccrs.PlateCarree(),
            **kwargs)
        ax.outline_patch.set_alpha(0)
        tight_layout()
        return ax

    # def _check_and_fix_coords(self):
    #     if not self.obj.coords:
    #         # get the lat lons from the swath or area def
    #         lon, lat = self.obj.area.get_lonlats()
    #         self.obj.coords['longitude'] = lon
    #         self.obj.coords['latitude'] = lat

    def _check_swath_def(self, defin):
        """checks if it is a pyresample SwathDefinition or AreaDefinition.

        Parameters
        ----------
        defin : type
            Description of parameter `defin`.

        Returns
        -------
        type
            Description of returned object.

        """
        from pyresample.geometry import SwathDefinition
        if isinstance(defin, SwathDefinition):
            return True
        else:
            return False

    def remap_data(self, dataarray, grid=None, **kwargs):
        """remaps from another grid to the current grid of self using pyresample.
        it assumes that the dimensions are ordered in ROW,COL,CHANNEL per
        pyresample docs

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
        from .util import resample
        # check to see if grid is supplied
        target = self.obj.area
        if grid is None:  # grid is assumed to be in da.area
            out = resample.resample_dataset(dataarray, target, **kwargs)
        else:
            dataarray.attrs['area'] = grid
            out = resample.resample_dataset(dataarray, target, **kwargs)
        return out

    def remap_xesmf(self, dataarray, method='bilinear', **kwargs):
        """remaps from another grid to the current grid of self using xesmf

        Parameters
        ----------
        daaarray : ndarray or xarray DataArray
            Description of parameter `dset`.
        radius_of_influence : float or integer
            radius of influcence for pyresample in meters.

        Returns
        -------
        xarray.DataArray
            resampled object on current grid.

        """
        from .util import resample
        # check to see if grid is supplied
        target = self.obj
        out = resample.resample_xesmf(
            dataarray, target, method=method, **kwargs)
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
        from .models.combinetool import combine_da_to_df
        # point source data
        if isinstance(data, pd.DataFrame):
            try:
                if col is None:
                    raise RuntimeError
                return combine_da_to_df(
                    self.obj,
                    data,
                    col=col,
                    radius_of_influence=radius_of_influence)
            except RuntimeError:
                print('Must enter col ')
        elif isinstance(data, xr.Dataset) or isinstance(data, xr.DataArray):
            print('do spatial transform')
        else:
            print('d must be either a pd.DataFrame or xr.DataArray')


@xr.register_dataset_accessor('monet')
class MONETAccessorDataset(object):
    """Short summary.

    Parameters
    ----------
    xray_obj : type
        Description of parameter `xray_obj`.

    Attributes
    ----------
    obj : type
        Description of attribute `obj`.

    """

    def __init__(self, xray_obj):
        self.obj = xray_obj

    def remap_data(self, data, grid=None, **kwargs):
        """Short summary.

        Parameters
        ----------
        data : type
            Description of parameter `data`.
        grid : type
            Description of parameter `grid`.
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        try:
            if isinstance(data, xr.DataArray):
                self._remap_dataarray(data, grid=grid, **kwargs)
            elif isinstance(data, xr.Dataset):
                self._remap_dataset(data, grid=None, **kwargs)
            else:
                raise TypeError
        except TypeError:
            print('data must be an xarray.DataArray or xarray.Dataset')

    def remap_xesmf(self, data, **kwargs):
        """Short summary.

        Parameters
        ----------
        data : type
            Description of parameter `data`.
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        print(data)
        # try:
        if isinstance(data, xr.DataArray):
            self._remap_xesmf_dataarray(data, **kwargs)
        elif isinstance(data, xr.Dataset):
            self._remap_xesmf_dataset(data, **kwargs)
        else:
            raise TypeError
        # except TypeError:
        #     print('data must be an xarray.DataArray or xarray.Dataset')

    def _remap_xesmf_dataset(self,
                             dset,
                             filename='monet_xesmf_regrid_file.nc',
                             **kwargs):

        skip_keys = ['latitude', 'longitude', 'time', 'TFLAG']
        vars = pd.Series(dset.variables)
        loop_vars = vars.loc[~vars.isin(skip_keys)]
        dataarray = dset[loop_vars[0]]
        da = self._remap_xesmf_dataarray(
            dataarray, self.obj, filename=filename, **kwargs)
        if da.name in self.obj.variables:
            da.name = da.name + '_y'
        self.obj[da.name] = da
        for i in loop_vars[1:]:
            dataarray = dset[i]
            self._remap_xesmf_dataarray(
                dataarray, filename=filename, reuse_weights=True, **kwargs)

    def _remap_xesmf_dataarray(self,
                               dataarray,
                               method='bilinear',
                               filename='monet_xesmf_regrid_file.nc',
                               **kwargs):
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
        from .util import resample
        target = self.obj
        out = resample.resample_xesmf(
            dataarray, target, method=method, filename=filename, **kwargs)
        print(out)
        if out.name in self.obj.variables:
            out.name = out.name + '_y'
        self.obj[out.name] = out
        return out

    def _remap_dataset(self, dset, grid=None, **kwargs):
        """Resample the entire dset (xarray.Dataset) to the current dataset object.

        Parameters
        ----------
        dset : xarray.Dataset
            Description of parameter `dataarray`.

        Returns
        -------
        type
            Description of returned object.

        """
        # from .util import resample
        # target = self.obj.area
        skip_keys = ['latitude', 'longitude', 'time', 'TFLAG']
        vars = pd.Series(dset.variables)
        loop_vars = vars.loc[~vars.isin(skip_keys)]
        # get the first one in the loop and get the resample_cache data
        dataarray = dset[loop_vars[0]]

        da, resample_cache = self._remap_dataarray(
            dataarray, grid=grid, return_neighbor_info=True, **kwargs)
        if da.name in self.obj.variables:
            da.name = da.name + '_y'
        self.obj[da.name] = da
        for i in loop_vars[1:]:
            dataarray = dset[i]
            da, resample_cache = self._remap_dataarray(
                dataarray, grid=grid, resample_cache=resample_cache, **kwargs)
            if da.name in self.obj.variables:
                da.name = da.name + '_y'
            self.obj[da.name] = da

    def _remap_dataarray(self, dataarray, grid=None, **kwargs):
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
        from .util import resample
        if grid is None:  # grid is assumed to be in da.area
            out = resample.resample_dataset(dataarray.chunk(), self.obj,
                                            **kwargs)

        else:
            dataarray.attrs['area'] = grid
            out = resample.resample_dataset(dataarray.chunk(), self.obj,
                                            **kwargs)
        return out

    def nearest_latlon(self, lat=None, lon=None, **kwargs):
        vars = pd.Series(self.obj.variables)
        skip_keys = ['latitude', 'longitude', 'time', 'TFLAG']
        loop_vars = vars.loc[~vars.isin(skip_keys)]
        orig = self.obj[loop_vars.iloc[0]].monet.nearest_latlon(
            lat=lat, lon=lon, **kwargs)
        dset = orig.to_dataset()
        dset.attrs = self.obj.attrs.copy()
        for i in loop_vars[1:].values:
            dset[i] = self.obj[i].monet.nearest_latlon(
                lat=lat, lon=lon, **kwargs)
        return dset
        """Short summary.

        Parameters
        ----------
        lat : type
            Description of parameter `lat`.
        lon : type
            Description of parameter `lon`.
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """

    def interp_constant_lat(self, lat=None, **kwargs):
        """Short summary.

        Parameters
        ----------
        lat : type
            Description of parameter `lat`.
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        vars = pd.Series(self.obj.variables)
        skip_keys = ['latitude', 'longitude', 'time', 'TFLAG']
        loop_vars = vars.loc[~vars.isin(skip_keys)]

        orig = self.obj[loop_vars.iloc[0]].monet.interp_constant_lat(
            lat=lat, **kwargs)

        dset = orig.to_dataset()
        dset.attrs = self.obj.attrs.copy()
        for i in loop_vars[1:].values:
            dset[i] = self.obj[i].interp_constant_lat(lat=lat, **kwargs)
        return dset

    def interp_constant_lon(self, lon=None, **kwargs):
        """Short summary.

        Parameters
        ----------
        lon : type
            Description of parameter `lon`.
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        vars = pd.Series(self.obj.variables)
        skip_keys = ['latitude', 'longitude', 'time', 'TFLAG']
        loop_vars = vars.loc[~vars.isin(skip_keys)]
        orig = self.obj[loop_vars[0]].interp_constant_lon(lon=lon, **kwargs)
        dset = orig.to_dataset()
        dset.attrs = self.obj.attrs.copy()
        for i in loop_vars[1:].values:
            dset[i] = self.obj[i].interp_constant_lon(lon=lon, **kwargs)
        return dset

    def stratify(self, levels, vertical, axis=1):
        """Short summary.

        Parameters
        ----------
        levels : type
            Description of parameter `levels`.
        vertical : type
            Description of parameter `vertical`.
        axis : type
            Description of parameter `axis`.

        Returns
        -------
        type
            Description of returned object.

        """
        loop_vars = [i for i in self.obj.variables if 'z' in self.obj[i].dims]
        orig = self.obj[loop_vars[0]].stratify(levels, vertical, axis=axis)
        dset = orig.to_dataset()
        dset.attrs = self.obj.attrs.copy()
        for i in loop_vars[1:]:
            dset[i] = self.obj[i].stratify(levels, vertical, axis=axis)
        return dset

    def cartopy(self):
        """Returns a cartopy.crs.Projection for this dataset."""
        return self.obj.area.to_cartopy_crs()

    def combine_to_df(self, df, mapping_table=None, radius_of_influence=None):
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
        from .models.combinetool import combine_da_to_df
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
