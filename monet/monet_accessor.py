"MONET Accessor"

import pandas as pd
import xarray as xr


def rename_latlon(ds):
    if 'latitude' in ds.coords:
        return ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif 'Latitude' in ds.coords:
        return ds.rename({'Latitude': 'lat', 'Longitude': 'lon'})
    elif 'Lat' in ds.coords:
        return ds.rename({'Lat': 'lat', 'Lon': 'lon'})
    else:
        return ds


def _rename_to_monet_latlon(ds):
    if 'lat' in ds.coords:
        return ds.rename({'lat': 'latitude', 'lon': 'longitude'})
    elif 'Latitude' in ds.coords:
        return ds.rename({'Latitude': 'latitude', 'Longitude': 'longitude'})
    elif 'Lat' in ds.coords:
        return ds.rename({'Lat': 'latitude', 'Lon': 'longitude'})
    else:
        return ds


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
        from .util.resample import resample_stratify
        out = resample_stratify(self.obj, levels, vertical, axis=1)
        return out

    def window(self,
               lat_min=None,
               lon_min=None,
               lat_max=None,
               lon_max=None,
               rectilinear=False):
        """Function to window, ie select a specific region, given the lower left
        latitude and longitude and the upper right latitude and longitude

        Parameters
        ----------
        lat_min : float
            lower left latitude .
        lon_min : float
            lower left longitude.
        lat_max : float
            upper right latitude.
        lon_max : float
            upper right longitude.
        rectilinear : bool
            flag if this is a rectilinear lat lon grid

        Returns
        -------
        xr.DataArray
            returns the windowed object.

        """
        try:
            from pyresample import utils
            from .util.interp_util import nearest_point_swathdefinition as npsd
            from .util.interp_util import lonlat_to_swathdefinition as llsd
            from numpy import concatenate
            has_pyresample = True
        except ImportError:
            has_pyresample = False
        try:
            if rectilinear:
                lat = dset.latitude.isel(x=0).values
                lon = dset.longitude.isel(y=0).values
                dset['x'] = lon
                dset['y'] = lat
                dset = dset.drop(['latitude', 'longitude'])
                # check if latitude is in the correct order
                if dset.latitude.isel(x=0).values[0] > dset.latitude.isel(
                        x=0).values[-1]:
                    lat_min_copy = lat_min
                    lat_min = lat_max
                    lat_max = lat_min_copy
                d = dset.sel(
                    x=slice(lon_min, lon_max), y=slice(lat_min, lat_max))
                return monet.coards_to_netcdf(
                    d.rename({
                        'x': 'lon',
                        'y': 'lat'
                    }))
            elif has_pyresample:
                lons, lats = utils.check_and_wrap(self.obj.longitude.values,
                                                  self.obj.latitude.values)
                swath = llsd(longitude=lons, latitude=lats)
                pswath_ll = npsd(
                    longitude=float(lon_min), latitude=float(lat_min))
                pswath_ur = npsd(
                    longitude=float(lon_max), latitude=float(lat_max))
                row, col = utils.generate_nearest_neighbour_linesample_arrays(
                    swath, pswath_ll, 1e6)
                y_ll, x_ll = row[0][0], col[0][0]
                row, col = utils.generate_nearest_neighbour_linesample_arrays(
                    swath, pswath_ur, 1e6)
                y_ur, x_ur = row[0][0], col[0][0]
                if x_ur < x_ll:
                    x1 = self.obj.x.where(self.obj.x >= x_ll, drop=True).values
                    x2 = self.obj.x.where(self.obj.x <= x_ur, drop=True).values
                    xrange = concatenate([x1, x2]).astype(int)
                    self.obj['longitude'][:] = utils.wrap_longitudes(
                        self.obj.longitude.values)
                    # xrange = arange(float(x_ur), float(x_ll), dtype=int)
                else:
                    xrange = slice(x_ll, x_ur)
                if y_ur < y_ll:
                    y1 = self.obj.y.where(self.obj.y >= y_ll, drop=True).values
                    y2 = self.obj.y.where(self.obj.y <= y_ur, drop=True).values
                    yrange = concatenate([y1, y2]).astype(int)
                else:
                    yrange = slice(y_ll, y_ur)
                return self.obj.isel(x=xrange, y=yrange)
            else:
                raise ImportError
        except ImportError:
            print(
                """If this is a rectilinear grid and you don't have pyresample
                  please add the rectilinear=True to the call.  Otherwise the window
                  functionality is unavailable without pyresample""")

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
        from .util.interp_util import constant_1d_xesmf
        from .util.resample import resample_xesmf
        from numpy import linspace, ones, asarray
        try:
            if lat is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lat value')
        longitude = linspace(self.obj.longitude.min(),
                             self.obj.longitude.max(), len(self.obj.y))
        latitude = ones(longitude.shape) * asarray(lat)
        self.obj = rename_latlon(self.obj)

        output = constant_1d_xesmf(latitude=latitude, longitude=longitude)
        out = resample_xesmf(self.obj, output, **kwargs)
        return rename_latlon(out)

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
        from .util.interp_util import constant_1d_xesmf
        from .util.resample import resample_xesmf
        from numpy import linspace, ones
        try:
            if lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lon value')
        latitude = linspace(self.obj.latitude.min(), self.obj.latitude.max(),
                            len(self.obj.y))
        longitude = ones(latitude.shape) * lon
        self.obj = rename_latlon(self.obj)

        output = constant_1d_xesmf(latitude=latitude, longitude=longitude)

        out = resample_xesmf(self.obj, output, **kwargs)
        return rename_latlon(out)

    def nearest_latlon(self,
                       lat=None,
                       lon=None,
                       cleanup=True,
                       esmf=False,
                       **kwargs):
        """Uses xesmf to intepolate to a given latitude and longitude.  Note
        that the conservative method is not available.

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
        try:
            from pyresample import geometry, utils
            from .util.resample import resample_dataset
            from .util.interp_util import nearest_point_swathdefinition as npsd
            from .util.interp_util import lonlat_to_swathdefinition as llsd
            has_pyresample = True
        except ImportError:
            has_pyresample = False
        if esmf:
            has_pyresample = False

        from .util.interp_util import lonlat_to_xesmf
        from .util.resample import resample_xesmf
        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must provide latitude and longitude')

        if has_pyresample:
            lons, lats = utils.check_and_wrap(self.obj.longitude.values,
                                              self.obj.latitude.values)
            swath = llsd(longitude=lons, latitude=lats)
            pswath = npsd(longitude=float(lon), latitude=float(lat))
            row, col = utils.generate_nearest_neighbour_linesample_arrays(
                swath, pswath, float(1e6))
            y, x = row[0][0], col[0][0]
            return self.obj.sel(x=x, y=y)
        else:
            kwargs = self._check_kwargs_and_set_defaults(**kwargs)
            self.obj = rename_latlon(self.obj)
            target = lonlat_to_xesmf(longitude=lon, latitude=lat)
            output = resample_xesmf(self.obj, target, **kwargs)
            if cleanup:
                output = resample_xesmf(
                    self.obj, target, cleanup=True, **kwargs)
            return rename_latlon(output.squeeze())

    @staticmethod
    def _check_kwargs_and_set_defaults(**kwargs):
        if 'reuse_weights' not in kwargs:
            kwargs['reuse_weights'] = False
        if 'method' not in kwargs:
            kwargs['method'] = 'bilinear'
        if 'periodic' not in kwargs:
            kwargs['periodic'] = False
        if 'filename' not in kwargs:
            kwargs['filename'] = 'monet_xesmf_regrid_file.nc'
        return kwargs

    def quick_map(self, map_kwarg={}, center=True, **kwargs):
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
        from .plots import _dynamic_fig_size
        import cartopy.crs as ccrs
        import seaborn as sns
        sns.set_context('notebook', font_scale=1.2)
        if 'crs' not in map_kwarg:
            if ~center:
                central_longitude = float(
                    _rename_to_monet_latlon(self.obj).longitude.mean().values)
                map_kwarg['crs'] = ccrs.PlateCarree(
                    central_longitude=central_longitude)
            else:
                map_kwarg['crs'] = ccrs.PlateCarree()
        if 'figsize' in kwargs:
            map_kwarg['figsize'] = kwargs['figsize']
            kwargs.pop('figsize', None)
        else:
            figsize = _dynamic_fig_size(self.obj)
            map_kwarg['figsize'] = figsize
        f, ax = draw_map(return_fig=True, **map_kwarg)
        ax = _rename_to_monet_latlon(self.obj).plot(
            x='longitude',
            y='latitude',
            ax=ax,
            transform=ccrs.PlateCarree(),
            infer_intervals=True,
            **kwargs)
        ax.outline_patch.set_alpha(0)
        self._tight_layout()
        return ax

    def _tight_layout(self):
        from matplotlib.pyplot import subplots_adjust
        subplots_adjust(0, 0, 1, 1)

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
        """remaps from another grid to the current grid of self using
        pyresample. It assumes that the dimensions are ordered in ROW,COL,CHANNEL per
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
        from pyresample import utils
        from .util import resample
        from .util.interp_util import lonlat_to_swathdefinition as llsd
        # from .grids import get_generic_projection_from_proj4
        # check to see if grid is supplied
        dataarray = _rename_to_monet_latlon(dataarray)
        lons_t, lats_t = utils.check_and_wrap(self.obj.longitude.values,
                                              self.obj.latitude.values)
        self.obj = _rename_to_monet_latlon(self.obj)
        lons_s, lats_s = utils.check_and_wrap(dataarray.longitude.values,
                                              dataarray.latitude.values)
        target = llsd(longitude=lons_t, latitude=lats_t)
        source = llsd(latitude=lats_s, longitude=lons_s)
        # target = get_generic_projection_from_proj4(
        #     self.obj.latitude, self.object.longitude, self.obj.proj4_srs)
        # source_grid = get_generic_projection_from_proj4(
        #     dataarray.latitude, dataarray.longitude, dataarray.proj4_srs)
        if grid is None:  # grid is assumed to be in da.area
            out = resample.resample_dataset(dataarray, source, target,
                                            **kwargs)
        else:
            out = resample.resample_dataset(dataarray, grid, target, **kwargs)
        return out

    def remap_nearest(self, dataarray, radius_of_influence=1e6):
        """remaps from another grid to the current grid of self using pyresample.
            it assumes that the dimensions are ordered in y,x,z per
        pyresample docs

        Parameters
        ----------
        da : ndarray or xarray DataArray
            Description of parameter `dset`.
        radius_of_influence : float or integer
            radius of influcence for pyresample in meters.

        Returns
        -------
        xarray.DataArray
            resampled object on current grid.

        """
        from pyresample import utils
        from .util import resample
        # from .grids import get_generic_projection_from_proj4
        # check to see if grid is supplied
        dataarray = _rename_to_monet_latlon(dataarray)
        lons_t, lats_t = utils.check_and_wrap(self.obj.longitude.values,
                                              self.obj.latitude.values)
        self.obj = _rename_to_monet_latlon(self.obj)
        return resample.resample_nearest_neighbor_pyresample_dask(
            dataarray, self.obj, radius_of_influence=radius_of_influence)

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
        target = rename_latlon(self.obj)
        source = rename_latlon(dataarray)
        out = resample.resample_xesmf(source, target, method=method, **kwargs)
        return _rename_to_monet_latlon(out)

    def combine_point(self,
                      data,
                      col=None,
                      suffix=None,
                      pyresample=False,
                      **kwargs):
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
        from .util.combinetool import combine_da_to_df_xesmf
        # point source data
        if isinstance(data, pd.DataFrame):
            try:
                if col is None:
                    raise RuntimeError
                if pyresample:
                    return combine_da_to_df()
                return combine_da_to_df_xesmf(
                    self.obj, data, col=col, suffix=suffix, **kwargs)
            except RuntimeError:
                print('Must enter col...')
        else:
            print('d must be either a pd.DataFrame')


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
        try:
            if isinstance(data, xr.DataArray):
                data = rename_latlon(data)
                self._remap_xesmf_dataarray(data, **kwargs)
            elif isinstance(data, xr.Dataset):
                data = rename_latlon(data)
                self._remap_xesmf_dataset(data, **kwargs)
            else:
                raise TypeError
        except TypeError:
            print('data must be an xarray.DataArray or xarray.Dataset')

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
        self.obj[da.name] = da
        das = {}
        das[da.name] = da
        for i in loop_vars[1:]:
            dataarray = dset[i]
            tmp = self._remap_xesmf_dataarray(
                dataarray, filename=filename, reuse_weights=True, **kwargs)
            das[tmp.name] = tmp.copy()
        return xr.Dataset(das)

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
        if out.name in self.obj.variables:
            out.name = out.name + '_y'
        self.obj[out.name] = out
        return out

    def remap_nearest(self, data, radius_of_influence=1e6):
        try:
            if isinstance(data, xr.DataArray):
                data = rename_latlon(data)
                self._remap_nearest_dataarray(
                    data, radius_of_influence=radius_of_influence)
            elif isinstance(data, xr.Dataset):
                data = rename_latlon(data)
                self._remap_nearest_dataset(
                    data, radius_of_influence=radius_of_influence)
            else:
                raise TypeError
        except TypeError:
            print('data must be an xarray.DataArray or xarray.Dataset')

    def _remap_nearest_dataset(self, dset, radius_of_influence=1e6):
        skip_keys = ['latitude', 'longitude', 'time', 'TFLAG', 'z']
        vars = pd.Series(dset.variables)
        loop_vars = vars.loc[~vars.isin(skip_keys)]
        dataarray = dset[loop_vars[0]]
        da = self._remap_nearest_dataarray(
            dataarray, self.obj, radius_of_influence=radius_of_influence)
        self.obj[da.name] = da
        das = {}
        das[da.name] = da
        for i in loop_vars[1:]:
            dataarray = dset[i]
            tmp = self._remap_nearest_dataarray(
                dataarray, radius_of_influence=radius_of_influence)
            das[tmp.name] = tmp.copy()
        return xr.Dataset(das)

    def _remap_nearest_dataarray(self, dataarray, radius_of_influence=1e6):
        """remaps from another grid to the current grid of self using pyresample.
            it assumes that the dimensions are ordered in y,x,z per
        pyresample docs

        Parameters
        ----------
        da : ndarray or xarray DataArray
            Description of parameter `dset`.
        radius_of_influence : float or integer
            radius of influcence for pyresample in meters.

        Returns
        -------
        xarray.DataArray
            resampled object on current grid.

        """
        from pyresample import utils
        from .util import resample
        # from .grids import get_generic_projection_from_proj4
        # check to see if grid is supplied
        dataarray = _rename_to_monet_latlon(dataarray)
        lons_t, lats_t = utils.check_and_wrap(self.obj.longitude.values,
                                              self.obj.latitude.values)
        self.obj = _rename_to_monet_latlon(self.obj)
        return resample.resample_nearest_neighbor_pyresample_dask(
            dataarray, self.obj, radius_of_influence=radius_of_influence)

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
        try:
            from pyresample import utils
            from .util.interp_util import nearest_point_swathdefinition as npsd
            from .util.interp_util import lonlat_to_swathdefinition as llsd
            has_pyresample = True
        except ImportError:
            has_pyresample = False
        if has_pyresample:
            lons, lats = utils.check_and_wrap(self.obj.longitude.values,
                                              self.obj.latitude.values)
            swath = llsd(longitude=lons, latitude=lats)
            pswath = npsd(longitude=float(lon), latitude=float(lat))
            row, col = utils.generate_nearest_neighbour_linesample_arrays(
                swath, pswath, float(1e6))
            y, x = row[0][0], col[0][0]
            return self.obj.isel(x=x, y=y)
        else:
            vars = pd.Series(self.obj.variables)
            skip_keys = ['latitude', 'longitude', 'time', 'TFLAG']
            loop_vars = vars.loc[~vars.isin(skip_keys)]
            kwargs = self._check_kwargs_and_set_defaults(**kwargs)
            kwargs['reuse_weights'] = True
            orig = self.obj[loop_vars.iloc[0]].monet.nearest_latlon(
                lat=lat, lon=lon, cleanup=False, **kwargs)
            dset = orig.to_dataset()
            dset.attrs = self.obj.attrs.copy()
            for i in loop_vars[1:-1].values:
                dset[i] = self.obj[i].monet.nearest_latlon(
                    lat=lat, lon=lon, cleanup=False, **kwargs)
            i = loop_vars.values[-1]
            dset[i] = self.obj[i].monet.nearest_latlon(
                lat=lat, lon=lon, cleanup=True, **kwargs)
            return dset

    @staticmethod
    def _check_kwargs_and_set_defaults(**kwargs):
        if 'reuse_weights' not in kwargs:
            kwargs['reuse_weights'] = False
        if 'method' not in kwargs:
            kwargs['method'] = 'bilinear'
        if 'periodic' not in kwargs:
            kwargs['periodic'] = False
        if 'filename' not in kwargs:
            kwargs['filename'] = 'monet_xesmf_regrid_file.nc'
        return kwargs

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
        kwargs = self._check_kwargs_and_set_defaults(**kwargs)
        kwargs['reuse_weights'] = True
        orig = self.obj[loop_vars.iloc[0]].monet.interp_constant_lat(
            lat=lat, cleanup=False, **kwargs)

        dset = orig.to_dataset()
        dset.attrs = self.obj.attrs.copy()
        for i in loop_vars[1:-1].values:
            dset[i] = self.obj[i].monet.interp_constant_lat(lat=lat, **kwargs)
        i = loop_vars.values[-1]
        dset[i] = self.obj[i].monet.interp_constant_lat(
            lat=lat, cleanup=True, **kwargs)
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
        kwargs = self._check_kwargs_and_set_defaults(**kwargs)
        kwargs['reuse_weights'] = True
        orig = self.obj[loop_vars[0]].monet.interp_constant_lon(
            lon=lon, **kwargs)
        dset = orig.to_dataset()
        dset.attrs = self.obj.attrs.copy()
        for i in loop_vars[1:-1].values:
            dset[i] = self.obj[i].monet.interp_constant_lon(lon=lon, **kwargs)
        i = loop_vars.values[-1]
        dset[i] = self.obj[i].monet.interp_constant_lon(
            lon=lon, cleanup=True, **kwargs)
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

    def window(self, lat_min, lon_min, lat_max, lon_max):
        """Function to window, ie select a specific region, given the lower left
        latitude and longitude and the upper right latitude and longitude

        Parameters
        ----------
        lat_min : float
            lower left latitude .
        lon_min : float
            lower left longitude.
        lat_max : float
            upper right latitude.
        lon_max : float
            upper right longitude.

        Returns
        -------
        xr.DataSet
            returns the windowed object.

        """
        try:
            from pyresample import utils
            from .util.interp_util import nearest_point_swathdefinition as npsd
            from .util.interp_util import lonlat_to_swathdefinition as llsd
            from numpy import concatenate
            has_pyresample = True
        except ImportError:
            has_pyresample = False
        try:
            if has_pyresample:
                lons, lats = utils.check_and_wrap(self.obj.longitude.values,
                                                  self.obj.latitude.values)
                swath = llsd(longitude=lons, latitude=lats)
                pswath_ll = npsd(
                    longitude=float(lon_min), latitude=float(lat_min))
                pswath_ur = npsd(
                    longitude=float(lon_max), latitude=float(lat_max))
                row, col = utils.generate_nearest_neighbour_linesample_arrays(
                    swath, pswath_ll, float(1e6))
                y_ll, x_ll = row[0][0], col[0][0]
                row, col = utils.generate_nearest_neighbour_linesample_arrays(
                    swath, pswath_ur, float(1e6))
                y_ur, x_ur = row[0][0], col[0][0]
                if x_ur < x_ll:
                    x1 = self.obj.x.where(self.obj.x >= x_ll, drop=True).values
                    x2 = self.obj.x.where(self.obj.x <= x_ur, drop=True).values
                    xrange = concatenate([x1, x2]).astype(int)
                    self.obj['longitude'][:] = utils.wrap_longitudes(
                        self.obj.longitude.values)
                    # xrange = arange(float(x_ur), float(x_ll), dtype=int)
                else:
                    xrange = slice(x_ll, x_ur)
                if y_ur < y_ll:
                    y1 = self.obj.y.where(self.obj.y >= y_ll, drop=True).values
                    y2 = self.obj.y.where(self.obj.y <= y_ur, drop=True).values
                    yrange = concatenate([y1, y2]).astype(int)
                else:
                    yrange = slice(y_ll, y_ur)
                return self.obj.sel(x=xrange, y=yrange)
            else:
                raise ImportError
        except ImportError:
            print('Window functionality is unavailable without pyresample')

    def combine_point(self, data, col=None, suffix=None, **kwargs):
        """Short summary.

        Parameters
        ----------
        data : type
            Description of parameter `data`.
        mapping_table : type
            Description of parameter `mapping_table`.
        radius : type
            Description of parameter `radius`.

        Returns
        -------
        type
            Description of returned object.

        """
        from .util.combinetool import combine_da_to_df_xesmf
        return combine_da_to_df_xesmf(self.obj, data, suffix=suffix, **kwargs)
