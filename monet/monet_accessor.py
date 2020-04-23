"MONET Accessor"

import pandas as pd
import xarray as xr

try:
    import xesmf
    has_xesmf = True
except ImportError:
    has_xesmf = False
try:
    import pyresample as pr
    from pyresample.utils import wrap_longitudes
    has_pyresample = True
except ImportError:
    has_pyresample = False


def wrap_longitudes(lons):
    """Short summary.

    Parameters
    ----------
    lons : type
        Description of parameter `lons`.

    Returns
    -------
    type
        Description of returned object.

    """
    return (lons + 180) % 360 - 180


def _rename_latlon(ds):
    """Short summary.

    Parameters
    ----------
    ds : type
        Description of parameter `ds`.

    Returns
    -------
    type
        Description of returned object.

    """
    if 'latitude' in ds.coords:
        return ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif 'Latitude' in ds.coords:
        return ds.rename({'Latitude': 'lat', 'Longitude': 'lon'})
    elif 'Lat' in ds.coords:
        return ds.rename({'Lat': 'lat', 'Lon': 'lon'})
    else:
        return ds


def _monet_to_latlon(da):
    if isinstance(da, xr.DataArray):
        dset = da.to_dataset()
    dset['x'] = da.longitude[0, :].values
    dset['y'] = da.latitude[:, 0].values
    dset = dset.drop(['latitude', 'longitude'])
    dset = dset.set_coords(['x', 'y'])
    dset = dset.rename({'x': 'lon', 'y': 'lat'})
    if isinstance(da, xr.DataArray):
        return dset[da.name]
    else:
        return dset


def _dataset_to_monet(dset,
                      lat_name='latitude',
                      lon_name='longitude',
                      latlon2d=False):
    """Renames XArray DataArray or Dataset for use with monet functions

    Parameters
    ----------
    dset : xr.DataArray or xr.Dataset
        a given data obj to be renamed for monet.
    lat_name : str
        name of the latitude array.
    lon_name : str
        name of the longitude array.
    latlon2d : bool
        flag for if the latitude and longitude data is two dimensional.

    Returns
    -------
    type
        Description of returned object.

    """
    if 'grid_xt' in dset.dims:
        # GFS v16 file
        try:
            if isinstance(dset, xr.DataArray):
                dset = _dataarray_coards_to_netcdf(dset,
                                                   lat_name='grid_yt',
                                                   lon_name='grid_xt')
            elif isinstance(dset, xr.Dataset):
                dset = _dataarray_coards_to_netcdf(dset,
                                                   lat_name='grid_yt',
                                                   lon_name='grid_xt')
            else:
                raise ValueError
        except ValueError:
            print('dset must be an Xarray.DataArray or Xarray.Dataset')

    if 'south_north' in dset.dims:  # WRF WPS file
        dset = dset.rename(dict(south_north='y', west_east='x'))
        try:
            if isinstance(dset, xr.Dataset):
                if 'XLAT_M' in dset.data_vars:
                    dset['XLAT_M'] = dset.XLAT_M.squeeze()
                    dset['XLONG_M'] = dset.XLONG_M.squeeze()
                    dset = dset.set_coords(['XLAT_M', 'XLONG_M'])
                elif 'XLAT' in dset.data_vars:
                    dset['XLAT'] = dset.XLAT.squeeze()
                    dset['XLONG'] = dset.XLONG.squeeze()
                    dset = dset.set_coords(['XLAT', 'XLONG'])
            elif isinstance(dset, xr.DataArray):
                if 'XLAT_M' in dset.coords:
                    dset['XLAT_M'] = dset.XLAT_M.squeeze()
                    dset['XLONG_M'] = dset.XLONG_M.squeeze()
                elif 'XLAT' in dset.coords:
                    dset['XLAT_M'] = dset.XLAT_M.squeeze()
                    dset['XLONG_M'] = dset.XLONG_M.squeeze()
            else:
                raise ValueError
        except ValueError:
            print('dset must be an Xarray.DataArray or Xarray.Dataset')

    dset = _rename_to_monet_latlon(dset)
    latlon2d = True
    # print(len(dset[lat_name].shape))
    # print(dset)
    if len(dset[lat_name].shape) < 2:
        # print(dset[lat_name].shape)
        latlon2d = False
    if latlon2d is False:
        try:
            if isinstance(dset, xr.DataArray):
                dset = _dataarray_coards_to_netcdf(dset,
                                                   lat_name=lat_name,
                                                   lon_name=lon_name)
            elif isinstance(dset, xr.Dataset):
                dset = _coards_to_netcdf(dset,
                                         lat_name=lat_name,
                                         lon_name=lon_name)
            else:
                raise ValueError
        except ValueError:
            print('dset must be an Xarray.DataArray or Xarray.Dataset')
    else:
        dset = _rename_to_monet_latlon(dset)
    dset['longitude'] = wrap_longitudes(dset['longitude'])
    return dset


def _rename_to_monet_latlon(ds):
    """Short summary.

    Parameters
    ----------
    ds : type
        Description of parameter `ds`.

    Returns
    -------
    type
        Description of returned object.

    """
    if 'lat' in ds.coords:
        return ds.rename({'lat': 'latitude', 'lon': 'longitude'})
    elif 'Latitude' in ds.coords:
        return ds.rename({'Latitude': 'latitude', 'Longitude': 'longitude'})
    elif 'Lat' in ds.coords:
        return ds.rename({'Lat': 'latitude', 'Lon': 'longitude'})
    elif 'XLAT_M' in ds.coords:
        return ds.rename({'XLAT_M': 'latitude', 'XLONG_M': 'longitude'})
    elif 'XLAT' in ds.coords:
        return ds.rename({'XLAT': 'latitude', 'XLONG': 'longitude'})
    else:
        return ds


def _coards_to_netcdf(dset, lat_name='lat', lon_name='lon'):
    """Short summary.

    Parameters
    ----------
    dset : type
        Description of parameter `dset`.
    lat_name : type
        Description of parameter `lat_name`.
    lon_name : type
        Description of parameter `lon_name`.

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import meshgrid, arange
    lon = wrap_longitudes(dset[lon_name])
    lat = dset[lat_name]
    lons, lats = meshgrid(lon, lat)
    x = arange(len(lon))
    y = arange(len(lat))
    dset = dset.rename({lon_name: 'x', lat_name: 'y'})
    dset.coords['longitude'] = (('y', 'x'), lons)
    dset.coords['latitude'] = (('y', 'x'), lats)
    dset['x'] = x
    dset['y'] = y
    dset = dset.set_coords(['latitude', 'longitude'])
    return dset


def _dataarray_coards_to_netcdf(dset, lat_name='lat', lon_name='lon'):
    """Short summary.

    Parameters
    ----------
    dset : type
        Description of parameter `dset`.
    lat_name : type
        Description of parameter `lat_name`.
    lon_name : type
        Description of parameter `lon_name`.

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import meshgrid, arange
    lon = wrap_longitudes(dset[lon_name])
    lat = dset[lat_name]
    lons, lats = meshgrid(lon, lat)
    x = arange(len(lon))
    y = arange(len(lat))
    dset = dset.rename({lon_name: 'x', lat_name: 'y'})
    dset.coords['latitude'] = (('y', 'x'), lats)
    dset.coords['longitude'] = (('y', 'x'), lons)
    dset['x'] = x
    dset['y'] = y
    return dset


@pd.api.extensions.register_dataframe_accessor("monet")
class MONETAccessorPandas:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        """Short summary.

        Parameters
        ----------
        obj : type
            Description of parameter `obj`.

        Returns
        -------
        type
            Description of returned object.

        """
        # verify there is a column latitude and a column longitude
        if 'latitude' not in obj.columns or 'longitude' not in obj.columns:
            raise AttributeError("Must have 'latitude' and 'longitude'.")

    @property
    def center(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
        # return the geographic center point of this DataFrame
        lat = self._obj.latitude
        lon = self._obj.longitude
        return (float(lon.mean()), float(lat.mean()))

    def to_ascii2nc_df(self,
                       grib_code=126,
                       height_msl=0.,
                       column='aod_550nm',
                       message_type='ADPUPA',
                       pressure=1000.,
                       qc=None,
                       height_agl=None):
        df = self._obj
        df['ascii2nc_time'] = df.time.dt.strftime('%Y%m%d_%H%M%S')
        df['ascii2nc_gribcode'] = int(grib_code)
        if isinstance(height_msl, str):
            df['ascii2nc_elevation'] = df[height_msl]
        else:
            df['ascii2nc_elevation'] = height_msl
        df['ascii2nc_message'] = message_type
        if isinstance(pressure, str):
            df['ascii2nc_pressure'] = df[pressure]
        else:
            df['ascii2nc_pressure'] = pressure
        df['ascii2nc_value'] = df[column]
        if qc is None:
            df['ascii2nc_qc'] = '0'
            df.loc[df['ascii2nc_value'].isnull(), 'ascii2nc_qc'] = '1'
        else:
            df['ascii2nc_qc'] = '0'
        if height_agl is None:
            df['ascii2nc_height_agl'] = df['ascii2nc_elevation']
        elif isinstance(height_agl, str):
            df['ascii2nc_height_agl'] = df[height_agl]
        else:
            df['ascii2nc_height_agl'] = height_agl
        out = df[[
            'ascii2nc_message', 'siteid', 'ascii2nc_time', 'latitude',
            'longitude', 'ascii2nc_elevation', 'ascii2nc_gribcode',
            'ascii2nc_pressure', 'ascii2nc_height_agl', 'ascii2nc_qc',
            'ascii2nc_value'
        ]]
        out = out.rename(dict(ascii2nc_message='typ',
                              siteid='sid',
                              ascii2nc_time='vld',
                              latitude='lat',
                              longitude='lon',
                              ascii2nc_elevation='elv',
                              ascii2nc_gribcode='var',
                              ascii2nc_pressure='lvl',
                              ascii2nc_height_agl='lvl',
                              ascii2nc_qc='qc',
                              ascii2nc_value='obs'),
                         axis=1)
        out = out.astype(dict(typ=str, sid=str, vld=str, var=str, qc=str))
        return out

    def to_ascii2nc_list(self, **kwargs):
        df = self._obj
        out = self.to_ascii2nc_df(**kwargs)
        return out.values.tolist()

    def rename_for_monet(self, df=None):
        """Rename latitude and longitude columns in the DataFrame.

        Parameters
        ----------
        df : type
            Description of parameter `df`.

        Returns
        -------
        type
            Description of returned object.

        """
        if df is None:
            df = self._obj
        if 'lat' in df.columns:
            df = df.rename({'lat': 'latitude', 'lon': 'longitude'})
        elif 'Latitude' in df.columns:
            df = df.rename({'Latitude': 'latitude', 'Longitude': 'longitude'})
        elif 'Lat' in df.columns:
            df = df.rename({'Lat': 'latitude', 'Lon': 'longitude'})
        elif 'LAT' in df.columns:
            df = df.rename({'LAT': 'latitude', 'LON': 'longitude'})
        return df

    def get_sparse_SwathDefinition(self):
        """Creates a pyreample.geometry.SwathDefinition for a single point.

        Returns
        -------
        pyreample.geometry.SwathDefinition

        """
        df = self.rename_for_monet(self._obj)
        if has_pyresample:
            from .util.interp_util import nearest_point_swathdefinition as npsd
        return nspd(latitude=df.latitude.values, longitude=df.longitude.values)

    def _df_to_da(self, d=None):
        """Short summary.

        Parameters
        ----------
        d : type
            Description of parameter `d`.

        Returns
        -------
        type
            Description of returned object.

        """
        import xarray as xr
        index_name = 'index'
        if d is None:
            d = self._obj
        if d.index.name is not None:
            index_name = d.index.name
        ds = d.to_xarray().rename({index_name: 'x'}).expand_dims('y')
        if 'time' in ds.data_vars.keys():
            ds['time'] = ds.time.squeeze()  # it is only 1D
        if 'latitude' in ds.data_vars.keys():
            ds = ds.set_coords(['latitude', 'longitude'])
        return ds

    def remap_nearest(self,
                      df,
                      radius_of_influence=1e5,
                      combine=False,
                      lat_name=None,
                      lon_name=None):
        """Remap df to find nearest sites

        Parameters
        ----------
        df : pandas.DataFrame
            dataframe to be interpolated
        radius_of_influence : float
            kwarg for pyresample.kd_tree

        Returns
        -------
        pandas.dataframe
            Returns the interpolated dataframe

        """
        d1 = self.rename_for_monet(df)
        d2 = self.rename_for_monet(self._obj)
        # make fake index
        if has_pyresample:
            d1 = self._make_fake_index_var(d1)
            ds1 = self._df_to_da(d1)
            ds2 = self._df_to_da(d2)
            source = ds1.monet._get_CoordinateDefinition(ds1)
            target = ds2.monet._get_CoordinateDefinition(ds2)
            res = pr.kd_tree.XArrayResamplerNN(
                source, target, radius_of_influence=radius_of_influence)
            res.get_neighbour_info()
            # interpolate just the make_fake_index variable
            # print(ds1)
            r = res.get_sample_from_neighbour_info(ds1.monet_fake_index)
            r.name = 'monet_fake_index'
            # r = ds2.monet.remap_nearest(
            #     ds1, radius_of_influence=radius_of_influence)
            # now merge back from original DataFrame
            q = r.compute()
            v = q.squeeze().to_dataframe()
            result = v.merge(d1, how='left',
                             on='monet_fake_index').drop('monet_fake_index',
                                                         axis=1)
            if combine:
                columns_to_use = result.columns.difference(d2.columns)
                return pd.merge(d2,
                                result[columns_to_use],
                                left_index=True,
                                right_index=True,
                                how='outer')
            else:
                return result

    def cftime_to_datetime64(self, col=None):
        """Short summary.

        Parameters
        ----------
        col : type
            Description of parameter `col`.

        Returns
        -------
        type
            Description of returned object.

        """
        df = self._obj
        def cf_to_dt64(x): return pd.to_datetime(x.strftime('%Y-%m-%d %H:%M:%S'))
        if col is None:  # assume 'time' is the column name to transform
            col = 'time'
        df[col] = df[col].apply(cf_to_dt64)
        return df

    def _make_fake_index_var(self, df):
        """Short summary.

        Parameters
        ----------
        df : type
            Description of parameter `df`.

        Returns
        -------
        type
            Description of returned object.

        """
        from numpy import arange
        column = df.columns[0]
        fake_index = arange(len(df))
        column_name = 'monet_fake_index'
        r = pd.Series(fake_index.astype(float), index=df.index)
        r.name = column_name
        df[column_name] = r
        return df


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
        """Short summary.

        Parameters
        ----------
        xray_obj : type
            Description of parameter `xray_obj`.

        Returns
        -------
        type
            Description of returned object.

        """
        self._obj = xray_obj

    def wrap_longitudes(self, lon_name='longitude'):
        """Ensures longitudes are from -180 -> 180

        Returns
        -------
        type
            Description of returned object.

        """
        dset = self._obj
        dset[lon_name] = (dset[lon_name] + 180) % 360 - 180
        return dset

    def tidy(self, lon_name='longitude'):
        """Tidy's DataArray–wraps longitudes and sorts lats and lons

        Returns
        -------
        xr.DataArray
            The tidy object

        """
        d = self._obj
        wd = d.monet.wrap_longitudes(lon_name=lon_name)
        wdl = wd.sortby(wd[lon_name])
        return wdl

    def is_land(self, return_xarray=False):
        """checks the mask of land and ocean.

        Parameters
        ----------
        return_xarray : bool
            If True, return the data array with the ocean values set to NaN.
            If False, return a numpy boolean array of the land (True).

        Returns
        -------
        xarray.DataArray or numpy.array


        """
        try:
            import global_land_mask as glm
        except ImportError:
            print('Please install global_land_mask from pypi')
        da = _dataset_to_monet(self._obj)
        island = glm.is_land(da.latitude.values, da.longitude.values)
        if return_xarray:
            return da.where(island)
        else:
            return island

    def is_ocean(self, return_xarray=False):
        """checks the mask of land and ocean.

        Parameters
        ----------
        return_xarray : bool
            If True, return the data array with the land values set to NaN.
            If False, return a numpy boolean array of the ocean (True).

        Returns
        -------
        xarray.DataArray or numpy.array


        """
        try:
            import global_land_mask as glm
        except ImportError:
            print('Please install global_land_mask from pypi')
        da = _dataset_to_monet(self._obj)
        isocean = glm.is_ocean(da.latitude.values, da.longitude.values)
        if return_xarray:
            return da.where(isocean)
        else:
            return isocean

    def cftime_to_datetime64(self, name=None):
        """Short summary.

        Parameters
        ----------
        name : type
            Description of parameter `name`.

        Returns
        -------
        type
            Description of returned object.

        """
        from numpy import vectorize
        da = self._obj
        def cf_to_dt64(x): return pd.to_datetime(x.strftime('%Y-%m-%d %H:%M:%S'))
        if name is None:  # assume 'time' is the column name to transform
            name = 'time'
        if isinstance(da[name].to_index(), xr.CFTimeIndex):
            da[name] = xr.apply_ufunc(vectorize(cf_to_dt64), da[name])
        return da

    def structure_for_monet(self,
                            lat_name='lat',
                            lon_name='lon',
                            return_obj=True):
        """This will attempt to restucture a given DataArray for use within MONET.

        Parameters
        ----------
        lat_name : type
            Description of parameter `lat_name`.
        lon_name : type
            Description of parameter `lon_name`.
        return : type
            Description of parameter `return`.

        Returns
        -------
        type
            Description of returned object.

        """
        if return_obj:
            return _dataset_to_monet(self._obj,
                                     lat_name=lat_name,
                                     lon_name=lon_name)
        else:
            self._obj = _dataset_to_monet(self._obj,
                                          lat_name=lat_name,
                                          lon_name=lon_name)

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
        out = resample_stratify(self._obj, levels, vertical, axis=1)
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
                dset = _dataset_to_monet(self._obj)
                lat = dset.latitude.isel(x=0).values
                lon = dset.longitude.isel(y=0).values
                dset['x'] = lon
                dset['y'] = lat
                # dset = dset.drop(['latitude', 'longitude'])
                # check if latitude is in the correct order
                if dset.latitude.isel(x=0).values[0] > dset.latitude.isel(
                        x=0).values[-1]:
                    lat_min_copy = lat_min
                    lat_min = lat_max
                    lat_max = lat_min_copy
                d = dset.sel(x=slice(lon_min, lon_max),
                             y=slice(lat_min, lat_max))
                return d
            elif has_pyresample:
                dset = _dataset_to_monet(self._obj)
                lons, lats = utils.check_and_wrap(dset.longitude.values,
                                                  dset.latitude.values)
                swath = llsd(longitude=lons, latitude=lats)
                x_ll, y_ll = dset.monet.nearest_ij(lat=float(lat_min), lon=float(lon_min))
                x_ur, y_ur = dset.monet.nearest_ij(lat=float(lat_max), lon=float(lon_max))
                # pswath_ll = npsd(longitude=float(lon_min),
                #                  latitude=float(lat_min))
                # pswath_ur = npsd(longitude=float(lon_max),
                #                  latitude=float(lat_max))
                # row, col = utils.generate_nearest_neighbour_linesample_arrays(
                #     swath, pswath_ll, 1e6)
                # y_ll, x_ll = row[0][0], col[0][0]
                # row, col = utils.generate_nearest_neighbour_linesample_arrays(
                #     swath, pswath_ur, 1e6)
                # y_ur, x_ur = row[0][0], col[0][0]
                if x_ur < x_ll:
                    x1 = dset.x.where(dset.x >= x_ll,
                                      drop=True).values
                    x2 = dset.x.where(dset.x <= x_ur,
                                      drop=True).values
                    xrange = concatenate([x1, x2]).astype(int)
                    dset['longitude'][:] = utils.wrap_longitudes(
                        dset.longitude.values)
                    # xrange = arange(float(x_ur), float(x_ll), dtype=int)
                else:
                    xrange = slice(x_ll, x_ur)
                if y_ur < y_ll:
                    y1 = dset.y.where(dset.y >= y_ll,
                                      drop=True).values
                    y2 = dset.y.where(dset.y <= y_ur,
                                      drop=True).values
                    yrange = concatenate([y1, y2]).astype(int)
                else:
                    yrange = slice(y_ll, y_ur)
                return dset.isel(x=xrange, y=yrange)
            else:
                raise ImportError
        except ImportError:
            print(
                """If this is a rectilinear grid and you don't have pyresample
                  please add the rectilinear=True to the call.  Otherwise the window
                  functionality is unavailable without pyresample""")

    def interp_constant_lat(self,
                            lat=None,
                            lat_name='latitude',
                            lon_name='longitude',
                            **kwargs):
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
        from numpy import linspace, ones, asarray
        try:
            import pyresample as pr
            has_pyresample = True
        except ImportError:
            has_pyresample = False
        if has_xesmf:
            from .util.interp_util import constant_1d_xesmf
            from .util.resample import resample_xesmf

        try:
            if lat is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lat value')
        d1 = _dataset_to_monet(self._obj, lat_name=lat_name, lon_name=lon_name)
        longitude = linspace(d1.longitude.min(), d1.longitude.max(), len(d1.x))
        latitude = ones(longitude.shape) * asarray(lat)
        if has_pyresample:
            d2 = xr.DataArray(ones((len(longitude), len(longitude))),
                              dims=['lon', 'lat'],
                              coords=[longitude, latitude])
            d2 = _dataset_to_monet(d2)
            result = d2.monet.remap_nearest(d1)
            return result.isel(y=0)
        elif has_xesmf:
            output = constant_1d_xesmf(latitude=latitude, longitude=longitude)
            out = resample_xesmf(self._obj, output, **kwargs)
            return _rename_latlon(out)

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
        if has_xesmf:
            from .util.interp_util import constant_1d_xesmf
            from .util.resample import resample_xesmf
        from numpy import linspace, ones, asarray
        try:
            if lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lon value')
        d1 = _dataset_to_monet(self._obj)
        latitude = linspace(d1.latitude.min(), d1.latitude.max(), len(d1.y))
        longitude = ones(latitude.shape) * asarray(lon)
        if has_pyresample:

            if has_pyresample:
                d2 = xr.DataArray(ones((len(longitude), len(longitude))),
                                  dims=['lon', 'lat'],
                                  coords=[longitude, latitude])
                d2 = _dataset_to_monet(d2)
                result = d2.monet.remap_nearest(d1)
                return result.isel(x=0)
            elif has_xesmf:
                output = constant_1d_xesmf(latitude=latitude,
                                           longitude=longitude)
                out = resample_xesmf(self._obj, output, **kwargs)
                return _rename_latlon(out)

    def nearest_ij(self, lat=None, lon=None, **kwargs):
        """Uses pyresample to intepolate to find the i, j index of grid with respect to the given lat lon.

        Parameters
        ----------
        lat : float
            latitude in question
        lon : float
            longitude in question
        **kwargs : dict
            pyresample kwargs for nearest neighbor interpolation

        Returns
        -------
        i,j
            Returns the i (x index) and j (y index) of the given latitude longitude value

        """
        try:
            from pyresample import geometry, utils
            from .util.interp_util import nearest_point_swathdefinition as npsd
            from .util.interp_util import lonlat_to_swathdefinition as llsd
            has_pyresample = True
        except ImportError:
            has_pyresample = False
            print('requires pyresample to be installed')

        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must provide latitude and longitude')

        if has_pyresample:
            dset = _dataset_to_monet(self._obj)
            lons, lats = utils.check_and_wrap(dset.longitude.values,
                                              dset.latitude.values)
            swath = llsd(longitude=lons, latitude=lats)
            pswath = npsd(longitude=float(lon), latitude=float(lat))
            row, col = utils.generate_nearest_neighbour_linesample_arrays(
                swath, pswath, float(1e6))
            y, x = row[0][0], col[0][0]
            return x, y

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
            from .util.interp_util import nearest_point_swathdefinition as npsd
            from .util.interp_util import lonlat_to_swathdefinition as llsd
            has_pyresample = True
        except ImportError:
            has_pyresample = False

        from .util.interp_util import lonlat_to_xesmf
        from .util.resample import resample_xesmf
        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must provide latitude and longitude')

        d = _dataset_to_monet(self._obj)
        if has_pyresample:
            lons, lats = utils.check_and_wrap(d.longitude.values,
                                              d.latitude.values)
            swath = self._get_CoordinateDefinition(d)
            pswath = npsd(longitude=float(lon), latitude=float(lat))
            row, col = utils.generate_nearest_neighbour_linesample_arrays(
                swath, pswath, **kwargs)
            y, x = row[0][0], col[0][0]
            return d.isel(x=x, y=y)
        elif has_xesmf:
            kwargs = self._check_kwargs_and_set_defaults(**kwargs)
            self._obj = _rename_latlon(self._obj)
            target = lonlat_to_xesmf(longitude=lon, latitude=lat)
            output = resample_xesmf(self._obj, target, **kwargs)
            if cleanup:
                output = resample_xesmf(self._obj,
                                        target,
                                        cleanup=True,
                                        **kwargs)
            return _rename_latlon(output.squeeze())

    @staticmethod
    def _check_kwargs_and_set_defaults(**kwargs):
        """Short summary.

        Parameters
        ----------
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        if 'reuse_weights' not in kwargs:
            kwargs['reuse_weights'] = False
        if 'method' not in kwargs:
            kwargs['method'] = 'bilinear'
        if 'periodic' not in kwargs:
            kwargs['periodic'] = False
        if 'filename' not in kwargs:
            kwargs['filename'] = 'monet_xesmf_regrid_file.nc'
        return kwargs

    def quick_imshow(self, map_kws={}, roll_datline=False, **kwargs):
        """Creates a quick map view of a given data array.

        Parameters
        ----------
        map_kws : dict
            kwargs for monet.plots.mapgen.draw_map.
        **kwargs : dict
            kwargs for xarray plotting.

        Returns
        -------
        matplotlib.axes
            return of the axes handle for matplotlib.

        """
        from .plots.mapgen import draw_map
        from .plots import _dynamic_fig_size
        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs
        import seaborn as sns
        sns.set_context('notebook', font_scale=1.2)
        da = _dataset_to_monet(self._obj)
        da = _monet_to_latlon(da)
        crs_p = ccrs.PlateCarree()
        if 'crs' not in map_kws:
            map_kws['crs'] = crs_p
        if 'figsize' in kwargs:
            map_kws['figsize'] = kwargs['figsize']
            kwargs.pop('figsize', None)
        else:
            figsize = _dynamic_fig_size(da)
            map_kws['figsize'] = figsize
            print(figsize[0], figsize[1])
        if 'transform' not in kwargs:
            transform = crs_p
        else:
            transform = kwargs['transform']
            kwargs.pop('transform', None)
        ax = draw_map(**map_kws)
        try:
            ax.axes.outline_patch.set_alpha(0)
        except:
            ax.outline_patch.set_alpha(0)
        if roll_dateline:
            ax = da.roll(lon=int(len(da.lon) / 2), roll_coords=True).plot.imshow(ax=ax, transform=transform, **kwargs)
        else:
            ax = da.plot.imshow(ax=ax, transform=transform, **kwargs)
        plt.tight_layout()
        return ax

    def quick_map(self, map_kws={}, roll_dateline=False, **kwargs):
        """Creates a quick map view of a given data array.

        Parameters
        ----------
        map_kws : dict
            kwargs for monet.plots.mapgen.draw_map.
        **kwargs : dict
            kwargs for xarray plotting.

        Returns
        -------
        matplotlib.axes
            return of the axes handle for matplotlib.

        """
        from .plots.mapgen import draw_map
        from .plots import _dynamic_fig_size
        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs
        import seaborn as sns
        sns.set_context('notebook')
        da = _dataset_to_monet(self._obj)
        crs_p = ccrs.PlateCarree()
        if 'crs' not in map_kws:
            map_kws['crs'] = crs_p
        if 'figsize' in kwargs:
            map_kws['figsize'] = kwargs['figsize']
            kwargs.pop('figsize', None)
        else:
            figsize = _dynamic_fig_size(da)
            map_kws['figsize'] = figsize
            print(figsize[0], figsize[1])
        if 'transform' not in kwargs:
            transform = crs_p
        else:
            transform = kwargs['transform']
            kwargs.pop('transform', None)
        ax = draw_map(**map_kws)
        try:
            ax.axes.outline_patch.set_alpha(0)
        except:
            ax.outline_patch.set_alpha(0)
        if roll_dateline:
            ax = da.roll(x=int(len(da.x) / 2), roll_coords=True).plot(x='longitude', y='latitude', ax=ax, transform=crs_p, **kwargs)
        else:
            ax = da.plot(x='longitude', y='latitude', ax=ax, transform=crs_p, **kwargs)
        try:
            ax.axes.outline_patch.set_alpha(0)
        except:
            ax.outline_patch.set_alpha(0)
        plt.tight_layout()
        return ax

    def quick_contourf(self, map_kws={}, roll_dateline=False, **kwargs):
        from monet.plots.mapgen import draw_map
        from monet.plots import _dynamic_fig_size
        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs
        import seaborn as sns
        sns.set_context('notebook')
        da = _dataset_to_monet(self._obj)
        crs_p = ccrs.PlateCarree()
        if 'crs' not in map_kws:
            map_kws['crs'] = crs_p
        if 'figsize' in kwargs:
            map_kws['figsize'] = kwargs['figsize']
            kwargs.pop('figsize', None)
        else:
            figsize = _dynamic_fig_size(da)
            map_kws['figsize'] = figsize
            print(figsize[0], figsize[1])
        if 'transform' not in kwargs:
            transform = crs_p
        else:
            transform = kwargs['transform']
            kwargs.pop('transform', None)
        ax = draw_map(**map_kws)
        try:
            ax.axes.outline_patch.set_alpha(0)
        except:
            ax.outline_patch.set_alpha(0)
        if roll_dateline:
            ax = da.roll(x=int(len(da.x) / 2), roll_coords=True).plot.contourf(x='longitude', y='latitude', ax=ax, transform=transform, **kwargs)
        else:
            ax = da.plot.contourf(x='longitude', y='latitude', ax=ax, transform=transform, **kwargs)

        plt.tight_layout()
        return ax

    def _tight_layout(self):
        """Short summary.

        Returns
        -------
        type
            Description of returned object.

        """
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

    def _get_CoordinateDefinition(self, data=None):
        """Creates a pyresample CoordinateDefinition

        Returns
        -------
        pyreseample.geometry.CoordinateDefinition

        """
        from pyresample import geometry as geo
        if data is not None:
            g = geo.CoordinateDefinition(lats=data.latitude,
                                         lons=data.longitude)
        else:
            g = geo.CoordinateDefinition(lats=self._obj.latitude,
                                         lons=self._obj.longitude)
        return g

    def remap_nearest(self, data, **kwargs):
        """Interpolates from another grid (data) to the current grid of self using pyresample.
            it assumes that the dimensions are ordered in y,x,z per
        pyresample docs

        Parameters
        ----------
        da : xarray DataArray or xarray DataSet
            Object to be interpolated
        radius_of_influence : float or integer
            radius of influcence for pyresample in meters.

        Returns
        -------
        xarray.DataArray
            resampled object on current grid.

        """
        from pyresample import utils
        from pyresample import kd_tree
        from .util import resample
        # from .grids import get_generic_projection_from_proj4
        # check to see if grid is supplied
        d1 = _dataset_to_monet(data)
        # print(d1)
        d2 = _dataset_to_monet(self._obj)
        # print(d2)
        source = self._get_CoordinateDefinition(data=d1)
        target = self._get_CoordinateDefinition(data=d2)
        r = kd_tree.XArrayResamplerNN(source, target, **kwargs)
        r.get_neighbour_info()
        if isinstance(d1, xr.DataArray):
            result = r.get_sample_from_neighbour_info(d1)
            result.name = d1.name
            result['latitude'] = d2.latitude

        elif isinstance(d1, xr.Dataset):
            results = {}
            for i in d1.data_vars.keys():
                results[i] = r.get_sample_from_neighbour_info(d1[i])
            result = xr.Dataset(results)
            if bool(d1.attrs):
                result.attrs = d1.attrs
            result.coords['latitude'] = d2.latitude
            result.coords['longitude'] = d2.longitude

        return result

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
        if has_xesmf:
            from .util import resample
            # check to see if grid is supplied
            target = _rename_latlon(self._obj)
            source = _rename_latlon(dataarray)
            out = resample.resample_xesmf(source,
                                          target,
                                          method=method,
                                          **kwargs)
            return _rename_to_monet_latlon(out)

    def combine_point(self,
                      data,
                      suffix=None,
                      pyresample=True,
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
        if has_pyresample:
            from .util.combinetool import combine_da_to_df
        if has_xesmf:
            from .util.combinetool import combine_da_to_df_xesmf
        # point source data
        da = _dataset_to_monet(self._obj)
        if isinstance(data, pd.DataFrame):
            if has_pyresample and pyresample:
                return combine_da_to_df(da, data, **kwargs)
            else:  # xesmf resample
                return combine_da_to_df_xesmf(da,
                                              data,
                                              suffix=suffix,
                                              **kwargs)
        else:
            print('d must be either a pd.DataFrame')


@xr.register_dataset_accessor('monet')
class MONETAccessorDataset(object):
    """Monet accessor to the xarray.Dataset.

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
        self._obj = xray_obj

    def is_land(self, return_xarray=False):
        """checks the mask of land and ocean if the global_land_mask libra.

        Parameters
        ----------
        return_xarray : bool
            If True, return the data array with the ocean values set to NaN.
            If False, return a numpy boolean array of the land (True).

        Returns
        -------
        xarray.DataArray or numpy.array


        """
        import global_land_mask as glm
        da = _dataset_to_monet(self._obj)
        island = glm.is_land(da.latitude.values, da.longitude.values)
        if return_xarray:
            return da.where(island)
        else:
            return island

    def is_ocean(self, return_xarray=False):
        """checks the mask of land and ocean.

        Parameters
        ----------
        return_xarray : bool
            If True, return the data array with the land values set to NaN.
            If False, return a numpy boolean array of the ocean (True).

        Returns
        -------
        xarray.DataArray or numpy.array


        """
        import global_land_mask as glm
        da = _dataset_to_monet(self._obj)
        isocean = glm.is_ocean(da.latitude.values, da.longitude.values)
        if return_xarray:
            return da.where(isocean)
        else:
            return isocean

    def cftime_to_datetime64(self, name=None):
        """Convert cftime o numpy datetime64 objects.

        Parameters
        ----------
        name : str
            time variable name.

        Returns
        -------
        xarray.DataArray
            Description of returned object.

        """
        from numpy import vectorize
        da = self._obj
        def cf_to_dt64(x): return pd.to_datetime(x.strftime('%Y-%m-%d %H:%M:%S'))
        if name is None:  # assume 'time' is the column name to transform
            name = 'time'
        if isinstance(da[name].to_index(), xr.CFTimeIndex):
            # assume cftime
            da[name] = xr.apply_ufunc(vectorize(cf_to_dt64), da[name])
        return da

    def remap_xesmf(self, data, **kwargs):
        """Resample the xesmf

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
        if has_xesmf:
            try:
                if isinstance(data, xr.DataArray):
                    data = _rename_latlon(data)
                    self._remap_xesmf_dataarray(data, **kwargs)
                elif isinstance(data, xr.Dataset):
                    data = _rename_latlon(data)
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
        da = self._remap_xesmf_dataarray(dataarray,
                                         self._obj,
                                         filename=filename,
                                         **kwargs)
        self._obj[da.name] = da
        das = {}
        das[da.name] = da
        for i in loop_vars[1:]:
            dataarray = dset[i]
            tmp = self._remap_xesmf_dataarray(dataarray,
                                              filename=filename,
                                              reuse_weights=True,
                                              **kwargs)
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
        target = self._obj
        out = resample.resample_xesmf(dataarray,
                                      target,
                                      method=method,
                                      filename=filename,
                                      **kwargs)
        if out.name in self._obj.variables:
            out.name = out.name + '_y'
        self._obj[out.name] = out
        return out

    def _get_CoordinateDefinition(self, data=None):
        """Creates a pyresample CoordinateDefinition

        Returns
        -------
        pyreseample.geometry.CoordinateDefinition

        """
        from pyresample import geometry as geo
        if data is not None:
            g = geo.CoordinateDefinition(lats=data.latitude,
                                         lons=data.longitude)
        else:
            g = geo.CoordinateDefinition(lats=self._obj.latitude,
                                         lons=self._obj.longitude)
        return g

    def remap_nearest(self, data, radius_of_influence=1e6):
        """Will remap data to the current dataset using the pyresample.kd_tree nearest neighbor interpolation.

        Parameters
        ----------
        data : xarray.DataArray or xarray.Dataset
            geospatial dataset that includes the latitude and longtide coordinates
        radius_of_influence : float
            radius_of_influence kwarg for pyresample.kd_tree. Default (1e6)

        Returns
        -------
        xarray.Dataset or xarray.DataArray
            The interpolated xarray object
        """
        from pyresample import utils
        from pyresample import kd_tree
        from .util import resample
        # from .grids import get_generic_projection_from_proj4
        # check to see if grid is supplied
        try:
            check_error = False
            if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
                check_error = False
            else:
                check_error = True
            if check_error:
                raise TypeError
        except TypeError:
            print('data must be either an Xarray.DataArray or Xarray.Dataset')
        d1 = _dataset_to_monet(data)
        d2 = _dataset_to_monet(self._obj)
        source = self._get_CoordinateDefinition(d1)
        target = self._get_CoordinateDefinition(d2)
        r = kd_tree.XArrayResamplerNN(source,
                                      target,
                                      radius_of_influence=radius_of_influence)
        r.get_neighbour_info()
        if isinstance(d1, xr.DataArray):
            result = r.get_sample_from_neighbour_info(d1)
            result.name = d1.name
            result['latitude'] = d2.latitude

        elif isinstance(d1, xr.Dataset):
            results = {}
            for i in d1.data_vars.keys():
                results[i] = r.get_sample_from_neighbour_info(d1[i])
            result = xr.Dataset(results)
            if bool(d1.attrs):
                result.attrs = d1.attrs
            result.coords['latitude'] = d2.latitude
            result.coords['longitude'] = d2.longitude

        return result

    def nearest_ij(self, lat=None, lon=None, **kwargs):
        """Uses pyresample to intepolate to find the i, j index of grid with respect to the given lat lon.

        Parameters
        ----------
        lat : float
            latitude in question
        lon : float
            longitude in question
        **kwargs : dict
            pyresample kwargs for nearest neighbor interpolation

        Returns
        -------
        i,j
            Returns the i (x index) and j (y index) of the given latitude longitude value

        """
        try:
            from pyresample import geometry, utils
            has_pyresample = True
        except ImportError:
            has_pyresample = False
            print('requires pyresample to be installed')
        from .util.interp_util import nearest_point_swathdefinition as npsd
        from .util.interp_util import lonlat_to_swathdefinition as llsd
        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must provide latitude and longitude')

        if has_pyresample:
            dset = _dataset_to_monet(self._obj)
            lons, lats = utils.check_and_wrap(dset.longitude.values,
                                              dset.latitude.values)
            swath = llsd(longitude=lons, latitude=lats)
            pswath = npsd(longitude=float(lon), latitude=float(lat))
            row, col = utils.generate_nearest_neighbour_linesample_arrays(
                swath, pswath, float(1e6))
            y, x = row[0][0], col[0][0]
            return x, y

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
            from .util.interp_util import nearest_point_swathdefinition as npsd
            from .util.interp_util import lonlat_to_swathdefinition as llsd
            has_pyresample = True
        except ImportError:
            has_pyresample = False

        from .util.interp_util import lonlat_to_xesmf
        from .util.resample import resample_xesmf
        try:
            if lat is None or lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must provide latitude and longitude')

        # d = _dataset_to_monet(self._obj)
        if has_pyresample:
            dset = _dataset_to_monet(self._obj)
            # print(dset)
            lons, lats = utils.check_and_wrap(dset.longitude.values,
                                              dset.latitude.values)
            swath = llsd(longitude=lons, latitude=lats)
            pswath = npsd(longitude=float(lon), latitude=float(lat))
            row, col = utils.generate_nearest_neighbour_linesample_arrays(
                swath, pswath, float(1e6))
            y, x = row[0][0], col[0][0]
            return dset.isel(x=x).isel(y=y)
        elif has_xesmf:
            kwargs = self._check_kwargs_and_set_defaults(**kwargs)
            self._obj = _rename_latlon(self._obj)
            target = lonlat_to_xesmf(longitude=lon, latitude=lat)
            output = resample_xesmf(self._obj, target, **kwargs)
            if cleanup:
                output = resample_xesmf(self._obj,
                                        target,
                                        cleanup=True,
                                        **kwargs)
            return _rename_latlon(output.squeeze())

    @staticmethod
    def _check_kwargs_and_set_defaults(**kwargs):
        """Short summary.

        Parameters
        ----------
        **kwargs : type
            Description of parameter `**kwargs`.

        Returns
        -------
        type
            Description of returned object.

        """
        if 'reuse_weights' not in kwargs:
            kwargs['reuse_weights'] = False
        if 'method' not in kwargs:
            kwargs['method'] = 'bilinear'
        if 'periodic' not in kwargs:
            kwargs['periodic'] = False
        if 'filename' not in kwargs:
            kwargs['filename'] = 'monet_xesmf_regrid_file.nc'
        return kwargs

    def interp_constant_lat(self,
                            lat=None,
                            lat_name='latitude',
                            lon_name='longitude',
                            **kwargs):
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
        from numpy import linspace, ones, asarray
        try:
            import pyresample as pr
            has_pyresample = True
        except ImportError:
            has_pyresample = False
        if has_xesmf:
            from .util.interp_util import constant_1d_xesmf
            from .util.resample import resample_xesmf

        try:
            if lat is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lat value')
        d1 = _dataset_to_monet(self._obj, lat_name=lat_name, lon_name=lon_name)
        longitude = linspace(d1.longitude.min(), d1.longitude.max(), len(d1.x))
        latitude = ones(longitude.shape) * asarray(lat)
        if has_pyresample:
            d2 = xr.DataArray(ones((len(longitude), len(longitude))),
                              dims=['lon', 'lat'],
                              coords=[longitude, latitude])
            d2 = _dataset_to_monet(d2)
            result = d2.monet.remap_nearest(d1)
            return result.isel(y=0)
        elif has_xesmf:
            output = constant_1d_xesmf(latitude=latitude, longitude=longitude)
            out = resample_xesmf(self._obj, output, **kwargs)
            return _rename_latlon(out)

    def interp_constant_lon(self, lon=None, **kwargs):
        """Interpolates the data array to constant longitude.

            Parameters
            ----------
            lon : float
                Latitude on which to interpolate to

            Returns
            -------
            xr.Dataset or xr.DataArray
                DataArray of at constant longitude

            """
        if has_xesmf:
            from .util.interp_util import constant_1d_xesmf
            from .util.resample import resample_xesmf
        from numpy import linspace, ones, asarray
        try:
            if lon is None:
                raise RuntimeError
        except RuntimeError:
            print('Must enter lon value')
        d1 = _dataset_to_monet(self._obj)
        latitude = linspace(d1.latitude.min(), d1.latitude.max(), len(d1.y))
        longitude = ones(latitude.shape) * asarray(lon)
        if has_pyresample:

            if has_pyresample:
                d2 = xr.DataArray(ones((len(longitude), len(longitude))),
                                  dims=['lon', 'lat'],
                                  coords=[longitude, latitude])
                d2 = _dataset_to_monet(d2)
                result = d2.monet.remap_nearest(d1)
                return result.isel(x=0)
            elif has_xesmf:
                output = constant_1d_xesmf(latitude=latitude,
                                           longitude=longitude)
                out = resample_xesmf(self._obj, output, **kwargs)
                return _rename_latlon(out)

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
        loop_vars = [
            i for i in self._obj.variables if 'z' in self._obj[i].dims
        ]
        orig = self._obj[loop_vars[0]].stratify(levels, vertical, axis=axis)
        dset = orig.to_dataset()
        dset.attrs = self._obj.attrs.copy()
        for i in loop_vars[1:]:
            dset[i] = self._obj[i].stratify(levels, vertical, axis=axis)
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
                lons, lats = utils.check_and_wrap(self._obj.longitude.values,
                                                  self._obj.latitude.values)
                swath = llsd(longitude=lons, latitude=lats)
                pswath_ll = npsd(longitude=float(lon_min),
                                 latitude=float(lat_min))
                pswath_ur = npsd(longitude=float(lon_max),
                                 latitude=float(lat_max))
                row, col = utils.generate_nearest_neighbour_linesample_arrays(
                    swath, pswath_ll, float(1e6))
                y_ll, x_ll = row[0][0], col[0][0]
                row, col = utils.generate_nearest_neighbour_linesample_arrays(
                    swath, pswath_ur, float(1e6))
                y_ur, x_ur = row[0][0], col[0][0]
                if x_ur < x_ll:
                    x1 = self._obj.x.where(self._obj.x >= x_ll,
                                           drop=True).values
                    x2 = self._obj.x.where(self._obj.x <= x_ur,
                                           drop=True).values
                    xrange = concatenate([x1, x2]).astype(int)
                    self._obj['longitude'][:] = utils.wrap_longitudes(
                        self._obj.longitude.values)
                    # xrange = arange(float(x_ur), float(x_ll), dtype=int)
                else:
                    xrange = slice(x_ll, x_ur)
                if y_ur < y_ll:
                    y1 = self._obj.y.where(self._obj.y >= y_ll,
                                           drop=True).values
                    y2 = self._obj.y.where(self._obj.y <= y_ur,
                                           drop=True).values
                    yrange = concatenate([y1, y2]).astype(int)
                else:
                    yrange = slice(y_ll, y_ur)
                return self._obj.sel(x=xrange, y=yrange)
            else:
                raise ImportError
        except ImportError:
            print('Window functionality is unavailable without pyresample')

    def combine_point(self,
                      data,
                      suffix=None,
                      pyresample=True,
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
        if has_pyresample:
            from .util.combinetool import combine_da_to_df
        if has_xesmf:
            from .util.combinetool import combine_da_to_df_xesmf
        # point source data
        da = _dataset_to_monet(self._obj)
        if isinstance(data, pd.DataFrame):
            if has_pyresample and pyresample:
                return combine_da_to_df(da, data, **kwargs)
            else:  # xesmf resample
                return combine_da_to_df_xesmf(da,
                                              data,
                                              suffix=suffix,
                                              **kwargs)
        else:
            print('d must be either a pd.DataFrame')

    def wrap_longitudes(self, lon_name='longitude'):
        """Ensures longitudes are from -180 -> 180

        Returns
        -------
        type
            Description of returned object.

        """
        dset = self._obj
        dset[lon_name] = (dset[lon_name] + 180) % 360 - 180
        return dset

    def tidy(self, lon_name='longitude'):
        """Tidy's DataArray–wraps longitudes and sorts lats and lons

        Returns
        -------
        xr.DataArray
            The tidy object

        """
        d = self._obj
        wd = d.monet.wrap_longitudes(lon_name=lon_name)
        wdl = wd.sortby(wd[lon_name])
        return wdl
