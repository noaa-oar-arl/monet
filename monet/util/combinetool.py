import xarray as xr
from numpy import ones
from pandas import Series, merge_asof


def combine_da_to_df(da, df, merge=True, **kwargs):
    """This function will combine an xarray data array with spatial information
    point observations in `df`.

    Parameters
    ----------
    da : xr.DataArray or xr.Dataset
        Description of parameter `da`.
    df : pd.DataFrame
        Description of parameter `df`.
    lay : iterable, default = [0]
        Description of parameter `lay`.
    radius : integer or float, default = 12e3
        Description of parameter `radius`.

    Returns
    -------
    pandas.DataFrame
    """
    from numpy import ones

    from ..util.interp_util import lonlat_to_swathdefinition

    # try:
    #     if col is None:
    #         raise RuntimeError
    # except RuntimeError:
    #     print('Must enter column name')
    # dfn = df.dropna(subset=[col])
    dfnn = df.drop_duplicates(subset=["siteid"]).dropna(subset=["latitude", "longitude", "siteid"])
    dfda = dfnn.monet._df_to_da()
    da_interped = dfda.monet.remap_nearest(da, **kwargs).compute()
    da_interped["siteid"] = (("x"), dfnn.siteid)
    df_interped = da_interped.to_dataframe().reset_index()
    cols = Series(df_interped.columns)
    drop_cols = cols.loc[cols.isin(["x", "y", "z", "latitude", "longitude"])]
    df_interped.drop(drop_cols, axis=1, inplace=True)
    if isinstance(da, xr.DataArray):
        if da.name in df.columns:
            df_interped.rename(columns={da.name: da.name + "_new"}, inplace=True)
    else:
        dup_name = isinname = [name for name in da.data_vars.keys() if name in df.columns]
        if len(dup_name) > 0:
            for name in dup_name:
                df_interped.rename(columns={name: name + "_new"}, inplace=True)
    if merge:
        df.reset_index(drop=True)
        df_interped.reset_index(drop=True)
        final_df = df.merge(df_interped, on=["time", "siteid"], how="left")
        return final_df
    else:
        return df_interped


def _rename_latlon(ds):
    if "latitude" in ds.coords:
        return ds.rename({"latitude": "lat", "longitude": "lon"})
    elif "lat" in ds.coords:
        return ds.rename({"lat": "latitude", "lon": "longitude"})
    else:
        return ds


def combine_da_to_df_xesmf(da, df, suffix=None, **kwargs):
    """This function will combine an xarray data array with spatial information
    point observations in `df`.

    Parameters
    ----------
    da : xr.DataArray
        Description of parameter `da`.
    df : pd.DataFrame
        Description of parameter `df`.
    lay : iterable, default = [0]
        Description of parameter `lay`.
    radius : integer or float, default = 12e3
        Description of parameter `radius`.

    Returns
    -------
    pandas.DataFrame
    """
    from ..util.interp_util import constant_1d_xesmf
    from ..util.resample import resample_xesmf

    # dfn = df.dropna(subset=[col])
    dfnn = df.drop_duplicates(subset=["latitude", "longitude"])
    # unit = dfnn[col + '_unit'].unique()[0]
    # target_grid = lonlat_to_swathdefinition(
    #     longitude=dfnn.longitude.values, latitude=dfnn.latitude.values)
    target = constant_1d_xesmf(longitude=dfnn.longitude.values, latitude=dfnn.latitude.values)

    da = _rename_latlon(da)  # check to rename latitude and longitude
    da_interped = resample_xesmf(da, target, **kwargs)
    da_interped = _rename_latlon(da_interped)  # check to change back
    if suffix is None:
        suffix = "_new"
    rename_dict = {}
    if isinstance(da_interped, xr.DataArray):
        if da_interped.name in dfnn.keys():
            da_interped.name = df_interped.name + suffix
    else:
        for i in da_interped.data_vars.keys():
            if i in dfnn.keys():
                rename_dict[i] = i + suffix
        da_interped = da_interped.rename(rename_dict)
    df_interped = da_interped.to_dataframe().reset_index()
    cols = Series(df_interped.columns)
    drop_cols = cols.loc[cols.isin(["x", "y", "z"])]
    df_interped.drop(drop_cols, axis=1, inplace=True)

    # if da.name in df.columns:
    #     df_interped.rename(columns={da.name: da.name + '_new'}, inplace=True)
    # print(df_interped.keys())
    final_df = df.merge(
        df_interped, on=["latitude", "longitude", "time"], how="left", suffixes=("", suffix)
    )
    return final_df


def combine_da_to_df_xesmf_strat(da, daz, df, **kwargs):
    """This function will combine an xarray data array with spatial information
    point observations in `df`.

    Parameters
    ----------
    da : xr.DataArray
        Description of parameter `da`.
    daz : xr.DataArray
        Description of parameter `daz`.
    df : pd.DataFrame
        Description of parameter `df`.
    lay : iterable, default = [0]
        Description of parameter `lay`.
    radius : integer or float, default = 12e3
        Description of parameter `radius`.

    Returns
    -------
    pandas.DataFrame
    """
    from ..util.interp_util import constant_1d_xesmf
    from ..util.resample import resample_xesmf

    try:
        if da.shape != daz.shape:
            raise RuntimeError
    except RuntimeError:
        print("da and daz must be of the same shape")
        print("da shape= ", da.shape, "daz shape= ", daz.shape)
        return -1

    target = constant_1d_xesmf(longitude=df.longitude.values, latitude=df.latitude.values)

    # check to rename 'latitude' and 'longitude' for xe.Regridder
    da = _rename_latlon(da)
    daz = _rename_latlon(daz)
    da_interped = resample_xesmf(da, target, **kwargs)  # interpolate fields
    daz_interped = resample_xesmf(daz, target, **kwargs)
    # check to change 'lat' 'lon' back
    da_interped = _rename_latlon(da_interped)
    daz_interped = _rename_latlon(daz_interped)

    # sort aircraft target altitudes and call stratfiy from resample to do vertical interpolation
    # resample_stratify from monet accessor
    daz_interped_xyz = daz_interped.monet.stratify(sorted(df["altitude"]), daz_interped, axis=1)
    da_interped_xyz = da_interped.monet.stratify(sorted(df["altitude"]), daz_interped, axis=1)
    da_interped_xyz.name = da.name
    daz_interped_xyz.name = "altitude"
    df_interped_xyz = da_interped_xyz.to_dataframe().reset_index()
    dfz_interped_xyz = daz_interped_xyz.to_dataframe().reset_index()

    df_interped_xyz.insert(0, "altitude", dfz_interped_xyz["altitude"], allow_duplicates=True)

    cols = Series(df_interped_xyz.columns)
    drop_cols = cols.loc[cols.isin(["x", "y", "z"])]
    df_interped_xyz.drop(drop_cols, axis=1, inplace=True)
    if da.name in df.columns:
        df_interped_xyz.rename(columns={da.name: da.name + "_new"}, inplace=True)
        print(df_interped_xyz.keys())

    final_df = merge_asof(
        df,
        df_interped_xyz,
        by=["latitude", "longitude", "altitude"],
        on="time",
        direction="nearest",
    )
    return final_df


def combine_da_to_height_profile(da, dset, radius_of_influence=12e3):
    """This function will combine an xarray.DataArray to a 2d dataset with
    dimensions (time,z)

    Parameters
    ----------
    da : xarray.DataArray
        Description of parameter `da`.
    dset : xarray.Dataset
        Description of parameter `dset`.

    Returns
    -------
    xarray.Dataset
        returns the xarray.Dataset with the `da` added as an additional
        variable.

    """
    # from ..util.interp_util import nearest_point_swathdefinition
    lon, lat = dset.longitude, dset.latitude
    # target_grid = nearest_point_swathdefinition(longitude=lon, latitude=lat)
    da_interped = da.monet.nearest_latlon(lon=lon, lat=lat, radius_of_influence=radius_of_influence)

    # FIXME: interp to height here

    dset[da.name] = da_interped

    return dset


#
# def combine_to_df(model=None,
#                   obs=None,
#                   mapping_table=None,
#                   lay=None,
#                   radius=None):
#     # first get mapping table for obs to model
#     if radius is None:
#         try:
#             radius = model.dset.XCELL
#         except AttributeError:
#             radius = 40e3
#     if mapping_table is None:
#         mapping_table = get_mapping_table(model, obs)
#     # get the data inside of the obs dataset (check for tolnet)
#     if obs.objtype is not 'TOLNET' and obs.objtype is not 'AERONET':
#         obslist = Series(obs.df.variable.unique())
#         # find all variables to map
#         comparelist = obslist.loc[obslist.isin(mapping_table.keys())]
#         dfs = []
#         for i in comparelist:
#             print('Pairing: ' + i)
#             obsdf = obs.df.groupby('variable').get_group(
#                 i)  # get observations locations
#             obsunit = obsdf.units.unique()[0]  # get observation unit
#             # get observation lat and lons
#             dfn = obsdf.drop_duplicates(subset=['latitude', 'longitude'])
#           factor = check_units(model, obsunit, variable=mapping_table[i][0])
#             try:
#                 if lay is None and Series([model.objtype]).isin(
#                     ['CAMX', 'CMAQ']).max():
#                     modelvar = get_model_fields(
#                         model, mapping_table[i], lay=0).compute() * factor
#                 else:
#                     modelvar = get_model_fields(
#                         model, mapping_table[i], lay=lay).compute() * factor
#                 mvar_interped = interpo.interp_latlon(
#                     modelvar,
#                     dfn.latitude.values,
#                     dfn.longitude.values,
#                     radius=radius)
#                 combined_df = merge_obs_and_model(
#                     mvar_interped,
#                     obsdf,
#                     dfn,
#                     model_time=modelvar.time.to_index(),
#                     daily=obs.daily,
#                     obstype=obs.objtype)
#                 dfs.append(combined_df)
#             except KeyError:
#                 print(i + ' not in dataset and will not be paired')
#         df = concat(dfs)
#
#     return df
#
#
# def merge_obs_and_model(model,
#                         obs,
#                         dfn,
#                         model_time=None,
#                         daily=False,
#                         obstype=None):
#     import pandas as pd
#     e = pd.DataFrame(model, index=dfn.siteid, columns=model_time)
#     w = e.stack(dropna=False).reset_index().rename(columns={
#         'level_1': 'time',
#         0: 'model'
#     })
#   if daily and pd.Series(['AirNow', 'AQS', 'IMPROVE']).isin([obstype]).max():
#         w = w.merge(
#             dfn[['siteid', 'variable', 'gmt_offset', 'pollutant_standard']],
#             on='siteid',
#             how='left')
#         w = epa_util.regulatory_resample(w)
#         w = w.merge(
#             obs.drop(['time', 'gmt_offset', 'variable'], axis=1),
#             on=['siteid', 'time_local', 'pollutant_standard'],
#             how='left')
#     elif daily:
#         w.index = w.time
#         w = w.resample('D').mean().reset_index().rename(
#             columns={'level_1': 'time'})
#         w = w.merge(obs, on=['siteid', 'time'], how='left')
#     else:
#         w = w.merge(
#             obs, on=['siteid', 'time'],
#             how='left')  # assume outputs are hourly
#     return w
#
#
# def get_model_fields(model, findkeys, lay=None, weights=None):
#     from numpy import ones
#     keys = model.dset.keys()
#     print(findkeys)
#     newkeys = Series(findkeys).loc[Series(findkeys).isin(keys)]
#     if len(newkeys) > 1:
#         mvar = model.select_layer(model.dset[newkeys[0]], lay=lay)
#         for i in newkeys:
#             mvar = mvar + model.select_layer(model.dset[newkeys[0]], lay=lay)
#     else:
#         mvar = model.get_var(findkeys[0], lay=lay)
#     return mvar
#
#
# def check_units(model, obsunit, variable=None):
#     """Short summary.
#
#     Parameters
#     ----------
#     df : type
#         Description of parameter `df`.
#     param : type
#         Description of parameter `param` (the default is 'O3').
#     aqs_param : type
#         Description of parameter `aqs_param` (the default is 'OZONE').
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     if obsunit == 'UG/M3':
#         fac = 1.
#     elif obsunit == 'PPB':
#         fac = 1000.
#     elif obsunit == 'ppbC':
#         fac = 1000.
#         if variable == 'ISOPRENE':
#             fac *= 5.
#         elif variable == 'BENZENE':
#             fac *= 6.
#         elif variable == 'TOLUENE':
#             fac *= 7.
#         elif variable == 'O-XYLENE':
#             fac *= 8.
#     else:
#         fac = 1.
#     return fac

# from __future__ import absolute_import, print_function
#
# from numpy import NaN, sort
# from pandas import concat
#
# from . import interpolation as interpo
# from ..obs import epa_util
#
#
# def combine(model=None, obs=None):
#     """Short summary.
#
#     Parameters
#     ----------
#     model : type
#         Description of parameter `model` (the default is None).
#     obs : type
#         Description of parameter `obs` (the default is None).
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     if model.objtype is 'CMAQ' and obs.objtype is 'AirNow':
#         df = combine_aqs_cmaq(model, obs)
#     if model.objtype is 'CAMX' and obs.objtype is 'AirNow':
#         df = combine_aqs_camx(model, obs)
#     if model.objtype is 'CMAQ' and obs.objtype is 'AQS':
#         if obs.daily:
#             df = combine_daily_aqs_cmaq(model, obs)
#         else:
#             df = combine_aqs_cmaq(model, obs)
#     if model.objtype is 'CAMX' and obs.objtype is 'AQS':
#         if obs.daily:
#             df = combine_daily_aqs_camx(model, obs)
#         else:
#             df = combine_aqs_cmaq(model, obs)
#     if (model.objtype is 'CMAQ' or model.objtype is 'CAMX') and obs.objtype is 'TOLNET':
#         model_dset, obs_dset = combine_tolnet_model(model, obs)
#     return df
#
#
# def combine_crn(model, obs):
#     """Short summary.
#
#     Parameters
#     ----------
#     model : type
#         Description of parameter `model`.
#     obs : type
#         Description of parameter `obs`.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     comparelist = obs.df.Species.unique()
#     g = obs.df.groupby('Species')
#     dfs = []
#     for i in comparelist:
#         if i == 'SUR_TEMP':
#             if ('TEMPG' in self.cmaq.metcrokeys):
#                 dfmet = g.get_group(i)
#                 cmaq = model.get_var(param='TEMPG').compute()
#                 dfmet = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                               radius=model.dset.XCELL)
#                 dfmet.Obs += 273.15
#                 dfs.append(dfmet)
#         elif i == 'T_HR_AVG':
#             if (self.cmaq.metcro2d is None) | ('TEMP2' not in self.cmaq.metcrokeys):
#                 dfmet = g.get_group(i)
#                 cmaq = model.get_var(param='TEMP2').compute()
#                 dfmet = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                               radius=model.dset.XCELL)
#                 dfmet.Obs += 273.15
#                 dfs.append(dfmet)
#         elif i == 'SOLARAD':
#             if (self.cmaq.metcro2d is None) | ('RGRND' not in self.cmaq.metcrokeys):
#                 dfmet = g.get_group(i)
#                 cmaq = model.get_var(param='RGRND').compute()
#                 dfmet = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                               radius=model.dset.XCELL)
#                 dfs.append(dfmet)
#         elif i == 'SOIL_MOISTURE_5':
#             if (self.cmaq.metcro2d is None) | ('SOIM1' not in self.cmaq.metcrokeys):
#                 dfmet = g.get_group(i)
#                 cmaq = model.get_var(param='SOILW').compute()
#                 dfmet = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                               radius=model.dset.XCELL)
#                 dfs.append(dfmet)
#         elif i == 'SOIL_MOISTURE_10':
#             if (self.cmaq.metcro2d is None) | ('SOIM1' not in self.cmaq.metcrokeys):
#                 dfmet = g.get_group(i)
#                 cmaq = model.get_var(param='SOILW').compute()
#                 dfmet = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                               radius=model.dset.XCELL)
#                 dfs.append(dfmet)
#     df = pd.concat(dfs)
#     df.dropna(inplace=True, subset=['Obs', 'model'])
#     return df
#
#
# def combine_improve_cmaq(model=None, obs=None):
#     """Short summary.
#
#     Parameters
#     ----------
#     model : type
#         Description of parameter `model` (the default is None).
#     obs : type
#         Description of parameter `obs` (the default is None).
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     comparelist = sort(obs.self.improve.df.Species.unique())
#     g = obs.df.groupby('Species')
#     dfs = []
#     for i in comparelist:
#         if i == 'CLf':
#             if ('ACLI' in self.cmaq.keys) | ('ACLJ' in self.cmaq.keys) | ('PM25_CL' in self.cmaq.keys):
#                 dfpm25 = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='CLf', improve_param=i)
#                 cmaq = self.cmaq.get_cmaqvar(lay=0, param='CLf').compute() * fac
#                 dfpm25 = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                                radius=model.dset.XCELL)
#                 self.cmaqpm25 = cmaq
#                 dfs.append(dfpm25)
#         elif i == 'PM10':
#             dfpm = g.get_group(i)
#             fac = epa_util.check_cmaq_units(param='PM10', improve_param=i)
#             cmaqvar = model.get_var(lay=0, param='PM10').compute() * fac
#             dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                          radius=model.dset.XCELL)
#             self.cmaqpm10 = cmaqvar
#             dfs.append(dfpm)
#         elif i == 'PM2.5':
#             dfpm = g.get_group(i)
#             fac = epa_util.check_cmaq_units(param='PM25', improve_param=i)
#             cmaqvar = model.get_var(lay=0, param='PM25').compute() * fac
#             dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                          radius=model.dset.XCELL)
#             self.cmaqpm25 = cmaqvar
#             dfs.append(dfpm)
#         elif i == 'NAf':
#             if ('ANAI' in self.cmaq.keys) | ('ANAJ' in self.cmaq.keys) | ('PM25_NA' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='NAf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='NAf').compute() * fac
#                 dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                              radius=model.dset.XCELL)
#                 self.cmaqna = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'MGf':
#             if ('AMGI' in self.cmaq.keys) | ('AMGJ' in self.cmaq.keys) | ('PM25_MG' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='MGf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='AMGJ').compute() * fac
#                 dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                              radius=model.dset.XCELL)
#                 self.cmaqmg = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'TIf':
#             if ('ATIJ' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='TIj', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='ATIJ').compute() * fac
#                 dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                              radius=model.dset.XCELL)
#                 self.cmaqti = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'SIf':
#             if ('ASIf' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='SIj', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='ASIJ').compute() * fac
#                 dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                              radius=model.dset.XCELL)
#                 self.cmaqti = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'Kf':
#             if ('AKI' in self.cmaq.keys) | ('AKJ' in self.cmaq.keys) | ('PM25_K' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='Kf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='Kf').compute() * fac
#                 dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                              radius=model.dset.XCELL)
#                 self.cmaqk = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'CAf':
#             if ('ACAJ' in self.cmaq.keys) | ('PM25_CA' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='CAf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='ACAJ').compute() * fac
#                 dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                              radius=model.dset.XCELL)
#                 self.cmaqca = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'SO4f':
#             if ('ASO4I' in self.cmaq.keys) | ('ASO4J' in self.cmaq.keys) | ('PM25_SO4' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='SO4f', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='SO4f').compute() * fac
#                 dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                              radius=model.dset.XCELL)
#                 self.cmaqso4 = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'NH4f':
#             if ('ANH4I' in self.cmaq.keys) | ('ANH4J' in self.cmaq.keys) | ('PM25_NH4' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='NH4f', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='NH4f').compute() * fac
#                 dfpm = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                              radius=model.dset.XCELL)
#                 self.cmaqnh4 = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'ammSO4f':
#             if ('ANH4I' in self.cmaq.keys) | ('ANH4J' in self.cmaq.keys) | ('PM25_NH4' in self.cmaq.keys):
#                 dfpmso4 = g.get_group(i)
#                 dfpmno3 = g.get_group('ammNO3f')
#                 dfpmso4.Species = 'NH4f'
#                 dfpm = merge(dfpmso4, dfpmno3[['Obs', 'datetime', 'Site_Code']], on=['datetime', 'Site_Code'])
#                 dfpm.rename(columns={'Obs_x': 'Obs'}, inplace=True)
#                 dfpm.Obs = 2 * dfpm.Obs * 18. / 132. + dfpm.Obs_y * 18. / 80.
#                 dfpm.drop('Obs_y', axis=1, inplace=True)
#                 cmaqvar = model.get_var(lay=0, param='NH4f')
#                 dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
#                 self.cmaqnh4 = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'NO3f':
#             if ('ANO3I' in self.cmaq.keys) | ('ANO3J' in self.cmaq.keys) | ('PM25_NO3' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='NO3f', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='NO3f').compute() * fac
#                 dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
#                 self.cmaqno3 = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'FEf':
#             if ('AFEJ' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='FEf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='AFEJ').compute() * fac
#                 dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
#                 self.cmaqfe = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'ALf':
#             if ('AALF' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='ALf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='AALF').compute() * fac
#                 dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
#                 self.cmaqal = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'MNf':
#             if ('AMNJ' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='MNf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='AMNJ').compute() * fac
#                 dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
#                 self.cmaqmn = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#         elif i == 'OCf':
#             if ('APOCJ' in self.cmaq.keys):
#                 dfpm = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(param='OCf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='OC').compute() * fac
#                 dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
#                 self.cmaqmn = cmaqvar
#                 dfs.append(dfpm)
#             else:
#                 pass
#
#
# def combine_aqs_camx(model, obs):
#     """Short summary.
#
#     Parameters
#     ----------
#     model : type
#         Description of parameter `model`.
#     obs : type
#         Description of parameter `obs`.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     g = obs.df.groupby('Species')
#     comparelist = sort(obs.df.Species.unique())
#     dfs = []
#     for i in comparelist:
#         if (i == 'OZONE') and ('O3' in model.keys):
#             print('Interpolating Ozone:')
#             df = g.get_group(i)
#             fac = epa_util.check_cmaq_units(df, param='O3', aqs_param=i)
#             cmaq = model.get_var(lay=0, param='O3').compute() * fac
#             df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                        radius=model.dset.XCELL)
#             df.Units = 'PPB'
#             dfs.append(df)
#         elif i == 'PM2.5':
#             if ('PM25_TOT' in model.keys) | ('ASO4J' in model.keys):
#                 print('Interpolating PM2.5:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='PM25', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='PM25').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'CO':
#             if 'CO' in model.keys:
#                 print('Interpolating CO:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='CO', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='CO').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NOY':
#             if 'NOY' in model.keys:
#                 print('Interpolating NOY:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NOY', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NOY').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'SO2':
#             if 'SO2' in model.keys:
#                 print('Interpolating SO2')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='SO2', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='SO2').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NOX':
#             if ('NO' in model.keys) | ('NO2' in model.keys):
#                 print('Interpolating NOX:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NOX', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NOX').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NO':
#             if ('NO' in model.keys):
#                 print('Interpolating NO:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NO2':
#             if ('NO2' in model.keys):
#                 print('Interpolating NO2:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO2', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO2').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'SO4f':
#             if ('PM25_SO4' in model.keys) | ('ASO4J' in model.keys) | ('ASO4I' in model.keys):
#                 print('Interpolating PSO4:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='SO4f', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='SO4f').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'PM10':
#             if ('PM_TOTAL' in self.camx.keys) | ('ASO4K' in self.camx.keys):
#                 print('Interpolating PM10:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='PM10', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='PM10').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NO3f':
#             if ('PM25_NO3' in model.keys) | ('ANO3J' in model.keys) | ('ANO3I' in model.keys):
#                 print('Interpolating PNO3:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO3f', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO3F').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'ECf':
#             if ('PM25_EC' in model.keys) | ('AECI' in model.keys) | ('AECJ' in model.keys):
#                 print('Interpolating PEC:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ECf', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ECf').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'OCf':
#             if ('APOCJ' in model.keys):
#                 print('Interpolating OCf:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='OCf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='OC').compute() * fac
#                 df = interpo.interp_to_obs(cmaqvar, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'ETHANE':
#             if ('ETHA' in model.keys):
#                 print('Interpolating Ethane:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ETHA', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ETHA').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'BENZENE':
#             if ('BENZENE' in model.keys):
#                 print('Interpolating BENZENE:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df,
#                                                 param='BENZENE', aqs_param=i)
#                 cmaq = model.get_var(
#                     lay=0, param='BENZENE').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'TOLUENE':
#             if ('TOL' in model.keys):
#                 print('Interpolating Toluene:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df,
#                                                 param='TOL', aqs_param=i)
#                 cmaq = model.get_var(
#                     lay=0, param='TOL').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'ISOPRENE':
#             if ('ISOP' in model.keys):
#                 print('Interpolating Isoprene:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ISOP', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ISOP').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'O-XYLENE':
#             if ('XYL' in model.keys):
#                 print('Interpolating Xylene')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='XYL', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='XYL').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'WS':
#             if ('WSPD10' in model.keys):
#                 print('Interpolating WS:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='WSPD10')
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'TEMP':
#             if 'TEMP2' in model.keys:
#                 print('Interpolating TEMP:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='TEMP2')
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'WD':
#             if ('WDIR10' in model.keys):
#                 print('Interpolating WD:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='WDIR10')
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#     df = concat(dfs)
#     df.dropna(subset=['Obs', 'CAMx'], inplace=True)
#     return df
#
#
# def combine_aqs_cmaq(model, obs):
#     """Short summary.
#
#     Parameters
#     ----------
#     model : type
#         Description of parameter `model`.
#     obs : type
#         Description of parameter `obs`.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#
#     g = obs.df.groupby('Species')
#     comparelist = sort(obs.df.Species.unique())
#     dfs = []
#     for i in comparelist:
#         if (i == 'OZONE'):  # & ('O3' in model.keys):
#             print('Interpolating Ozone:')
#             df = g.get_group(i)
#             fac = epa_util.check_cmaq_units(df, param='O3', aqs_param=i)
#             print(fac)
#             cmaq = model.get_var(lay=0, param='O3').compute() * fac
#             df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                        radius=model.dset.XCELL)
#             #                    df.Obs, df.CMAQ = df.Obs, df.CMAQ
#             df.Units = 'PPB'
#             dfs.append(df)
#         elif i == 'PM2.5':
#             if ('PM25_TOT' in model.keys) | ('ASO4J' in model.keys):
#                 print('Interpolating PM2.5:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='PM25', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='PM25').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'CO':
#             if 'CO' in model.keys:
#                 print('Interpolating CO:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='CO', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='CO').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NOY':
#             if 'NOY' in model.keys:
#                 print('Interpolating NOY:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NOY', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NOY').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'SO2':
#             if 'SO2' in model.keys:
#                 print('Interpolating SO2')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='SO2', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='SO2').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NOX':
#             if ('NO' in model.keys) | ('NO2' in model.keys):
#                 print('Interpolating NOX:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NOX', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NOX').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NO':
#             if ('NO' in model.keys):
#                 print('Interpolating NO:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NO2':
#             if ('NO2' in model.keys):
#                 print('Interpolating NO2:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO2', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO2').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'SO4f':
#             if ('PM25_SO4' in model.keys) | ('ASO4J' in model.keys) | ('ASO4I' in model.keys):
#                 print('Interpolating PSO4:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='SO4f', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='SO4f').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'PM10':
#             if ('PM_TOTAL' in model.keys) or ('ASO4K' in model.keys):
#                 print('Interpolating PM10:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='PM10', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='PM10').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'NO3f':
#             if ('PM25_NO3' in model.keys) | ('ANO3J' in model.keys) | ('ANO3I' in model.keys):
#                 print('Interpolating PNO3:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO3f', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO3F').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'ECf':
#             if ('PM25_EC' in model.keys) | ('AECI' in model.keys) | ('AECJ' in model.keys):
#                 print('Interpolating PEC:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ECf', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ECf').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'OCf':
#             if ('APOCJ' in model.keys):
#                 print('Interpolating OCf:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='OCf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='OC').compute() * fac
#                 df = interpo.interp_to_obs(cmaqvar, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'ETHANE':
#             if ('ETHA' in model.keys):
#                 print('Interpolating Ethane:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ETHA', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ETHA').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'BENZENE':
#             if ('BENZENE' in model.keys):
#                 print('Interpolating BENZENE:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='BENZENE', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='BENZENE').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'TOLUENE':
#             if ('TOL' in model.keys):
#                 print('Interpolating Toluene:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='TOL', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='TOL').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'ISOPRENE':
#             if ('ISOP' in model.keys):
#                 print('Interpolating Isoprene:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ISOP', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ISOP').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'O-XYLENE':
#             if ('XYL' in model.keys):
#                 print('Interpolating Xylene')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='XYL', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='XYL').compute() * fac
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'WS':
#             if ('WSPD10' in model.keys):
#                 print('Interpolating WS:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='WSPD10')
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'TEMP':
#             if 'TEMP2' in model.keys:
#                 print('Interpolating TEMP:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='TEMP2')
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#         elif i == 'WD':
#             if ('WDIR10' in model.keys):
#                 print('Interpolating WD:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='WDIR10')
#                 df = interpo.interp_to_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                            radius=model.dset.XCELL)
#                 dfs.append(df)
#     df = concat(dfs)
#     df.dropna(subset=['Obs', 'model'], inplace=True)
#     return df
#
#
# def combine_daily_aqs_cmaq(model, obs):
#     """Short summary.
#
#     Parameters
#     ----------
#     model : type
#         Description of parameter `model`.
#     obs : type
#         Description of parameter `obs`.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#
#     g = obs.d_df.groupby('Species')
#     comparelist = sort(obs.d_df.Species.unique())
#     for i in comparelist:
#         if (i == 'OZONE') and ('O3' in model.keys):
#             print('Interpolating Ozone:')
#             df = g.get_group(i)
#             fac = 1000.
#             cmaq = model.get_var(lay=0, param='O3').compute() * fac
#             df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                           radius=model.dset.XCELL, daily=True)
#             df.Units = 'PPB'
#             print(df)
#             dfs.append(df)
#         elif i == 'PM2.5':
#             if ('PM25_TOT' in model.keys) | ('ASO4J' in model.keys):
#                 print('Interpolating PM2.5:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='PM25', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='PM25').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'CO':
#             if 'CO' in model.keys:
#                 print('Interpolating CO:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='CO', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='CO').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NOY':
#             if 'NOY' in model.keys:
#                 print('Interpolating NOY:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NOY', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NOY').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'SO2':
#             if 'SO2' in model.keys:
#                 print('Interpolating SO2')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='SO2', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='SO2').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#
#                 dfs.append(df)
#         elif i == 'NOX':
#             if ('NO' in model.keys) & ('NO2' in model.keys):
#                 print('Interpolating NOX:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NOX', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NOX').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NO':
#             if ('NO' in model.keys):
#                 print('Interpolating NO:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NO2':
#             if ('NO2' in model.keys):
#                 print('Interpolating NO2:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO2', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO2').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'SO4f':
#             if ('PM25_SO4' in model.keys) | ('ASO4J' in model.keys) | ('ASO4I' in model.keys):
#                 print('Interpolating PSO4:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='SO4f', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='SO4f').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'PM10':
#             if ('PM_TOTAL' in model.keys) | ('ASO4K' in model.keys):
#                 print('Interpolating PM10:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='PM10', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='PM10').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                               radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NO3f':
#             if ('PM25_NO3' in model.keys) | ('ANO3J' in model.keys) | ('ANO3I' in model.keys):
#                 print('Interpolating PNO3:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO3f', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO3F').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'ECf':
#             if ('PM25_EC' in model.keys) | ('AECI' in model.keys) | ('AECJ' in model.keys):
#                 print('Interpolating PEC:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ECf', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ECf').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'OCf':
#             if ('APOCJ' in model.keys):
#                 print('Interpolating OCf:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df,
#                                                 param='OCf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='OC').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaqvar, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'ETHANE':
#             if ('ETHA' in model.keys):
#                 print('Interpolating Ethane:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ETHA', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ETHA').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'BENZENE':
#             if ('BENZENE' in model.keys):
#                 print('Interpolating BENZENE:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='BENZENE', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='BENZENE').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'TOLUENE':
#             if ('TOL' in model.keys):
#                 print('Interpolating Toluene:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='TOL', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='TOL').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'ISOPRENE':
#             if ('ISOP' in model.keys):
#                 print('Interpolating Isoprene:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ISOP', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ISOP').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'O-XYLENE':
#             if ('XYL' in model.keys):
#                 print('Interpolating Xylene')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='XYL', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='XYL').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'WS':
#             if ('WSPD10' in model.keys):
#                 print('Interpolating WS:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='WSPD10')
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'TEMP':
#             if 'TEMP2' in model.keys:
#                 print('Interpolating TEMP:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='TEMP2')
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'WD':
#             if 'WDIR10' in model.keys:
#                 print('Interpolating WD:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='WDIR10')
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#     df = concat(dfs)
#     df.loc[df.Obs < 0] = NaN
#     return df
#
#
# def combine_daily_aqs_camx(model, obs):
#     """Short summary.
#
#     Parameters
#     ----------
#     model : type
#         Description of parameter `model`.
#     obs : type
#         Description of parameter `obs`.
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     g = obs.d_df.groupby('Species')
#     comparelist = sort(obs.d_df.Species.unique())
#     for i in comparelist:
#         if (i == 'OZONE') and ('O3' in model.keys):
#             print('Interpolating Ozone:')
#             df = g.get_group(i)
#             fac = 1000.
#             camx = model.get_var(lay=0, param='O3').compute() * fac
#             df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                           radius=model.dset.XCELL, daily=True)
#             df.Units = 'PPB'
#             dfs.append(df)
#         elif i == 'PM2.5':
#             if ('PM25_TOT' in model.keys) | ('ASO4J' in model.keys):
#                 print('Interpolating PM2.5:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='PM25', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='PM25').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'CO':
#             if 'CO' in model.keys:
#                 print('Interpolating CO:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='CO', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='CO').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NOY':
#             if 'NOY' in model.keys:
#                 print('Interpolating NOY:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NOY', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NOY').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'SO2':
#             if 'SO2' in model.keys:
#                 print('Interpolating SO2')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='SO2', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='SO2').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NOX':
#             if ('NO' in model.keys) | ('NO2' in model.keys):
#                 print('Interpolating NOX:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NOX', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NOX').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NO':
#             if ('NO' in model.keys):
#                 print('Interpolating NO:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NO2':
#             if ('NO2' in model.keys):
#                 print('Interpolating NO2:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO2', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO2').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'SO4f':
#             if ('PM25_SO4' in model.keys) | ('ASO4J' in model.keys) | ('ASO4I' in model.keys):
#                 print('Interpolating PSO4:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='SO4f', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='SO4f').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'PM10':
#             if ('PM_TOTAL' in model.keys) | ('ASO4K' in model.keys):
#                 print('Interpolating PM10:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='PM10', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='PM10').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values, model.longitude.values,
#                                               radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'NO3f':
#             if ('PM25_NO3' in model.keys) | ('ANO3J' in model.keys) | ('ANO3I' in model.keys):
#                 print('Interpolating PNO3:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='NO3f', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='NO3F').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'ECf':
#             if ('PM25_EC' in model.keys) | ('AECI' in model.keys) | ('AECJ' in model.keys):
#                 print('Interpolating PEC:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ECf', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ECf').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'OCf':
#             if ('APOCJ' in model.keys):
#                 print('Interpolating OCf:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df,
#                                                 param='OCf', improve_param=i)
#                 cmaqvar = model.get_var(lay=0, param='OC').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaqvar, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'ETHANE':
#             if ('ETHA' in model.keys):
#                 print('Interpolating Ethane:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ETHA', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ETHA').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'BENZENE':
#             if ('BENZENE' in model.keys):
#                 print('Interpolating BENZENE:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='BENZENE', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='BENZENE').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'TOLUENE':
#             if ('TOL' in model.keys):
#                 print('Interpolating Toluene:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='TOL', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='TOL').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'ISOPRENE':
#             if ('ISOP' in model.keys):
#                 print('Interpolating Isoprene:')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='ISOP', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='ISOP').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'O-XYLENE':
#             if ('XYL' in model.keys):
#                 print('Interpolating Xylene')
#                 df = g.get_group(i)
#                 fac = epa_util.check_cmaq_units(df, param='XYL', aqs_param=i)
#                 cmaq = model.get_var(lay=0, param='XYL').compute() * fac
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'WS':
#             if ('WSPD10' in model.keys):
#                 print('Interpolating WS:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='WSPD10')
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'TEMP':
#             if 'TEMP2' in model.keys:
#                 print('Interpolating TEMP:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='TEMP2')
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#         elif i == 'WD':
#             if 'WDIR10' in model.keys:
#                 print('Interpolating WD:')
#                 df = g.get_group(i)
#                 cmaq = model.get_var(lay=0, param='WDIR10')
#                 df = interpo.interp_to_pt_obs(cmaq, df, model.latitude.values,
#                                               model.longitude.values, radius=model.dset.XCELL, daily=True)
#                 dfs.append(df)
#     df = concat(dfs)
#     df.loc[df.Obs < 0] = NaN
#     return df
#
#
# def combine_tolnet_model(model, obs, param='O3', resample=False, freq='H'):
#     """Short summary.
#
#     Parameters
#     ----------
#     model : type
#         Description of parameter `model`.
#     obs : type
#         Description of parameter `obs`.
#     param : type
#         Description of parameter `param` (the default is 'O3').
#     resample : type
#         Description of parameter `resample` (the default is False).
#     freq : type
#         Description of parameter `freq` (the default is 'H').
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     # wont do to much.  just interpolate the model to observations in the x y space
#     lat = obs.dset.Latitude
#     lon = obs.dset.Longitude
#     dset = find_nearest_latlon_xarray(model.dset[param], lat=lat, lon=lon, radius=model.dset.XCELL)
#     if resample:
#         dset = dset.resample(time=freq).mean()
#         tolnet = obs.dset.resample(time=freq).mean()
#     return dset, tolnet
