from __future__ import absolute_import, print_function

from pandas import Series

# from ..util import interp_util as interpo
# from ..obs import epa_util


def combine_da_to_df(da, df, col=None, radius_of_influence=12e3, merge=True):
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
    from util.interp_util import lonlat_to_swathdefinition
    from util.resample import resample_dataset
    try:
        if col is None:
            raise RuntimeError
    except RuntimeError:
        print('Must enter column name')
    dfn = df.dropna(subset=[col])
    dfnn = dfn.drop_duplicates(subset=['latitude', 'longitude'])
    # unit = dfnn[col + '_unit'].unique()[0]
    target_grid = lonlat_to_swathdefinition(
        longitude=dfnn.longitude.values, latitude=dfnn.latitude.values)
    da_interped = resample_dataset(
        da.compute(), target_grid, radius_of_influence=radius_of_influence)
    # add model if da.name is the same as column
    df_interped = da_interped.to_dataframe().reset_index()
    cols = Series(df_interped.columns)
    drop_cols = cols.loc[cols.isin(['x', 'y', 'z'])]
    df_interped.drop(drop_cols, axis=1, inplace=True)
    if da.name in df.columns:
        df_interped.rename(columns={da.name: da.name + '_new'}, inplace=True)
        print(df_interped.keys())
    final_df = df.merge(
        df_interped, on=['latitude', 'longitude', 'time'], how='left')
    return final_df

def combine_da_to_df_strat(df,
                           dobs,
                           daz, 
                           da, 
                           col_obs=None, 
                           col_mod=None, 
                           radius_of_influence=12e3, 
                           merge=True):
    """This function will combine an xarray data array with spatial information
    point observations in `df`.

    Parameters
    ----------
    daz:  xr.DataArray (Model Altitude)
           Description of parameter `damodp`
    da : xr.DataArray  (Model Variable)
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
    from util.interp_util import lonlat_to_swathdefinition
    from util.resample import resample_dataset_stratify
    from util.tools import findclosest_modtimes_to_dfindex
    import pandas as pd
    
    try:
        if col_obs is None:
            raise RuntimeError
    except RuntimeError:
        print('Must enter column name')
    dfn = df.dropna(subset=[col_obs])
    
    # unit = dfnn[col + '_unit'].unique()[0]
    #Set target grid lat/lon and altitudes
    target_grid = lonlat_to_swathdefinition(
        longitude=dfn.longitude.values, latitude=dfn.latitude.values)
    target_altitudes = dfn.altitude.values
    
     #Perform horizontal & Vertical interpolation:  
     #default to nearest neighbor and linear interpolation, respectively 
     #but independent of model time at this point as sorted by altitudes
    df_interped_vert = resample_dataset_stratify(da.compute(), 
                                            daz.compute(), 
                                            target_grid, 
                                            target_altitudes, 
                                            radius_of_influence=radius_of_influence)
    
    
    #Scan closest model times and set their indices to true for all obs times
    #df_timeindex = findclosest_modtimes_to_dfindex(da, dobs.time)
    
    #reindex 2d dataframe with obs sorted altitudes vs. model times
    #df_interped_vert.index = dobs.time
    df_interped_vert.index = sorted(target_altitudes)
    df_interped_vert.index.rename('altitude', inplace=True)
    df_interped_vert.columns = da.time
    
    #Make new dataframe with obs altitude and time
    dfsort = {'altitude': target_altitudes, 'time': dobs.time}
    dfsort = pd.DataFrame(data=dfsort)
    
    #Sort dataframe with ascending altitudes, expand to times to match, reset index
    dfsorted = dfsort.sort_values('altitude')
    dfsorted_index = dfsorted.reset_index(drop=True)
    
    
    #Place obs times into dataframe that are associated with sorted altitudes
    df_interped_vert.insert(0, 'obs_time', dfsorted_index['time'], allow_duplicates=True)
    
    #Re-sort whole dataframe as ascending times, and expand to whole dataframe
    df_interped_vert_sorted = df_interped_vert.sort_values('obs_time')
    df_interped_vert_sorted_i = df_interped_vert_sorted.reset_index(drop=True)
    
    #Scan closest model times and set their indices to true for all obs times
    df_times = findclosest_modtimes_to_dfindex(da, dobs.time)
    
    #Selecting subset of rows and columns in the datframe based on closest matching obs/model times
    df_interped_vert_sorted_isub = df_interped_vert_sorted_i[df_times]
    
    #May eventually want to mask if there are multiple model column hours for flights
    #Based on closest or resampled linear interpolated
    # i.e., if multiple columns...then mask...create one column
        
    #Add suffix to column headers with model variable name     
    df_interped_vert_sorted_isub.columns =[str(df_times) + '_' + col_mod]


    #Reindex interpolated datframe by observed times for merge
    df_interped_vert_sorted_isub.index = dobs.time
    df_interped_vert_sorted_isub.index.names = ['time']
      
    #Merge the interpolated model with original observation dataframe
    final_df = df.merge(
        df_interped_vert_sorted_isub,on=['time'], how='left')
    
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
    da_interped = da.monet.nearest_latlon(
        lon=lon, lat=lat, radius_of_influence=radius_of_influence)

    # FIXME: interp to height here

    dset[da.name] = da_interped

    return dset

def combine_mod_to_flight_data(daobs, 
                               damod,
                               modzvar, 
                               obsvar=None, 
                               modvar=None, 
                               resample = False, 
                               freq = '3600S'):
    """This function will combine an model xarray.DataArray with an xarray of
    flight path data (typically ICARTT, use icartt_mod reader), and deliver
    a combined dataframe/file for analysis
    
    It does pair up final obs and model data for analysis in dataframe
    Does NOT convert units to match

    Parameters
    ----------
    daobs : xarray.DataSet of Flight/ICARTT data
        Description of parameter `daobs`.
    dmod : xarray.DataSet of Model/CMAQ data
        Description of parameter `damod`.
    modzvar:  xrray.DataArray of Model/CMAQ heights/altitudes

    Returns
    -------
    xarray.Dataset
        returns the xarray.Dataset with the `da` added as an additional
        variable.

    """
    try:
        if obsvar is None or modvar is None:
            raise RuntimeError
    except RuntimeError:
        print('Error: Must enter both a obs and model variable name')
    
        
    #converts icartt flight obs to dataframe for combination/resampling
    dobsdf = daobs.to_dataframe()
        
    #performs horizontal and vertical interpolation and combines with obs
    dfinterp = combine_da_to_df_strat(dobsdf,
                                      daobs,
                                      modzvar,
                                      damod[modvar], 
                                      col_obs=obsvar, 
                                      col_mod=modvar, 
                                      radius_of_influence=12e3,
                                      merge=True)
    
     #final resample to the data to desired frequency (default = False)
    if resample:       
        dfinterpset = dfinterp.resample(freq).mean()  
    else:
        dfinterpset = dfinterp
   
    return dfinterpset
    
    
    
    
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
