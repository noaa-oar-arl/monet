from __future__ import absolute_import, print_function

from numpy import NaN, sort
from pandas import DataFrame, Series, concat

from monet.util import interpolation as interpo
from ..obs import epa_util


def combine_da_to_df(da, df, col=None, radius=12e3):
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
    try:
        if col is None:
            raise RuntimeError

        dfn = df.dropna(subset=[col])
        dfnn = dfn.drop_duplicates(subset=['latitude', 'longitude'])
        unit = dfnn[col + '_unit'].unique()[0]
        da_interped = interpo.interp_latlon(
            da, dfnn.latitude, dfnn.longitude, radius=radius)
        #print(da_interped)
        # add model if da.name is the same as column
        print(da_interped.name, col)
        if da_interped.name == col:
            da_interped.name == da_interped.name + '_model'
            print(da_interped.name)
        df_interped = da_interped.to_dataframe().reset_index()  #.drop('space')
        #print(df_interped.head())
        return df.merge(
            df_interped, on=['latitude', 'longitude', 'time'], how='left')
    except RuntimeError:
        print('Must enter column name')


# def combine_da_to_da_zt(da,df,)


def combine_to_df(model=None,
                  obs=None,
                  mapping_table=None,
                  lay=None,
                  radius=None):
    # first get mapping table for obs to model
    if radius is None:
        try:
            radius = model.dset.XCELL
        except AttributeError:
            radius = 40e3
    if mapping_table is None:
        mapping_table = get_mapping_table(model, obs)
    # get the data inside of the obs dataset (check for tolnet)
    if obs.objtype is not 'TOLNET' and obs.objtype is not 'AERONET':
        obslist = Series(obs.df.variable.unique())
        # find all variables to map
        comparelist = obslist.loc[obslist.isin(mapping_table.keys())]
        dfs = []
        for i in comparelist:
            print('Pairing: ' + i)
            obsdf = obs.df.groupby('variable').get_group(
                i)  # get observations locations
            obsunit = obsdf.units.unique()[0]  # get observation unit
            # get observation lat and lons
            dfn = obsdf.drop_duplicates(subset=['latitude', 'longitude'])
            factor = check_units(model, obsunit, variable=mapping_table[i][0])
            try:
                if lay is None and Series([model.objtype]).isin(
                    ['CAMX', 'CMAQ']).max():
                    modelvar = get_model_fields(
                        model, mapping_table[i], lay=0).compute() * factor
                else:
                    modelvar = get_model_fields(
                        model, mapping_table[i], lay=lay).compute() * factor
                mvar_interped = interpo.interp_latlon(
                    modelvar,
                    dfn.latitude.values,
                    dfn.longitude.values,
                    radius=radius)
                combined_df = merge_obs_and_model(
                    mvar_interped,
                    obsdf,
                    dfn,
                    model_time=modelvar.time.to_index(),
                    daily=obs.daily,
                    obstype=obs.objtype)
                dfs.append(combined_df)
            except KeyError:
                print(i + ' not in dataset and will not be paired')
        df = concat(dfs)

    return df


def merge_obs_and_model(model,
                        obs,
                        dfn,
                        model_time=None,
                        daily=False,
                        obstype=None):
    import pandas as pd
    e = pd.DataFrame(model, index=dfn.siteid, columns=model_time)
    w = e.stack(dropna=False).reset_index().rename(columns={
        'level_1': 'time',
        0: 'model'
    })
    if daily and pd.Series(['AirNow', 'AQS', 'IMPROVE']).isin([obstype]).max():
        w = w.merge(
            dfn[['siteid', 'variable', 'gmt_offset', 'pollutant_standard']],
            on='siteid',
            how='left')
        w = epa_util.regulatory_resample(w)
        w = w.merge(
            obs.drop(['time', 'gmt_offset', 'variable'], axis=1),
            on=['siteid', 'time_local', 'pollutant_standard'],
            how='left')
    elif daily:
        w.index = w.time
        w = w.resample('D').mean().reset_index().rename(
            columns={'level_1': 'time'})
        w = w.merge(obs, on=['siteid', 'time'], how='left')
    else:
        w = w.merge(
            obs, on=['siteid', 'time'],
            how='left')  # assume outputs are hourly
    return w


def get_model_fields(model, findkeys, lay=None, weights=None):
    from numpy import ones
    keys = model.dset.keys()
    print(findkeys)
    newkeys = Series(findkeys).loc[Series(findkeys).isin(keys)]
    if len(newkeys) > 1:
        mvar = model.select_layer(model.dset[newkeys[0]], lay=lay)
        for i in newkeys:
            mvar = mvar + model.select_layer(model.dset[newkeys[0]], lay=lay)
    else:
        mvar = model.get_var(findkeys[0], lay=lay)
    return mvar


def check_units(model, obsunit, variable=None):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    param : type
        Description of parameter `param` (the default is 'O3').
    aqs_param : type
        Description of parameter `aqs_param` (the default is 'OZONE').

    Returns
    -------
    type
        Description of returned object.

    """
    if obsunit == 'UG/M3':
        fac = 1.
    elif obsunit == 'PPB':
        fac = 1000.
    elif obsunit == 'ppbC':
        fac = 1000.
        if variable == 'ISOPRENE':
            fac *= 5.
        elif variable == 'BENZENE':
            fac *= 6.
        elif variable == 'TOLUENE':
            fac *= 7.
        elif variable == 'O-XYLENE':
            fac *= 8.
    else:
        fac = 1.
    return fac
