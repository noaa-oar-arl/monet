from __future__ import absolute_import, print_function

from numpy import NaN, sort
from pandas import DataFrame, Series, concat

from . import interpolation as interpo
from ..obs import epa_util


def combine(model=None, obs=None, mapping_table=None, lay=None, radius=None):
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
            obsdf = obs.df.groupby('variable').get_group(i)  # get observations locations
            obsunit = obsdf.units.unique()[0]  # get observation unit
            # get observation lat and lons
            dfn = obsdf.drop_duplicates(subset=['latitude', 'longitude'])
            factor = check_units(model, obsunit, variable=mapping_table[i][0])
            # try:
            if lay is None and Series(['IMPROVE', 'AirNow', 'AQS', 'CRN', 'ISH']).isin([obs.objtype]).max():
                modelvar = get_model_fields(model, mapping_table[i], lay=0).compute() * factor
            elif lay is not None:
                modelvar = get_model_fields(model, mapping_table[i], lay=lay).compute() * factor
            else:
                modelvar = get_model_fields(model, mapping_table[i]).compute() * factor
            # except KeyError:
            #     print(i, ' not found... Skipping')
            #     pass
            mvar_interped = interpo.interp_latlon(modelvar, dfn.latitude.values, dfn.longitude.values, radius=radius)
            combined_df = merge_obs_and_model(mvar_interped, obsdf, dfn, model_time=modelvar.time.to_index(), daily=obs.daily, obstype=obs.objtype)
            dfs.append(combined_df)
        df = concat(dfs)

    return df


def merge_obs_and_model(model, obs, dfn, model_time=None, daily=False, obstype=None):
    import pandas as pd
    e = pd.DataFrame(model, index=dfn.siteid, columns=model_time)
    w = e.stack(dropna=False).reset_index().rename(columns={'level_1': 'time', 0: 'model'})
    if daily and pd.Series(['AirNow', 'AQS', 'IMPROVE']).isin([obstype]).max():
        w = w.merge(dfn[['siteid', 'variable', 'gmt_offset', 'pollutant_standard']], on='siteid', how='left')
        w = epa_util.regulatory_resample(w)
        w = w.merge(obs.drop(['time', 'gmt_offset', 'variable'], axis=1), on=['siteid', 'time_local', 'pollutant_standard'], how='left')
    elif daily:
        w.index = w.time
        w = w.resample('D').mean().reset_index().rename(columns={'level_1': 'time'})
        w = w.merge(obs, on=['siteid', 'time'], how='left')
    else:
        w = w.merge(obs, on=['siteid', 'time'], how='left')  # assume outputs are hourly
    return w


def get_model_fields(model, findkeys, lay=None, weights=None):
    from numpy import ones
    keys = model.dset.keys()
    newkeys = Series(findkeys).loc[Series(findkeys).isin(keys)]
    if len(newkeys) > 1:
        mvar = model.dset[newkeys[0]]
        for i in newkeys[1:]:
            mvar = mvar + model.dset[i]
    else:
        newkeys = findkeys
        mvar = model.dset[newkeys[0]]
    if lay is not None and Series(model.dset.dims).isin(['z']).max():
        mvar = mvar.sel(z=lay)
    elif lay is not None and Series(model.dset.dims).isin(['levels']).max():  # fix for hysplit temporary
        mvar = mvar.sel(levels=lay)
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


def get_mapping_table(model, obs):
    if obs.objtype is 'AirNow' or obs.objtype is 'AQS' or objtype is 'IMPROVE':
        if model.objtype == 'CMAQ':
            table = {'OZONE': ['O3'],
                     'PM2.5': ['PM25'],
                     'CO': ['CO'],
                     'NOY': ['NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA', 'NTR', 'CRON', 'CRN2', 'CRNO',
                             'CRPX', 'OPAN'],
                     'NOX': ['NO', 'NO2'],
                     'SO2': ['SO2'],
                     'NOX': ['NO', 'NO2'],
                     'NO': ['NO'],
                     'NO2': ['NO2'],
                     'SO4f': ['SO4f'],
                     'PM10': ['PM10'],
                     'NO3f': ['NO3f'],
                     'ECf': ['ECf'],
                     'OCf': ['OCf'],
                     'ETHANE': ['ETHA'],
                     'BENZENE': ['BENZENE'],
                     'TOLUENE': ['TOL'],
                     'ISOPRENE': ['ISOP'],
                     'O-XYLENE': ['XYL'],
                     'WS': ['WSPD10'],
                     'TEMP': ['TEMP2'],
                     'WD': ['WDIR10'],
                     'NAf': ['NAf'],
                     'MGf': ['AMGJ'],
                     'TIf': ['ATIJ'],
                     'SIf': ['ASIJ'],
                     'Kf': ['Kf'],
                     'CAf': ['CAf'],
                     'NH4f': ['NH4f'],
                     'FEf': ['AFEJ'],
                     'ALf': ['AALJ'],
                     'MNf': ['AMNJ']}
        elif model.objtype is 'CAMX':
            table = {'OZONE': ['O3'],
                     'PM2.5': ['PM25'],
                     'CO': ['CO'],
                     'NOY': ['NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA', 'NTR', 'CRON', 'CRN2', 'CRNO',
                             'CRPX', 'OPAN'],
                     'NOX': ['NO', 'NO2'],
                     'SO2': ['SO2'],
                     'NOX': ['NO', 'NO2'],
                     'NO': ['NO'],
                     'NO2': ['NO2'],
                     'SO4f': ['PSO4'],
                     'PM10': ['PM10'],
                     'NO3f': ['PNO3'],
                     'ECf': ['PEC'],
                     'OCf': ['OC'],
                     'ETHANE': ['ETHA'],
                     'BENZENE': ['BENZENE'],
                     'TOLUENE': ['TOL'],
                     'ISOPRENE': ['ISOP'],
                     'O-XYLENE': ['XYL'],
                     'WS': ['WSPD10'],
                     'TEMP': ['TEMP2'],
                     'WD': ['WDIR10'],
                     'NAf': ['NA'],
                     'NH4f': ['PNH4']}
    if obs.objtype is 'CRN':
        if model.objtype is 'CMAQ':
            table = {'SUR_TEMP': ['TEMPG'],
                     'T_HR_AVG': ['TEMP2'],
                     'SOLARAD': ['RGRND'],
                     'SOIL_MOISTURE_5': ['SOIM1'],
                     'SOIL_MOISTURE_10': ['SOIM2']}
    return table
