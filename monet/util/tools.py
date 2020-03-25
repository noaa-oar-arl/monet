from __future__ import division

from builtins import range

import numpy as np

__author__ = 'barry'


def search_listinlist(array1, array2):
    # find intersections

    s1 = set(array1.flatten())
    s2 = set(array2.flatten())

    inter = s1.intersection(s2)

    index1 = np.array([])
    index2 = np.array([])
    # find the indexes in array1
    for i in inter:
        index11 = np.where(array1 == i)
        index22 = np.where(array2 == i)
        index1 = np.concatenate([index1[:], index11[0]])
        index2 = np.concatenate([index2[:], index22[0]])

    return np.sort(np.int32(index1)), np.sort(np.int32(index2))


def linregress(x, y):
    import statsmodels.api as sm

    xx = sm.add_constant(x)
    model = sm.OLS(y, xx)
    fit = model.fit()
    b, a = fit.params[0], fit.params[1]
    rsquared = fit.rsquared
    std_err = np.sqrt(fit.mse_resid)
    return a, b, rsquared, std_err


def findclosest(list, value):
    a = min((abs(x - value), x, i) for i, x in enumerate(list))
    return a[2], a[1]


def _force_forder(x):
    """
    Converts arrays x to fortran order. Returns
    a tuple in the form (x, is_transposed).
    """
    if x.flags.c_contiguous:
        return (x.T, True)
    else:
        return (x, False)


def kolmogorov_zurbenko_filter(df, window, iterations):
    import pandas as pd
    """KZ filter implementation
        series is a pandas series
        window is the filter window m in the units of the data (m = 2q+1)
        iterations is the number of times the moving average is evaluated
        """
    z = df.copy()
    for i in range(iterations):
        z = pd.rolling_mean(z, window=window, min_periods=1, center=True)
    return z


def wsdir2uv(ws, wdir):
    from numpy import pi, sin, cos
    u = -ws * sin(wdir * pi / 180.)
    v = -ws * cos(wdir * pi / 180.)
    return u, v


def get_relhum(temp, press, vap):
    # temp:  temperature (K)
    # press: pressure (Pa)
    # vap:   water vapor mixing ratio (kg/kg)
    temp_o = 273.16
    es_vap = 611.0 * np.exp(17.67 * ((temp - temp_o) / (temp - 29.65)))
    ws_vap = 0.622 * (es_vap / press)
    relhum = 100.0 * (vap / ws_vap)
    return relhum


def long_to_wide(df):
    from pandas import Series, merge
    w = df.pivot_table(values='obs',
                       index=['time', 'siteid'],
                       columns='variable').reset_index()
    cols = Series(df.columns)
    g = df.groupby('variable')
    for name, group in g:
        w[name + '_unit'] = group.units.unique()[0]
    # mergeon = hstack((index.values, df.variable.unique()))
    return merge(w, df, on=['siteid', 'time'])


def calc_8hr_rolling_max(df, col=None, window=None):
    df.index = df.time_local
    df_rolling = df.groupby('siteid')[col].rolling(
        window, center=True, win_type='boxcar').mean().reset_index().dropna()
    df_rolling_max = df_rolling.groupby('siteid').resample(
        'D', on='time_local').max().reset_index(drop=True)
    df = df.reset_index(drop=True)
    return df.merge(df_rolling_max, on=['siteid', 'time_local'])


def calc_24hr_ave(df, col=None):
    df.index = df.time_local
    df_24hr_ave = df.groupby('siteid')[col].resample('D').mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_24hr_ave, on=['siteid', 'time_local'])


def calc_3hr_ave(df, col=None):
    df.index = df.time_local
    df_3hr_ave = df.groupby('siteid')[col].resample('3H').mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_3hr_ave, on=['siteid', 'time_local'])


def calc_annual_ave(df, col=None):
    df.index = df.time_local
    df_annual_ave = df.groupby('siteid')[col].resample(
        'A').mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_annual_ave, on=['siteid', 'time_local'])


def get_giorgi_region_bounds(index=None, acronym=None):
    import pandas as pd
    i = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        21, 22
    ]
    acro = [
        'NAU', 'SAU', 'AMZ', 'SSA', 'CAM', 'WNA', 'CNA', 'ENA', 'ALA', 'GRL',
        'MED', 'NEU', 'WAF', 'EAF', 'SAF', 'SAH', 'SEA', 'EAS', 'SAS', 'CAS',
        'TIB', 'NAS'
    ]
    lonmax = [
        155, 155, -34, -40, -83, -103, -85, -60, -103, -10, 40, 40, 22, 52, 52,
        65, 155, 145, 100, 75, 100, 180
    ]
    lonmin = [
        110, 110, -82, -76, -116, -130, -103, -85, -170, -103, -10, -10, -20,
        22, -10, -20, 95, 100, 65, 40, 75, 40
    ]
    latmax = [
        -11, -28, 12, -20, 30, 60, 50, 50, 72, 85, 48, 75, 18, 18, -12, 30, 20,
        50, 30, 50, 50, 70
    ]
    latmin = [
        -28, -45, -20, -56, 10, 30, 30, 25, 60, 50, 30, 48, -12, -12, -35, 18,
        -11, 20, 5, 30, 30, 50
    ]
    df = pd.DataFrame(
        {
            'latmin': latmin,
            'lonmin': lonmin,
            'latmax': latmax,
            'lonmax': lonmax,
            'acronym': acro
        },
        index=i)
    try:
        if index is None and acronym is None:
            print('either index or acronym needs to be supplied')
            print(
                'look here https://web.northeastern.edu/sds/web/demsos/images_002/subregions.jpg'
            )
            raise ValueError
        elif index is not None:
            return df.loc[df.index == index].values.flatten()
        else:
            return df.loc[df.acronym == acronym.upper()].values.flatten()
    except ValueError:
        exit


def get_giorgi_region_df(df):
    df.loc[:, 'GIORGI_INDEX'] = None
    df.loc[:, 'GIORGI_ACRO'] = None
    for i in range(22):
        latmin, lonmin, latmax, lonmax, acro = get_giorgi_region_bounds(
            index=int(i + 1))
        con = (df.longitude <= lonmax) & (df.longitude >= lonmin) & (
            df.latitude <= latmax) & (df.latitude >= latmin)
        df.loc[con, 'GIORGI_INDEX'] = i + 1
        df.loc[con, 'GIORGI_ACRO'] = acro
    return df


def get_epa_region_bounds(index=None, acronym=None):
    import pandas as pd
    i = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    acro = [
        'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'R10', 'AK',
        'PR', 'VI'
    ]
    lonmax = [
        -66.8628, -73.8885, -74.8526, -75.4129, -80.5188, -88.7421, -89.1005,
        -96.438, -109.0475, -111.0471, -129.99, -65.177765, -64.26384
    ]
    lonmin = [
        -73.7272, -79.7624, -83.6753, -91.6589, -97.2304, -109.0489, -104.0543,
        -116.0458, -124.6509, -124.7305, -169.9146, -67.289886, -64.861221
    ]
    latmax = [
        47.455, 45.0153, 42.5167, 39.1439, 49.3877, 37.0015, 43.5008, 48.9991,
        42.0126, 49.0027, 71.5232, 18.520551, 18.751244
    ]
    latmin = [
        40.9509, 38.8472, 36.5427, 24.3959, 36.9894, 25.8419, 35.9958, 36.9949,
        31.3325, 41.9871, 52.5964, 17.904834, 18.302014
    ]
    df = pd.DataFrame(
        {
            'latmin': latmin,
            'lonmin': lonmin,
            'latmax': latmax,
            'lonmax': lonmax,
            'acronym': acro
        },
        index=i)
    try:
        if index is None and acronym is None:
            print('either index or acronym needs to be supplied')
            print(
                'Look here for more information: https://www.epa.gov/enviro/epa-regional-kml-download',
                'https://gist.github.com/jakebathman/719e8416191ba14bb6e700fc2d5fccc5'
            )
            raise ValueError
        elif index is not None:
            return df.loc[df.index == index].values.flatten()
        else:
            return df.loc[df.acronym == acronym.upper()].values.flatten()
    except ValueError:
        exit


def get_epa_region_df(df):
    df.loc[:, 'EPA_INDEX'] = None
    df.loc[:, 'EPA_ACRO'] = None
    for i in range(13):
        latmin, lonmin, latmax, lonmax, acro = get_epa_region_bounds(
            index=int(i + 1))
        con = (df.longitude <= lonmax) & (df.longitude >= lonmin) & (
            df.latitude <= latmax) & (df.latitude >= latmin)
        df.loc[con, 'EPA_INDEX'] = i + 1
        df.loc[con, 'EPA_ACRO'] = acro
    return df
