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

def long_to_wide(df):
    from pandas import Series, merge
    w = df.pivot_table(
        values='obs', index=['time', 'siteid'],
        columns='variable').reset_index()
    cols = Series(df.columns)
    g = df.groupby('variable')
    for name, group in g:
        w[name + '_unit'] = group.units.unique()[0]
    #mergeon = hstack((index.values, df.variable.unique()))
    return merge(w, df, on=['siteid', 'time'])

def calc_8hr_rolling_max(df,col=None,window=None):
    df.index = df.time_local
    df_rolling = df.groupby('siteid')[col].rolling(window,center=True,win_type='boxcar').mean().reset_index().dropna()
    df_rolling_max = df_rolling.groupby('siteid').resample('D',on='time_local').max().reset_index(drop=True)
    df = df.reset_index(drop=True)
    return df.merge(df_rolling_max,on=['siteid','time_local'])

def calc_24hr_ave(df,col=None):
    df.index = df.time_local
    df_24hr_ave = df.groupby('siteid')[col].resample('D').mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_24hr_ave,on=['siteid','time_local'])

def calc_3hr_ave(df,col=None):
    df.index = df.time_local
    df_24hr_ave = df.groupby('siteid')[col].resample('3H').mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_24hr_ave,on=['siteid','time_local'])

def calc_annual_ave(df,col=None):
    df.index = df.time_local
    df_annual_ave = df.groupby('siteid')[col].resample('A').mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_annual_ave,on=['siteid','time_local'])

