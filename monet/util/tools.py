"""
Utility tools for MONET.
"""

from typing import Optional, Tuple

import numpy as np
import pandas as pd
from numpy import cos, pi, sin
from pandas import merge

try:
    import statsmodels.api as sm
except ImportError:
    sm = None

__author__ = "barry"

# ==============================================================================
# Constants
# ==============================================================================

# Giorgi Regions Constants
GIORGI_INDICES = list(range(1, 23))
GIORGI_ACRONYMS = [
    "NAU",
    "SAU",
    "AMZ",
    "SSA",
    "CAM",
    "WNA",
    "CNA",
    "ENA",
    "ALA",
    "GRL",
    "MED",
    "NEU",
    "WAF",
    "EAF",
    "SAF",
    "SAH",
    "SEA",
    "EAS",
    "SAS",
    "CAS",
    "TIB",
    "NAS",
]
GIORGI_LONMAX = [
    155,
    155,
    -34,
    -40,
    -83,
    -103,
    -85,
    -60,
    -103,
    -10,
    40,
    40,
    22,
    52,
    52,
    65,
    155,
    145,
    100,
    75,
    100,
    180,
]
GIORGI_LONMIN = [
    110,
    110,
    -82,
    -76,
    -116,
    -130,
    -103,
    -85,
    -170,
    -103,
    -10,
    -10,
    -20,
    22,
    -10,
    -20,
    95,
    100,
    65,
    40,
    75,
    40,
    75,
    40,
]
GIORGI_LATMAX = [
    -11,
    -28,
    12,
    -20,
    30,
    60,
    50,
    50,
    72,
    85,
    48,
    75,
    18,
    18,
    -12,
    30,
    20,
    50,
    30,
    50,
    50,
    70,
]
GIORGI_LATMIN = [
    -28,
    -45,
    -20,
    -56,
    10,
    30,
    30,
    25,
    60,
    50,
    30,
    48,
    -12,
    -12,
    -35,
    18,
    -11,
    20,
    5,
    30,
    30,
    50,
]

# EPA Regions Constants
EPA_INDICES = list(range(1, 14))
EPA_ACRONYMS = [
    "R1",
    "R2",
    "R3",
    "R4",
    "R5",
    "R6",
    "R7",
    "R8",
    "R9",
    "R10",
    "AK",
    "PR",
    "VI",
]
EPA_LONMAX = [
    -66.8628,
    -73.8885,
    -74.8526,
    -75.4129,
    -80.5188,
    -88.7421,
    -89.1005,
    -96.438,
    -109.0475,
    -111.0471,
    -129.99,
    -65.177765,
    -64.26384,
]
EPA_LONMIN = [
    -73.7272,
    -79.7624,
    -83.6753,
    -91.6589,
    -97.2304,
    -109.0489,
    -104.0543,
    -116.0458,
    -124.6509,
    -124.7305,
    -169.9146,
    -67.289886,
    -64.861221,
]
EPA_LATMAX = [
    47.455,
    45.0153,
    42.5167,
    39.1439,
    49.3877,
    37.0015,
    43.5008,
    48.9991,
    42.0126,
    49.0027,
    71.5232,
    18.520551,
    18.751244,
]
EPA_LATMIN = [
    40.9509,
    38.8472,
    36.5427,
    24.3959,
    36.9894,
    25.8419,
    35.9958,
    36.9949,
    31.3325,
    41.9871,
    52.5964,
    17.904834,
    18.302014,
]


def search_listinlist(
    array1: np.ndarray, array2: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    """Find matching indices between two arrays.

    Parameters
    ----------
    array1 : numpy.ndarray
        First array to search for matches
    array2 : numpy.ndarray
        Second array to search for matches

    Returns
    -------
    tuple
        (index1, index2) containing:
        - index1: sorted array of indices in array1 where matches were found
        - index2: sorted array of indices in array2 where matches were found
    """
    # Find the intersection of the two arrays
    inter = np.intersect1d(array1, array2)

    # Find the indices in array1
    index1 = np.where(np.isin(array1, inter))[0]

    # Find the indices in array2
    index2 = np.where(np.isin(array2, inter))[0]

    return np.sort(index1), np.sort(index2)


def linregress(x: np.ndarray, y: np.ndarray) -> Tuple[float, float, float, float]:
    """Perform a linear regression using statsmodels.

    Parameters
    ----------
    x : array-like
        Independent variable values.
    y : array-like
        Dependent variable values.

    Returns
    -------
    tuple
        (slope, intercept, r_squared, standard_error) where:
        - slope is the regression line slope
        - intercept is the regression line y-intercept
        - r_squared is the coefficient of determination
        - standard_error is the standard error of the residuals
    """
    if sm is None:
        raise ImportError("statsmodels is required for linregress")

    xx = sm.add_constant(x)
    model = sm.OLS(y, xx)
    fit = model.fit()
    b, a = fit.params[0], fit.params[1]
    rsquared = fit.rsquared
    std_err = np.sqrt(fit.mse_resid)
    return a, b, rsquared, std_err


def findclosest(list_obj: list, value: float) -> Tuple[int, float]:
    """Find the index and value of the closest element to a target value.

    Parameters
    ----------
    list_obj : list-like
        Collection of values to search through.
    value : float or int
        The target value to find the closest match to.

    Returns
    -------
    tuple
        (index, closest_value) where:
        - index is the position in the list of the closest value
        - closest_value is the value from the list that is closest to the target
    """
    a = min((abs(x - value), x, i) for i, x in enumerate(list_obj))
    return a[2], a[1]


def _force_forder(x: np.ndarray) -> Tuple[np.ndarray, bool]:
    """
    Converts arrays x to fortran order. Returns
    a tuple in the form (x, is_transposed).

    Parameters
    ----------
    x : numpy.ndarray
        Array to potentially convert to Fortran-order.

    Returns
    -------
    tuple
        (result_array, is_transposed) where:
        - result_array is the array in Fortran-order
        - is_transposed is a boolean indicating if transposition was performed
    """
    if x.flags.c_contiguous:
        return (x.T, True)
    else:
        return (x, False)


def kolmogorov_zurbenko_filter(
    df: pd.DataFrame, col: str, window: int, iterations: int
) -> pd.DataFrame:
    """Apply a Kolmogorov-Zurbenko filter to a specific column in a DataFrame.

    A Kolmogorov-Zurbenko filter is a low-pass filter created by iteratively
    applying a moving average of specified window length. This implementation
    applies the filter to a DataFrame grouped by site ID.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the data to filter.
    col : str
        Column name to apply the filter to.
    window : int
        Size of the moving average window.
    iterations : int
        Number of times to apply the moving average filter.

    Returns
    -------
    pandas.DataFrame
        DataFrame with original data and filtered values merged in.
    """
    df.index = df.time_local
    z = df.copy()
    for i in range(iterations):
        z.index = z.time_local
        z = (
            z.groupby("siteid")[col]
            .rolling(window, center=True, min_periods=1)
            .mean()
            .reset_index()
            .dropna()
        )
    df = df.reset_index(drop=True)
    return df.merge(z, on=["siteid", "time_local"])


def wsdir2uv(ws: np.ndarray, wdir: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Convert wind speed and direction to U and V components.

    Parameters
    ----------
    ws : array-like
        Wind speed values.
    wdir : array-like
        Wind direction values in degrees (meteorological convention: 0=North, 90=East).

    Returns
    -------
    tuple
        (u, v) where:
        - u is the zonal wind component (positive for eastward wind)
        - v is the meridional wind component (positive for northward wind)
    """
    u = -ws * sin(wdir * pi / 180.0)
    v = -ws * cos(wdir * pi / 180.0)
    return u, v


def get_relhum(temp: np.ndarray, press: np.ndarray, vap: np.ndarray) -> np.ndarray:
    """Calculate relative humidity from temperature, pressure and vapor pressure.

    Parameters
    ----------
    temp : array-like
        Temperature in Kelvin
    press : array-like
        Pressure in hPa/mb
    vap : array-like
        Vapor pressure in hPa/mb

    Returns
    -------
    array-like
        Relative humidity as a percentage (0-100)
    """
    temp_o = 273.16
    es_vap = 611.0 * np.exp(17.67 * ((temp - temp_o) / (temp - 29.65)))
    ws_vap = 0.622 * (es_vap / press)
    relhum = 100.0 * (vap / ws_vap)
    return relhum


def long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a DataFrame from long (stacked) to wide format.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame in long format with 'time', 'siteid', 'variable',
        'obs', and 'units' columns

    Returns
    -------
    pandas.DataFrame
        DataFrame in wide format with variables as columns
    """
    w = df.pivot_table(
        values="obs", index=["time", "siteid"], columns="variable"
    ).reset_index()
    g = df.groupby("variable")
    for name, group in g:
        w[name + "_unit"] = group.units.unique()[0]
    return merge(w, df, on=["siteid", "time"])


def calc_8hr_rolling_max(
    df: pd.DataFrame, col: Optional[str] = None, window: Optional[int] = None
) -> pd.DataFrame:
    """Calculate 8-hour rolling maximum values.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data with 'time_local' and 'siteid' columns
    col : str
        Column name to calculate rolling max for
    window : int
        Rolling window size in hours

    Returns
    -------
    pandas.DataFrame
        DataFrame with added column containing 8-hour maxima
    """
    if col is None or window is None:
        raise ValueError("col and window must be provided")

    df.index = df.time_local
    df_rolling = (
        df.groupby("siteid")[col]
        .rolling(window, center=True, win_type="boxcar")
        .mean()
        .reset_index()
        .dropna()
    )
    df_rolling_max = (
        df_rolling.groupby("siteid")
        .resample("D", on="time_local")
        .max()
        .reset_index(drop=True)
    )
    df = df.reset_index(drop=True)
    return df.merge(df_rolling_max, on=["siteid", "time_local"])


def calc_24hr_ave(df: pd.DataFrame, col: Optional[str] = None) -> pd.DataFrame:
    """Calculate 24-hour averages.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data with 'time_local' and 'siteid' columns
    col : str
        Column name to average

    Returns
    -------
    pandas.DataFrame
        DataFrame with added column containing daily averages
    """
    if col is None:
        raise ValueError("col must be provided")

    df.index = df.time_local
    df_24hr_ave = df.groupby("siteid")[col].resample("D").mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_24hr_ave, on=["siteid", "time_local"])


def calc_3hr_ave(df: pd.DataFrame, col: Optional[str] = None) -> pd.DataFrame:
    """Calculate 3-hour averages.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data with 'time_local' and 'siteid' columns
    col : str
        Column name to average

    Returns
    -------
    pandas.DataFrame
        DataFrame with added column containing 3-hour averages
    """
    if col is None:
        raise ValueError("col must be provided")

    df.index = df.time_local
    df_3hr_ave = df.groupby("siteid")[col].resample("3H").mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_3hr_ave, on=["siteid", "time_local"])


def calc_annual_ave(df: pd.DataFrame, col: Optional[str] = None) -> pd.DataFrame:
    """Calculate annual averages.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data with 'time_local' and 'siteid' columns
    col : str
        Column name to average

    Returns
    -------
    pandas.DataFrame
        DataFrame with added column containing annual averages
    """
    if col is None:
        raise ValueError("col must be provided")

    df.index = df.time_local
    df_annual_ave = df.groupby("siteid")[col].resample("A").mean().reset_index()
    df = df.reset_index(drop=True)
    return df.merge(df_annual_ave, on=["siteid", "time_local"])


def get_giorgi_region_bounds(
    index: Optional[int] = None, acronym: Optional[str] = None
) -> np.ndarray:
    """Get lat/lon boundaries for a Giorgi region.

    Giorgi regions are geographical regions defined for climate studies.
    Returns bounds for a region specified by index number or acronym.

    Parameters
    ----------
    index : int, optional
        Region index number (1-22)
    acronym : str, optional
        Region acronym (e.g. 'NAU', 'SAU', etc)

    Returns
    -------
    numpy.ndarray
        Array containing [latmin, lonmin, latmax, lonmax, acronym]

    Notes
    -----
    Either index or acronym must be provided. For region definitions see:
    https://web.northeastern.edu/sds/web/demsos/images_002/subregions.jpg
    """
    df = pd.DataFrame(
        {
            "latmin": GIORGI_LATMIN,
            "lonmin": GIORGI_LONMIN,
            "latmax": GIORGI_LATMAX,
            "lonmax": GIORGI_LONMAX,
            "acronym": GIORGI_ACRONYMS,
        },
        index=GIORGI_INDICES,
    )

    if index is None and acronym is None:
        msg = (
            "either index or acronym needs to be supplied. "
            "look here https://web.northeastern.edu/sds/web/demsos/images_002/subregions.jpg"
        )
        raise ValueError(msg)
    elif index is not None:
        return df.loc[df.index == index].values.flatten()
    else:
        return df.loc[df.acronym == acronym.upper()].values.flatten()


def _add_region_info(coords, bounds, region_indices, region_acronyms):
    """Worker function to find region for a set of coordinates."""
    is_inside = np.all(
        (coords[:, np.newaxis, :] >= bounds[np.newaxis, :, :2])
        & (coords[:, np.newaxis, :] <= bounds[np.newaxis, :, 2:]),
        axis=2,
    )
    indices = np.argmax(is_inside, axis=1)
    mask = is_inside.any(axis=1)

    out_indices = np.full(len(coords), np.nan)
    out_acronyms = np.full(len(coords), None, dtype=object)

    out_indices[mask] = np.array(region_indices)[indices[mask]]
    out_acronyms[mask] = np.array(region_acronyms)[indices[mask]]

    return out_indices, out_acronyms


def get_giorgi_region_df(dset):
    """Add Giorgi region index and acronym to DataFrame or Dataset.

    This is a vectorized implementation using NumPy broadcasting for high
    performance on large datasets.

    Parameters
    ----------
    dset : pandas.DataFrame or xarray.Dataset
        DataFrame or Dataset containing 'latitude' and 'longitude' columns/coordinates.

    Returns
    -------
    pandas.DataFrame or xarray.Dataset
        Input object with added columns/variables:
        - GIORGI_INDEX: region index number (float, to accommodate NaN)
        - GIORGI_ACRO: region acronym (str)
    """
    bounds = np.array(
        [GIORGI_LONMIN[:22], GIORGI_LATMIN, GIORGI_LONMAX, GIORGI_LATMAX]
    ).T

    if isinstance(dset, pd.DataFrame):
        coords = dset[["longitude", "latitude"]].values
        indices, acronyms = _add_region_info(
            coords, bounds, GIORGI_INDICES, GIORGI_ACRONYMS
        )
        dset["GIORGI_INDEX"] = indices
        dset["GIORGI_ACRO"] = acronyms
        return dset
    else:  # xarray.Dataset
        lon, lat = np.meshgrid(dset.longitude, dset.latitude)
        coords = np.vstack([lon.ravel(), lat.ravel()]).T
        indices, acronyms = _add_region_info(
            coords, bounds, GIORGI_INDICES, GIORGI_ACRONYMS
        )

        dset["GIORGI_INDEX"] = (("latitude", "longitude"), indices.reshape(lon.shape))
        dset["GIORGI_ACRO"] = (("latitude", "longitude"), acronyms.reshape(lon.shape))
        return dset


def get_epa_region_bounds(
    index: Optional[int] = None, acronym: Optional[str] = None
) -> np.ndarray:
    """Get lat/lon boundaries for an EPA region.

    Parameters
    ----------
    index : int, optional
        EPA region number
    acronym : str, optional
        EPA region acronym

    Returns
    -------
    list
        [latmin, lonmin, latmax, lonmax, acronym]
    """
    df = pd.DataFrame(
        {
            "latmin": EPA_LATMIN,
            "lonmin": EPA_LONMIN,
            "latmax": EPA_LATMAX,
            "lonmax": EPA_LONMAX,
            "acronym": EPA_ACRONYMS,
        },
        index=EPA_INDICES,
    )

    if index is None and acronym is None:
        msg = (
            "either index or acronym needs to be supplied. "
            "Look here for more information: "
            "https://www.epa.gov/enviro/epa-regional-kml-download "
            "https://gist.github.com/jakebathman/719e8416191ba14bb6e700fc2d5fccc5"
        )
        raise ValueError(msg)
    elif index is not None:
        return df.loc[df.index == index].values.flatten()
    else:
        return df.loc[df.acronym == acronym.upper()].values.flatten()


def get_epa_region_df(dset):
    """Add EPA region index and acronym to DataFrame or Dataset.

    This is a vectorized implementation using NumPy broadcasting for high
    performance on large datasets.

    Parameters
    ----------
    dset : pandas.DataFrame or xarray.Dataset
        DataFrame or Dataset containing 'latitude' and 'longitude' columns/coordinates.

    Returns
    -------
    pandas.DataFrame or xarray.Dataset
        Input object with added columns/variables:
        - EPA_INDEX: region index number (float, to accommodate NaN)
        - EPA_ACRO: region acronym (str)
    """
    bounds = np.array([EPA_LONMIN, EPA_LATMIN, EPA_LONMAX, EPA_LATMAX]).T

    if isinstance(dset, pd.DataFrame):
        coords = dset[["longitude", "latitude"]].values
        indices, acronyms = _add_region_info(coords, bounds, EPA_INDICES, EPA_ACRONYMS)
        dset["EPA_INDEX"] = indices
        dset["EPA_ACRO"] = acronyms
        return dset
    else:  # xarray.Dataset
        lon, lat = np.meshgrid(dset.longitude, dset.latitude)
        coords = np.vstack([lon.ravel(), lat.ravel()]).T
        indices, acronyms = _add_region_info(coords, bounds, EPA_INDICES, EPA_ACRONYMS)

        dset["EPA_INDEX"] = (("latitude", "longitude"), indices.reshape(lon.shape))
        dset["EPA_ACRO"] = (("latitude", "longitude"), acronyms.reshape(lon.shape))
        return dset
