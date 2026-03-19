"""
Utility tools for MONET.
"""

from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
from pandas import merge

from .aero import _apply_aero
from .conventions import update_history

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


def search_listinlist(array1: np.ndarray, array2: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
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


def linregress(x: xr.DataArray | np.ndarray, y: xr.DataArray | np.ndarray, dim: str | None = None) -> Any:
    """Perform a linear regression.

    This implementation is backend-agnostic and supports Dask-backed
    xarray objects using xarray.apply_ufunc. If x and y are multi-dimensional,
    the regression is performed over the specified dimension (for xarray)
    or the last dimension (for numpy).

    Parameters
    ----------
    x : numpy.ndarray or xarray.DataArray
        Independent variable values.
    y : numpy.ndarray or xarray.DataArray
        Dependent variable values.
    dim : str, optional
        The dimension along which to perform the regression. Only used if
        inputs are xarray objects. If None and inputs are xarray, the last
        dimension is used.

    Returns
    -------
    slope, intercept, r_squared, std_err : same type as input
        - slope: regression line slope
        - intercept: regression line y-intercept
        - r_squared: coefficient of determination
        - std_err: standard error of the residuals
    """

    def _logic(x, y):
        # We use numpy for the core logic to avoid statsmodels dependency
        # and support vectorized operations across chunks.
        # Ensure we are working with at least 1D arrays
        x = np.asanyarray(x)
        y = np.asanyarray(y)

        # Handle multi-dimensional arrays by calculating along the last axis
        # This is compatible with apply_ufunc(..., input_core_dims=[['dim'], ['dim']])
        n = x.shape[-1]
        sum_x = np.sum(x, axis=-1)
        sum_y = np.sum(y, axis=-1)
        sum_xx = np.sum(x * x, axis=-1)
        sum_yy = np.sum(y * y, axis=-1)
        sum_xy = np.sum(x * y, axis=-1)

        denominator = n * sum_xx - sum_x**2
        # Use np.where to avoid division by zero
        # Ensure floating point division to match xr.apply_ufunc expectations and avoid casting errors
        slope = np.divide(
            (n * sum_xy - sum_x * sum_y).astype(float),
            denominator.astype(float),
            out=np.zeros_like(denominator, dtype=float),
            where=denominator != 0,
        )
        intercept = (sum_y.astype(float) - slope * sum_x.astype(float)) / n

        # R-squared
        # SS_tot = sum((y - y_mean)**2) = sum(y**2) - (sum(y)**2)/n
        # SS_res = sum((y - (slope*x + intercept))**2)
        ss_tot = sum_yy - (sum_y**2) / n
        y_pred = slope[..., np.newaxis] * x + intercept[..., np.newaxis]
        ss_res = np.sum((y - y_pred) ** 2, axis=-1)

        r_squared = np.divide(ss_tot - ss_res, ss_tot, out=np.zeros_like(ss_tot), where=ss_tot != 0)
        std_err = np.sqrt(np.divide(ss_res, n - 2, out=np.zeros_like(ss_res), where=n > 2))

        return slope, intercept, r_squared, std_err

    # Determine core dimension for xarray
    if dim is None and isinstance(x, xr.DataArray):
        dim = x.dims[-1]
    elif dim is None:
        dim = "core_dim"  # Placeholder for numpy

    return _apply_aero(
        _logic,
        x,
        y,
        name="linear regression",
        output_dtypes=[float, float, float, float],
        input_core_dims=[[dim], [dim]],
        output_core_dims=[[], [], [], []],
        source="monet.util.tools",
    )


def findclosest(list_obj: Any, value: Any) -> Any:
    """Find the index and value of the closest element to a target value.

    This implementation is backend-agnostic and supports Dask-backed
    xarray objects.

    Parameters
    ----------
    list_obj : array-like
        Collection of values to search through.
    value : float, int, or array-like
        The target value(s) to find the closest match to.

    Returns
    -------
    index, closest_value : same type as input
        - index: the position in list_obj of the closest value
        - closest_value: the value from list_obj that is closest to the target
    """
    if isinstance(list_obj, xr.DataArray | xr.Dataset) or isinstance(value, xr.DataArray | xr.Dataset):
        # Use xarray operations to preserve laziness and avoid apply_ufunc scalar issues
        # Ensure they are DataArrays for indexing
        if not isinstance(list_obj, xr.DataArray):
            list_obj = xr.DataArray(list_obj, dims=["search_dim"])
        if not isinstance(value, xr.DataArray):
            value = xr.DataArray(value)

        # Name search dimension if not already named
        if len(list_obj.dims) == 1 and list_obj.dims[0] == "dim_0":
            list_obj = list_obj.rename({"dim_0": "search_dim"})
        search_dim = list_obj.dims[0]

        diff = np.abs(list_obj - value)
        idx = diff.argmin(dim=search_dim)
        res = list_obj.isel({search_dim: idx})

        # Add history
        for out in (idx, res):
            if hasattr(out, "attrs"):
                update_history(out, "Found closest element via monet.util.tools")

        return idx, res

    # Fallback for non-xarray (NumPy or small lists)
    arr = np.asanyarray(list_obj)
    val = np.asanyarray(value)

    if arr.ndim == 1 and (val.ndim == 0 or val.size == 1):
        # Original simple path
        a = min((abs(x - float(val)), x, i) for i, x in enumerate(list_obj))
        return a[2], a[1]

    # Vectorized NumPy path
    diff = np.abs(arr[np.newaxis, :] - val[..., np.newaxis])
    idx = np.argmin(diff, axis=-1)
    return idx, arr[idx]


def nearest(items: Any, pivot: Any) -> Any:
    """Find the nearest value to pivot in a collection.

    This implementation is backend-agnostic and supports Dask-backed
    xarray objects using xarray.apply_ufunc.

    Parameters
    ----------
    items : array-like
        Collection of values to search through.
    pivot : float, int, or array-like
        The value(s) to find the nearest match to.

    Returns
    -------
    closest_value : same type as input
        The item from the collection that is closest to the pivot value.
    """
    _, val = findclosest(items, pivot)
    return val


def _force_forder(x: np.ndarray) -> tuple[np.ndarray, bool]:
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


def kolmogorov_zurbenko_filter(df: pd.DataFrame, col: str, window: int, iterations: int) -> pd.DataFrame:
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
        z = z.groupby("siteid")[col].rolling(window, center=True, min_periods=1).mean().reset_index().dropna()
    df = df.reset_index(drop=True)
    return df.merge(z, on=["siteid", "time_local"])


def wsdir2uv(ws: Any, wdir: Any) -> Any:
    """Convert wind speed and direction to U and V components.

    This implementation is backend-agnostic and supports Dask-backed
    xarray objects using xarray.apply_ufunc.

    Parameters
    ----------
    ws : float, numpy.ndarray, or xarray.DataArray
        Wind speed values.
    wdir : float, numpy.ndarray, or xarray.DataArray
        Wind direction values in degrees (meteorological convention: 0=North, 90=East).

    Returns
    -------
    u, v : same type as input
        - u is the zonal wind component (positive for eastward wind)
        - v is the meridional wind component (positive for northward wind)
    """

    def _logic(ws, wdir):
        u = -ws * np.sin(wdir * np.pi / 180.0)
        v = -ws * np.cos(wdir * np.pi / 180.0)
        return u, v

    return _apply_aero(
        _logic,
        ws,
        wdir,
        name="U and V wind components",
        output_dtypes=[float, float],
        output_core_dims=[[], []],
        source="monet.util.tools.wsdir2uv",
    )


def get_relhum(temp: Any, press: Any, vap: Any) -> Any:
    """Calculate relative humidity from temperature, pressure and vapor pressure.

    This implementation is backend-agnostic and supports Dask-backed
    xarray objects using xarray.apply_ufunc.

    Parameters
    ----------
    temp : float, numpy.ndarray, or xarray.DataArray
        Temperature in Kelvin
    press : float, numpy.ndarray, or xarray.DataArray
        Pressure in hPa/mb
    vap : float, numpy.ndarray, or xarray.DataArray
        Vapor pressure in hPa/mb

    Returns
    -------
    relhum : same type as input
        Relative humidity as a percentage (0-100)
    """

    def _logic(temp, press, vap):
        temp_o = 273.16
        es_vap = 611.0 * np.exp(17.67 * ((temp - temp_o) / (temp - 29.65)))
        ws_vap = 0.622 * (es_vap / press)
        return 100.0 * (vap / ws_vap)

    return _apply_aero(_logic, temp, press, vap, name="relative humidity", source="monet.util.tools")


def calc_13_category_usda_soil_type(clay: Any, sand: Any, silt: Any) -> Any:
    """Calculate the 13 category USDA soil type from clay, sand and silt percentages.

    Categories:
    0 -- WATER
    1 -- SAND
    2 -- LOAMY SAND
    3 -- SANDY LOAM
    4 -- SILT LOAM
    5 -- SILT
    6 -- LOAM
    7 -- SANDY CLAY LOAM
    8 -- SILTY CLAY LOAM
    9 -- CLAY LOAM
    10 -- SANDY CLAY
    11 -- SILTY CLAY
    12 -- CLAY

    Parameters
    ----------
    clay : float, numpy.ndarray, or xarray.DataArray
        Clay percentage (0-100).
    sand : float, numpy.ndarray, or xarray.DataArray
        Sand percentage (0-100).
    silt : float, numpy.ndarray, or xarray.DataArray
        Silt percentage (0-100).

    Returns
    -------
    stype : same type as input
        USDA soil type category (0-12).
    """

    def _logic(clay, sand, silt):
        stype = np.zeros(clay.shape)
        # 1 -- SAND
        stype[(silt + clay * 1.5 < 15.0) & (clay != 255)] = 1.0
        # 2 -- LOAMY SAND
        stype[(silt + 1.5 * clay >= 15.0) & (silt + 1.5 * clay < 30) & (clay != 255)] = 2.0
        # 3 -- SANDY LOAM
        stype[(clay >= 7.0) & (clay < 20) & (sand > 52) & (silt + 2 * clay >= 30) & (clay != 255)] = 3.0
        stype[(clay < 7) & (silt < 50) & (silt + 2 * clay >= 30) & (clay != 255)] = 3.0
        # 4 -- SILT LOAM
        stype[(silt >= 50) & (clay >= 12) & (clay < 27) & (clay != 255)] = 4.0
        stype[(silt >= 50) & (silt < 80) & (clay < 12) & (clay != 255)] = 4.0
        # 5 -- SILT
        stype[(silt >= 80) & (clay < 12) & (clay != 255)] = 5.0
        # 6 -- LOAM
        stype[(clay >= 7) & (clay < 27) & (silt >= 28) & (silt < 50) & (sand <= 52) & (clay != 255)] = 6.0
        # 7 -- SANDY CLAY LOAM
        stype[(clay >= 20) & (clay < 35) & (silt < 28) & (sand > 45) & (clay != 255)] = 7.0
        # 8 -- SILTY CLAY LOAM
        stype[(clay >= 27) & (clay < 40.0) & (sand > 40) & (clay != 255)] = 8.0
        # 9 -- CLAY LOAM
        stype[(clay >= 27) & (clay < 40.0) & (sand > 20) & (sand <= 45) & (clay != 255)] = 9.0
        # 10 -- SANDY CLAY
        stype[(clay >= 35) & (sand > 45) & (clay != 255)] = 10.0
        # 11 -- SILTY CLAY
        stype[(clay >= 40) & (silt >= 40) & (clay != 255)] = 11.0
        # 12 -- CLAY
        stype[(clay >= 40) & (sand <= 45) & (silt < 40) & (clay != 255)] = 12.0
        return stype

    return _apply_aero(_logic, clay, sand, silt, name="USDA soil type", source="monet.util.tools")


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
    w = df.pivot_table(values="obs", index=["time", "siteid"], columns="variable").reset_index()
    g = df.groupby("variable")
    for name, group in g:
        w[name + "_unit"] = group.units.unique()[0]
    return merge(w, df, on=["siteid", "time"])


def calc_8hr_rolling_max(df: pd.DataFrame, col: str | None = None, window: int | None = None) -> pd.DataFrame:
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
    df_rolling = df.groupby("siteid")[col].rolling(window, center=True, win_type="boxcar").mean().reset_index().dropna()
    df_rolling_max = df_rolling.groupby("siteid").resample("D", on="time_local").max().reset_index(drop=True)
    df = df.reset_index(drop=True)
    return df.merge(df_rolling_max, on=["siteid", "time_local"])


def calc_24hr_ave(df: pd.DataFrame, col: str | None = None) -> pd.DataFrame:
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


def calc_3hr_ave(df: pd.DataFrame, col: str | None = None) -> pd.DataFrame:
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


def calc_annual_ave(df: pd.DataFrame, col: str | None = None) -> pd.DataFrame:
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


def get_giorgi_region_bounds(index: int | None = None, acronym: str | None = None) -> np.ndarray:
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


def _find_region_indices(lon: np.ndarray, lat: np.ndarray, bounds: np.ndarray, indices: np.ndarray) -> np.ndarray:
    """Core logic to find region indices for given lon/lat.

    Supports broadcasting for use with xarray.apply_ufunc.
    """
    # bounds: (N, 4) -> lonmin, latmin, lonmax, latmax
    # lon, lat: (...)
    # Add region dimension to lon/lat for broadcasting
    lon_b = lon[..., np.newaxis]
    lat_b = lat[..., np.newaxis]

    is_inside = (lon_b >= bounds[:, 0]) & (lat_b >= bounds[:, 1]) & (lon_b <= bounds[:, 2]) & (lat_b <= bounds[:, 3])

    any_match = np.any(is_inside, axis=-1)
    region_idx = np.argmax(is_inside, axis=-1)

    out = np.full(lon.shape, np.nan)
    out[any_match] = indices[region_idx[any_match]]
    return out


def _find_region_acronyms(lon: np.ndarray, lat: np.ndarray, bounds: np.ndarray, acronyms: np.ndarray) -> np.ndarray:
    """Core logic to find region acronyms for given lon/lat.

    Supports broadcasting for use with xarray.apply_ufunc.
    """
    lon_b = lon[..., np.newaxis]
    lat_b = lat[..., np.newaxis]

    is_inside = (lon_b >= bounds[:, 0]) & (lat_b >= bounds[:, 1]) & (lon_b <= bounds[:, 2]) & (lat_b <= bounds[:, 3])

    any_match = np.any(is_inside, axis=-1)
    region_idx = np.argmax(is_inside, axis=-1)

    out = np.full(lon.shape, None, dtype=object)
    out[any_match] = acronyms[region_idx[any_match]]
    return out


def get_giorgi_region_df(
    dset: pd.DataFrame | xr.Dataset,
) -> pd.DataFrame | xr.Dataset:
    """Add Giorgi region index and acronym to DataFrame or Dataset.

    This implementation is backend-agnostic and supports Dask-backed
    xarray objects using xarray.apply_ufunc.
    Convention-aware: supports CF/COARDS and UGRID via MONET accessors.

    Parameters
    ----------
    dset : pandas.DataFrame or xarray.Dataset
        DataFrame or Dataset containing latitude and longitude.

    Returns
    -------
    pandas.DataFrame or xarray.Dataset
        Input object with added columns/variables:
        - GIORGI_INDEX: region index number (float, to accommodate NaN)
        - GIORGI_ACRO: region acronym (str)
    """
    bounds = np.array([GIORGI_LONMIN, GIORGI_LATMIN, GIORGI_LONMAX, GIORGI_LATMAX]).T
    indices = np.array(GIORGI_INDICES)
    acronyms = np.array(GIORGI_ACRONYMS)

    lat = dset.monet.lat
    lon = dset.monet.lon

    if lat is None or lon is None:
        raise ValueError("Could not detect latitude and longitude coordinates.")

    if isinstance(dset, pd.DataFrame):
        dset["GIORGI_INDEX"] = _find_region_indices(lon.values, lat.values, bounds, indices)
        dset["GIORGI_ACRO"] = _find_region_acronyms(lon.values, lat.values, bounds, acronyms)
        return dset
    elif isinstance(dset, xr.Dataset):
        lat, lon = xr.broadcast(lat, lon)
        # Use apply_ufunc for Dask compatibility
        idx = _apply_aero(
            _find_region_indices,
            lon,
            lat,
            bounds=bounds,
            indices=indices,
            name="GIORGI region indices",
            source="monet.util.tools",
        )
        acro = _apply_aero(
            _find_region_acronyms,
            lon,
            lat,
            bounds=bounds,
            acronyms=acronyms,
            name="GIORGI region acronyms",
            output_dtypes=[object],
            source="monet.util.tools",
        )
        dset["GIORGI_INDEX"] = idx
        dset["GIORGI_ACRO"] = acro

        return dset
    else:
        raise TypeError("dset must be a pandas.DataFrame or xarray.Dataset")


def add_mask(
    dset: pd.DataFrame | xr.Dataset, mask_name: str, resolution: float = 0.05, new_var: str | None = None
) -> pd.DataFrame | xr.Dataset:
    """Add a mask to a DataFrame or Dataset using the pre-computed mask system.

    Parameters
    ----------
    dset : pandas.DataFrame or xarray.Dataset
        Input object containing latitude and longitude.
    mask_name : str
        Name of the mask (e.g., 'giorgi', 'ipcc_ar6', 'epa_eco', 'timezones', 'epa_admin', 'land').
    resolution : float, default: 0.05
        Resolution of the mask in degrees.
    new_var : str, optional
        Name of the new variable/column to create. Defaults to mask_name.

    Returns
    -------
    pandas.DataFrame or xarray.Dataset
        The input object with the added mask information.
    """
    from .mask import query_mask

    return query_mask(dset, mask_name, resolution=resolution, new_var=new_var)


def get_epa_region_bounds(index: int | None = None, acronym: str | None = None) -> np.ndarray:
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


def get_epa_region_df(
    dset: pd.DataFrame | xr.Dataset,
) -> pd.DataFrame | xr.Dataset:
    """Add EPA region index and acronym to DataFrame or Dataset.

    This implementation is backend-agnostic and supports Dask-backed
    xarray objects using xarray.apply_ufunc.
    Convention-aware: supports CF/COARDS and UGRID via MONET accessors.

    Parameters
    ----------
    dset : pandas.DataFrame or xarray.Dataset
        DataFrame or Dataset containing latitude and longitude.

    Returns
    -------
    pandas.DataFrame or xarray.Dataset
        Input object with added columns/variables:
        - EPA_INDEX: region index number (float, to accommodate NaN)
        - EPA_ACRO: region acronym (str)
    """
    bounds = np.array([EPA_LONMIN, EPA_LATMIN, EPA_LONMAX, EPA_LATMAX]).T
    indices = np.array(EPA_INDICES)
    acronyms = np.array(EPA_ACRONYMS)

    lat = dset.monet.lat
    lon = dset.monet.lon

    if lat is None or lon is None:
        raise ValueError("Could not detect latitude and longitude coordinates.")

    if isinstance(dset, pd.DataFrame):
        dset["EPA_INDEX"] = _find_region_indices(lon.values, lat.values, bounds, indices)
        dset["EPA_ACRO"] = _find_region_acronyms(lon.values, lat.values, bounds, acronyms)
        return dset
    elif isinstance(dset, xr.Dataset):
        lat, lon = xr.broadcast(lat, lon)
        # Use apply_ufunc for Dask compatibility
        idx = _apply_aero(
            _find_region_indices,
            lon,
            lat,
            bounds=bounds,
            indices=indices,
            name="EPA region indices",
            source="monet.util.tools",
        )
        acro = _apply_aero(
            _find_region_acronyms,
            lon,
            lat,
            bounds=bounds,
            acronyms=acronyms,
            name="EPA region acronyms",
            output_dtypes=[object],
            source="monet.util.tools",
        )
        dset["EPA_INDEX"] = idx
        dset["EPA_ACRO"] = acro

        return dset
    else:
        raise TypeError("dset must be a pandas.DataFrame or xarray.Dataset")
