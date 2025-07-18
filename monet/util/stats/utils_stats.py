"""
Utility Functions for Statistics
"""

import numpy as np


def matchedcompressed(a1, a2):
    """
    Return compressed (non-masked) values from two masked arrays with matched masks.

    Typical Use Cases
    -----------------
    - Ensuring paired, valid (non-masked) values for statistical calculations (e.g., correlation, regression).
    - Used in metrics that require both arrays to have valid data at the same locations.

    Parameters
    ----------
    a1 : array-like or numpy.ma.MaskedArray
        First input array.
    a2 : array-like or numpy.ma.MaskedArray
        Second input array.

    Returns
    -------
    tuple of ndarray
        Tuple of (a1_compressed, a2_compressed), both 1D arrays of valid values.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> a1 = np.ma.array([1, 2, 3], mask=[0, 1, 0])
    >>> a2 = np.ma.array([4, 5, 6], mask=[0, 0, 1])
    >>> stats.matchedcompressed(a1, a2)
    (array([1]), array([4]))
    """
    a1, a2 = matchmasks(a1, a2)
    return a1.compressed(), a2.compressed()


def matchmasks(a1, a2):
    """
    Match and combine masks from two masked arrays.

    Typical Use Cases
    -----------------
    - Ensuring that two arrays have the same mask for paired statistical calculations.
    - Used in metrics that require both arrays to have valid data at the same locations (e.g., correlation, regression).

    Parameters
    ----------
    a1 : array-like or numpy.ma.MaskedArray
        First input array.
    a2 : array-like or numpy.ma.MaskedArray
        Second input array.

    Returns
    -------
    tuple of numpy.ma.MaskedArray
        Tuple of (a1_masked, a2_masked) with combined mask.

    Examples
    --------
    >>> import numpy as np
    >>> a1 = np.ma.array([1, 2, 3], mask=[0, 1, 0])
    >>> a2 = np.ma.array([4, 5, 6], mask=[0, 0, 1])
    >>> matchmasks(a1, a2)
    (masked_array(data=[1, --, 3], mask=[False,  True, False]),
     masked_array(data=[4, --, --], mask=[False, False,  True]))
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None

    if xr is not None and isinstance(a1, xr.DataArray) and isinstance(a2, xr.DataArray):
        # Align xarray objects (works for dask-backed as well)
        a1a, a2a = xr.align(a1, a2, join="inner")
        return a1a, a2a
    else:
        mask = np.ma.getmaskarray(a1) | np.ma.getmaskarray(a2)
        return np.ma.masked_where(mask, a1), np.ma.masked_where(mask, a2)


def circlebias_m(b):
    """
    Circular bias for wind direction (avoid single block error in np.ma).

    Typical Use Cases
    -----------------
    - Calculating the signed difference between two wind directions, accounting for circularity,
      robust to masked arrays.
    - Used in wind direction bias and error metrics for masked or missing data.

    Parameters
    ----------
    b : array-like or numpy.ma.MaskedArray
        Difference between two wind directions (degrees).

    Returns
    -------
    array-like or numpy.ma.MaskedArray
        Circularly wrapped difference (degrees).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> stats.circlebias_m(np.array([190, -190, 10, -10]))
    array([-170, 170,  10, -10])
    """
    b = np.asarray(b)
    out = (b + 180) % 360 - 180
    if np.ma.isMaskedArray(b):
        out = np.ma.array(out, mask=np.ma.getmaskarray(b))
    return out


def circlebias(b):
    """
    Circular bias (wind direction difference, wrapped to [-180, 180] degrees).

    Typical Use Cases
    -----------------
    - Calculating the signed difference between two wind directions, accounting for circularity.
    - Used in wind direction bias and error metrics to avoid artificial large errors across 0/360 boundaries.

    Parameters
    ----------
    b : array-like
        Difference between two wind directions (degrees).

    Returns
    -------
    array-like
        Circularly wrapped difference (degrees).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> stats.circlebias(np.array([190, -190, 10, -10]))
    array([-170, 170,  10, -10])
    """
    b = np.asarray(b)
    return (b + 180) % 360 - 180
