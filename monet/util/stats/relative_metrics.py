"""
Relative/Percentage Metrics for Model Evaluation
"""

import numpy as np
import xarray as xr

from .utils_stats import circlebias, circlebias_m


def NMB(obs, mod, axis=None):
    """
    Normalized Mean Bias (%)

    Typical Use Cases
    -----------------
    - Comparing model bias across variables or datasets with different units or scales.
    - Common in regulatory and operational air quality model performance reports.

    Parameters
    ----------
    obs : type
        Description of parameter `obs`.
    mod : type
        Description of parameter `mod`.
    axis : type
        Description of parameter `axis`.

    Returns
    -------
    """
    xr = None
    try:
        import xarray as xr
    except ImportError:
        pass
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        return (mod - obs).sum(dim=axis) / obs.sum(dim=axis) * 100.0
    elif hasattr(mod, "sum") and hasattr(obs, "sum"):
        return (mod - obs).sum(axis=axis) / obs.sum(axis=axis) * 100.0
    else:
        return np.sum(mod - obs, axis=axis) / np.sum(obs, axis=axis) * 100.0


def WDNMB_m(obs, mod, axis=None):
    """
    Wind Direction Normalized Mean Bias (%) (avoid single block error in np.ma)

    Typical Use Cases
    -----------------
    - Comparing the average wind direction bias, normalized by observed wind direction, across sites or time periods.
    - Used in wind energy and meteorological model evaluation for directionally normalized performance.

    Parameters
    ----------
    obs : type
        Description of parameter `obs`.
    mod : type
        Description of parameter `mod`.
    axis : type
        Description of parameter `axis`.

    Returns
    -------
    type
        Description of returned object.

    """

    xr = None
    try:
        import xarray as xr
    except ImportError:
        pass
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        return circlebias_m(mod - obs).sum(dim=axis) / obs.sum(dim=axis) * 100.0  # type: ignore
    elif hasattr(mod, "sum") and hasattr(obs, "sum"):
        return circlebias_m(mod - obs).sum(axis=axis) / obs.sum(axis=axis) * 100.0
    else:
        return (
            np.sum(circlebias_m(mod - obs), axis=axis) / np.sum(obs, axis=axis) * 100.0
        )


def NMB_ABS(obs, mod, axis=None):
    """
    Normalized Mean Bias - Absolute of the denominator (%)

    Typical Use Cases
    -----------------
    - Quantifying normalized mean bias when the denominator (sum of observations) may be negative or zero.
    - Used for robust model evaluation in cases with possible sign changes in the observed data sum.

    Parameters
    ----------
    obs : type
        Description of parameter `obs`.
    mod : type
        Description of parameter `mod`.
    axis : type
        Description of parameter `axis`.

    Returns
    -------
    type
        Description of returned object.

    """
    xr = None
    try:
        import xarray as xr
    except ImportError:
        pass
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        return (mod - obs).sum(dim=axis) / abs(obs.sum(dim=axis)) * 100.0
    elif hasattr(mod, "sum") and hasattr(obs, "sum"):
        return (mod - obs).sum(axis=axis) / np.abs(obs.sum(axis=axis)) * 100.0
    else:
        return np.sum(mod - obs, axis=axis) / np.abs(np.sum(obs, axis=axis)) * 100.0


def NMdnB(obs, mod, axis=None):
    """
    Normalized Median Bias (%)

    Typical Use Cases
    -----------------
    - Assessing the central tendency of normalized bias, robust to outliers and non-normal distributions.
    - Used for robust model evaluation across variables or sites with different scales.

    Parameters
    ----------
    obs : type
        Description of parameter `obs`.
    mod : type
        Description of parameter `mod`.
    axis : type
        Description of parameter `axis`.

    Returns
    -------
    type
        Description of returned object.


    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (mod - obs).median(dim=axis) / obs.median(dim=axis) * 100.0
    else:
        return np.ma.median(mod - obs, axis=axis) / np.ma.median(obs, axis=axis) * 100.0


def FB(obs, mod, axis=None):
    """
    Fractional Bias (%)

    Typical Use Cases
    -----------------
    - Quantifying the average bias as a fraction of the sum of model and observed values.
    - Used in air quality and meteorological model evaluation for normalized bias assessment.

    Parameters
    ----------
    obs : type
        Description of parameter `obs`.
    mod : type
        Description of parameter `mod`.
    axis : type
        Description of parameter `axis`.

    Returns
    -------
    type
        Description of returned object.

    """
    xr = None
    try:
        import xarray as xr
    except ImportError:
        pass
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        return (((mod - obs) / (mod + obs)).mean(dim=axis) * 2.0) * 100.0
    elif hasattr(mod, "mean") and hasattr(obs, "mean"):
        return ((mod - obs) / (mod + obs)).mean(axis=axis) * 2.0 * 100.0
    else:
        return (
            np.ma.masked_invalid((mod - obs) / (mod + obs)).mean(axis=axis) * 2.0
        ) * 100.0


def ME(obs, mod, axis=None):
    """
    Mean Gross Error (model and obs unit)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of model errors, regardless of direction.
    - Used in model evaluation to summarize overall error size.

    Parameters
    ----------
    obs : type
        Description of parameter `obs`.
    mod : type
        Description of parameter `mod`.
    axis : type
        Description of parameter `axis`.

    Returns
    -------
    type
        Description of returned object.

    """
    xr = None
    try:
        import xarray as xr
    except ImportError:
        pass
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        return abs(mod - obs).mean(dim=axis)
    elif hasattr(mod, "mean") and hasattr(obs, "mean"):
        return np.abs(mod - obs).mean(axis=axis)
    else:
        return np.mean(np.abs(mod - obs), axis=axis)


def MdnE(obs, mod, axis=None):
    """
    Median Gross Error (model and obs unit)

    Typical Use Cases
    -----------------
    - Evaluating the typical magnitude of model errors, robust to outliers.
    - Used in model evaluation when error distributions are skewed or non-normal.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Median gross error value(s).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.MdnE(obs, mod)
    1.0
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return abs(mod - obs).median(dim=axis)
    else:
        return np.ma.median(np.ma.abs(mod - obs), axis=axis)


def WDME_m(obs, mod, axis=None):
    """
    Wind Direction Mean Gross Error (model and obs unit)
    (avoid single block error in np.ma)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of wind direction errors, regardless of direction.
    - Used in wind energy, meteorology, and air quality studies to assess wind direction model performance.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed wind direction values (degrees).
    mod : array-like or xarray.DataArray
        Model predicted wind direction values (degrees).
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Mean gross error in wind direction (degrees).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([350, 10, 20])
    >>> mod = np.array([10, 20, 30])
    >>> stats.WDME_m(obs, mod)
    20.0
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return abs(circlebias_m(mod - obs)).mean(dim=axis)  # type: ignore
    else:
        return np.abs(circlebias_m(mod - obs)).mean(axis=axis)


def WDME(obs, mod, axis=None):
    """
    Wind Direction Mean Gross Error (model and obs unit)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of wind direction errors, regardless of direction.
    - Used in wind energy, meteorology, and air quality studies to assess wind direction model performance.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed wind direction values (degrees).
    mod : array-like or xarray.DataArray
        Model predicted wind direction values (degrees).
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Mean gross error in wind direction (degrees).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([350, 10, 20])
    >>> mod = np.array([10, 20, 30])
    >>> stats.WDME(obs, mod)
    20.0
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return abs(circlebias(mod - obs)).mean(dim=axis)
    elif isinstance(mod, np.ndarray) and isinstance(obs, np.ndarray):
        return abs(circlebias(mod - obs)).mean(axis=axis)
    else:
        return np.ma.mean(np.ma.abs(circlebias(mod - obs)), axis=axis)


def WDMdnE(obs, mod, axis=None):
    """
    Wind Direction Median Gross Error (model and obs unit)

    Typical Use Cases
    -----------------
    - Evaluating the typical magnitude of wind direction errors, robust to outliers.
    - Used in wind energy and meteorological applications for robust wind direction model evaluation.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed wind direction values (degrees).
    mod : array-like or xarray.DataArray
        Model predicted wind direction values (degrees).
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Median gross error in wind direction (degrees).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([350, 10, 20])
    >>> mod = np.array([10, 20, 30])
    >>> stats.WDMdnE(obs, mod)
    10.0
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        cb = circlebias(mod - obs)
        return abs(cb).median(dim=axis)
    elif isinstance(mod, np.ndarray) and isinstance(obs, np.ndarray):
        cb = circlebias(mod - obs)
        return np.median(np.abs(cb), axis=axis)
    else:
        cb = circlebias(mod - obs)
        return np.ma.median(np.ma.abs(cb), axis=axis)


def NME_m(obs, mod, axis=None):
    """
    Normalized Mean Error (%) (avoid single block error in np.ma)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of model errors relative to observations, robust to masked arrays.
    - Used for model evaluation when data may contain masked or missing values.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Normalized mean error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.NME_m(obs, mod)
    37.5
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (abs(mod - obs).sum(dim=axis) / obs.sum(dim=axis)) * 100
    else:
        out = (np.abs(mod - obs).sum(axis=axis) / obs.sum(axis=axis)) * 100
        return out


def NME_m_ABS(obs, mod, axis=None):
    """
    Normalized Mean Error (%) - Absolute of the denominator
    (avoid single block error in np.ma)

    Typical Use Cases
    -----------------
    - Quantifying normalized mean error when the denominator (sum of observations)
      may be negative or zero, robust to masked arrays.
    - Used for model evaluation with possible sign changes or missing values in observed data.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Normalized mean error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.NME_m_ABS(obs, mod)
    37.5
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (abs(mod - obs).sum(dim=axis) / abs(obs.sum(dim=axis))) * 100
    else:
        out = (np.abs(mod - obs).sum(axis=axis) / np.abs(obs.sum(axis=axis))) * 100
        return out


def NME(obs, mod, axis=None):
    """
    Normalized Mean Error (%)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of model errors relative to observations.
    - Used for model evaluation and comparison across variables or datasets with different scales.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Normalized mean error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.NME(obs, mod)
    37.5
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (abs(mod - obs).sum(dim=axis) / obs.sum(dim=axis)) * 100
    else:
        out = (np.ma.abs(mod - obs).sum(axis=axis) / obs.sum(axis=axis)) * 100
        return out


def NMdnE(obs, mod, axis=None):
    """
    Normalized Median Error (%)

    Typical Use Cases
    -----------------
    - Evaluating the typical magnitude of model errors relative to observations, robust to outliers.
    - Used for robust model evaluation and comparison across variables or datasets with different scales.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Normalized median error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.NMdnE(obs, mod)
    33.33333333333333
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return abs(mod - obs).median(dim=axis) / obs.median(dim=axis) * 100
    else:
        out = (
            np.ma.median(np.ma.abs(mod - obs), axis=axis)
            / np.ma.median(obs, axis=axis)
            * 100
        )
        return out


def FE(obs, mod, axis=None):
    """
    Fractional Error (%)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of model errors as a fraction of the sum of model and observed values.
    - Used in air quality and meteorological model evaluation for normalized error assessment.

    Parameters
    ----------
    obs : type
        Description of parameter `obs`.
    mod : type
        Description of parameter `mod`.
    axis : type
        Description of parameter `axis`.

    Returns
    -------
    type
        Description of returned object.

    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (abs(mod - obs) / (mod + obs)).mean(dim=axis) * 2.0 * 100.0
    elif isinstance(mod, np.ndarray) and isinstance(obs, np.ndarray):
        return (np.abs(mod - obs) / (mod + obs)).mean(axis=axis) * 2.0 * 100.0
    else:
        return (np.ma.mean(np.ma.abs(mod - obs) / (mod + obs), axis=axis)) * 2.0 * 100.0


def USUTPB(obs, mod, axis=None):
    """
    Unpaired Space/Unpaired Time Peak Bias (%)

    Typical Use Cases
    -----------------
    - Assessing the bias in peak values between model and observations, regardless of spatial or temporal pairing.
    - Used in event-based or extreme value model evaluation, especially for air quality and meteorological extremes.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Peak bias (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 5])
    >>> stats.USUTPB(obs, mod)
    25.0
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return ((mod.max(dim=axis) - obs.max(dim=axis)) / obs.max(dim=axis)) * 100.0
    elif isinstance(mod, np.ndarray) and isinstance(obs, np.ndarray):
        return ((mod.max(axis=axis) - obs.max(axis=axis)) / obs.max(axis=axis)) * 100.0
    else:
        return (
            (np.ma.max(mod, axis=axis) - np.ma.max(obs, axis=axis))
            / np.ma.max(obs, axis=axis)
        ) * 100.0


def USUTPE(obs, mod, axis=None):
    """
    Unpaired Space/Unpaired Time Peak Error (%)

    Typical Use Cases
    -----------------
    - Quantifying the error in peak values between model and observations, regardless of spatial or temporal pairing.
    - Used in event-based or extreme value model evaluation, especially for air quality and meteorological extremes.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or xarray.DataArray
        Peak error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 5])
    >>> stats.USUTPE(obs, mod)
    25.0
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (abs(mod.max(dim=axis) - obs.max(dim=axis)) / obs.max(dim=axis)) * 100.0
    elif isinstance(mod, np.ndarray) and isinstance(obs, np.ndarray):
        return (
            np.abs(mod.max(axis=axis) - obs.max(axis=axis)) / obs.max(axis=axis)
        ) * 100.0
    else:
        return (
            np.ma.abs(np.ma.max(mod, axis=axis) - np.ma.max(obs, axis=axis))
            / np.ma.max(obs, axis=axis)
        ) * 100.0


def MNPB(obs, mod, paxis, axis=None):
    """
    Mean Normalized Peak Bias (%)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    paxis : int
        Axis along which to compute the peak (e.g., time or space).
    axis : int or None, optional
        Axis along which to compute the mean of normalized peak bias.

    Returns
    -------
    float or xarray.DataArray
        Mean normalized peak bias (percent).
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (
            ((mod.max(dim=paxis) - obs.max(dim=paxis)) / obs.max(dim=paxis)).mean(
                dim=axis
            )
        ) * 100.0
    else:
        return (
            (np.ma.max(mod, axis=paxis) - np.ma.max(obs, axis=paxis))
            / np.ma.max(obs, axis=paxis)
        ).mean(axis=axis) * 100.0


def MdnNPB(obs, mod, paxis, axis=None):
    """
    Median Normalized Peak Bias (%)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    paxis : int
        Axis along which to compute the peak (e.g., time or space).
    axis : int or None, optional
        Axis along which to compute the median of normalized peak bias.

    Returns
    -------
    float or xarray.DataArray
        Median normalized peak bias (percent).
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return ((mod.max(dim=paxis) - obs.max(dim=paxis)) / obs.max(dim=paxis)).median(
            dim=axis
        ) * 100.0
    else:
        return (
            np.ma.median(
                (
                    (np.ma.max(mod, axis=paxis) - np.ma.max(obs, axis=paxis))
                    / np.ma.max(obs, axis=paxis)
                ),
                axis=axis,
            )
            * 100.0
        )


def MNPE(obs, mod, paxis, axis=None):
    """
    Mean Normalized Peak Error (MNPE, %)

    Typical Use Cases
    -----------------
    - Quantifying the average error in peak values between model and observations, normalized by observed peaks.
    - Used in model evaluation for extreme events, such as air quality exceedances or meteorological extremes.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    paxis : int
        Axis along which to compute the peak (e.g., time or space).
    axis : int or None, optional
        Axis along which to compute the mean of normalized peak error.

    Returns
    -------
    float or xarray.DataArray
        Mean normalized peak error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([[1, 2, 3], [2, 3, 4]])
    >>> mod = np.array([[2, 2, 2], [2, 2, 5]])
    >>> stats.MNPE(obs, mod, paxis=1)
    33.33333333333333
    """

    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (abs(mod.max(dim=paxis) - obs.max(dim=paxis)) / obs.max(dim=paxis)).mean(
            dim=axis
        ) * 100.0
    else:
        return (
            np.ma.abs(np.ma.max(mod, axis=paxis) - np.ma.max(obs, axis=paxis))
            / np.ma.max(obs, axis=paxis)
        ).mean(axis=axis) * 100.0


def MdnNPE(obs, mod, paxis, axis=None):
    """
    Median Normalized Peak Error (MdnNPE, %)

    Typical Use Cases
    -----------------
    - Evaluating the typical error in peak values between model and observations,
      normalized by observed peaks, robust to outliers.
    - Used in robust model evaluation for extreme events, such as air quality exceedances
      or meteorological extremes.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    paxis : int
        Axis along which to compute the peak (e.g., time or space).
    axis : int or None, optional
        Axis along which to compute the median of normalized peak error.

    Returns
    -------
    float or xarray.DataArray
        Median normalized peak error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([[1, 2, 3], [2, 3, 4]])
    >>> mod = np.array([[2, 2, 2], [2, 2, 5]])
    >>> stats.MdnNPE(obs, mod, paxis=1)
    33.33333333333333
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (
            abs(mod.max(dim=paxis) - obs.max(dim=paxis)) / obs.max(dim=paxis)
        ).median(dim=axis) * 100.0
    else:
        return (
            np.ma.median(
                (
                    np.ma.abs(np.ma.max(mod, axis=paxis) - np.ma.max(obs, axis=paxis))
                    / np.ma.max(obs, axis=paxis)
                ),
                axis=axis,
            )
            * 100.0
        )


def NMPB(obs, mod, paxis, axis=None):
    """
    Normalized Mean Peak Bias (NMPB, %)

    Typical Use Cases
    -----------------
    - Quantifying the average bias in peak values, normalized by the mean of observed peaks.
    - Used in model evaluation for extreme events, especially when comparing across sites or time periods.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    paxis : int
        Axis along which to compute the peak (e.g., time or space).
    axis : int or None, optional
        Axis along which to compute the mean of normalized peak bias.

    Returns
    -------
    float or xarray.DataArray
        Normalized mean peak bias (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([[1, 2, 3], [2, 3, 4]])
    >>> mod = np.array([[2, 2, 2], [2, 2, 5]])
    >>> stats.NMPB(obs, mod, paxis=1)
    33.33333333333333
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (
            (mod.max(dim=paxis) - obs.max(dim=paxis)).mean(dim=axis)
            / obs.max(dim=paxis).mean(dim=axis)
        ) * 100.0
    else:
        return (
            (np.ma.max(mod, axis=paxis) - np.ma.max(obs, axis=paxis)).mean(axis=axis)
            / np.ma.max(obs, axis=paxis).mean(axis=axis)
        ) * 100.0


def NMdnPB(obs, mod, paxis, axis=None):
    """
    Normalized Median Peak Bias (NMdnPB, %)

    Typical Use Cases
    -----------------
    - Evaluating the typical bias in peak values, normalized by the median of observed peaks, robust to outliers.
    - Used in robust model evaluation for extreme events, especially when comparing across sites or time periods.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    paxis : int
        Axis along which to compute the peak (e.g., time or space).
    axis : int or None, optional
        Axis along which to compute the median of normalized peak bias.

    Returns
    -------
    float or xarray.DataArray
        Normalized median peak bias (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([[1, 2, 3], [2, 3, 4]])
    >>> mod = np.array([[2, 2, 2], [2, 2, 5]])
    >>> stats.NMdnPB(obs, mod, paxis=1)
    33.33333333333333
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (
            (mod.max(dim=paxis) - obs.max(dim=paxis)).median(dim=axis)
            / obs.max(dim=paxis).median(dim=axis)
            * 100.0
        )
    else:
        return (
            np.ma.median(
                np.ma.max(mod, axis=paxis) - np.ma.max(obs, axis=paxis), axis=axis
            )
            / np.ma.median(np.ma.max(obs, axis=paxis), axis=axis)
        ) * 100.0


def NMPE(obs, mod, paxis, axis=None):
    """
    Normalized Mean Peak Error (NMPE, %)

    Typical Use Cases
    -----------------
    - Quantifying the average error in peak values, normalized by the mean of observed peaks.
    - Used in model evaluation for extreme events, especially when comparing across sites or time periods.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    paxis : int
        Axis along which to compute the peak (e.g., time or space).
    axis : int or None, optional
        Axis along which to compute the mean of normalized peak error.

    Returns
    -------
    float or xarray.DataArray
        Normalized mean peak error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([[1, 2, 3], [2, 3, 4]])
    >>> mod = np.array([[2, 2, 2], [2, 2, 5]])
    >>> stats.NMPE(obs, mod, paxis=1)
    33.33333333333333
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (
            abs(mod.max(dim=paxis) - obs.max(dim=paxis)).mean(dim=axis)
            / obs.max(dim=paxis).mean(dim=axis)
        ) * 100.0
    else:
        return (
            np.ma.abs(np.ma.max(mod, axis=paxis) - np.ma.max(obs, axis=paxis)).mean(
                axis=axis
            )
            / np.ma.max(obs, axis=paxis).mean(axis=axis)
        ) * 100.0


def NMdnPE(obs, mod, paxis, axis=None):
    """
    Normalized Median Peak Error (NMdnPE, %)

    Typical Use Cases
    -----------------
    - Evaluating the typical error in peak values, normalized by the median of observed peaks, robust to outliers.
    - Used in robust model evaluation for extreme events, especially when comparing across sites or time periods.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    paxis : int
        Axis along which to compute the peak (e.g., time or space).
    axis : int or None, optional
        Axis along which to compute the median of normalized peak error.

    Returns
    -------
    float or xarray.DataArray
        Normalized median peak error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([[1, 2, 3], [2, 3, 4]])
    >>> mod = np.array([[2, 2, 2], [2, 2, 5]])
    >>> stats.NMdnPE(obs, mod, paxis=1)
    33.33333333333333
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (
            (abs(mod.max(dim=paxis) - obs.max(dim=paxis))).median(dim=axis)
            / obs.max(dim=paxis).median(dim=axis)
            * 100.0
        )
    else:
        return (
            np.ma.median(
                np.ma.abs(np.ma.max(mod, axis=paxis) - np.ma.max(obs, axis=paxis)),
                axis=axis,
            )
            / np.ma.median(np.ma.max(obs, axis=paxis), axis=axis)
        ) * 100.0


def PSUTMNPB(obs, mod, axis=None):
    """
    Paired Space/Unpaired Time Mean Normalized Peak Bias (PSUTMNPB, %)

    Wrapper for MNPB with paxis=0, axis=None.
    """

    return MNPB(obs, mod, paxis=0, axis=None)


def PSUTMdnNPB(obs, mod, axis=None):
    """
    Paired Space/Unpaired Time Median Normalized Peak Bias (PSUTMdnNPB, %)

    Wrapper for MdnNPB with paxis=0, axis=None.
    """

    return MdnNPB(obs, mod, paxis=0, axis=None)


def PSUTMNPE(obs, mod, axis=None):
    """
    Paired Space/Unpaired Time Mean Normalized Peak Error (PSUTMNPE, %)

    Wrapper for MNPE with paxis=0, axis=None.
    """

    return MNPE(obs, mod, paxis=0, axis=None)


def PSUTMdnNPE(obs, mod, axis=None):
    """
    Paired Space/Unpaired Time Median Normalized Peak Error (PSUTMdnNPE, %)

    Wrapper for MdnNPE with paxis=0, axis=None.
    """

    return MdnNPE(obs, mod, paxis=0, axis=None)


def PSUTNMPB(obs, mod, axis=None):
    """
    Paired Space/Unpaired Time Normalized Mean Peak Bias (PSUTNMPB, %)

    Wrapper for NMPB with paxis=0, axis=None.
    """
    return NMPB(obs, mod, paxis=0, axis=None)


def PSUTNMPE(obs, mod, axis=None):
    """
    Paired Space/Unpaired Time Normalized Mean Peak Error (PSUTNMPE, %)

    Wrapper for NMPE with paxis=0, axis=None.
    """
    return NMPE(obs, mod, paxis=0, axis=None)


def PSUTNMdnPB(obs, mod, axis=None):
    """
    Paired Space/Unpaired Time Normalized Median Peak Bias (PSUTNMdnPB, %)

    Typical Use Cases
    -----------------
    - Evaluating the normalized median peak bias for spatially paired, temporally unpaired datasets, robust to outliers.
    - Used in robust model evaluation for spatial ensemble or multi-time analysis.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the median of normalized peak bias.

    Returns
    -------
    float or xarray.DataArray
        Normalized median peak bias (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([[1, 2, 3], [2, 3, 4]])
    >>> mod = np.array([[2, 2, 2], [2, 2, 5]])
    >>> stats.PSUTNMdnPB(obs, mod)
    33.33333333333333
    """
    return NMdnPB(obs, mod, paxis=0, axis=None)


def PSUTNMdnPE(obs, mod, axis=None):
    """
    Paired Space/Unpaired Time Normalized Median Peak Error (PSUTNMdnPE, %)

    Typical Use Cases
    -----------------
    - Evaluating the normalized median peak error for spatially paired, temporally unpaired
      datasets, robust to outliers.
    - Used in robust model evaluation for spatial ensemble or multi-time analysis.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the median of normalized peak error.

    Returns
    -------
    float or xarray.DataArray
        Normalized median peak error (percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([[1, 2, 3], [2, 3, 4]])
    >>> mod = np.array([[2, 2, 2], [2, 2, 5]])
    >>> stats.PSUTNMdnPE(obs, mod)
    33.33333333333333
    """
    return NMdnPE(obs, mod, paxis=0, axis=None)
