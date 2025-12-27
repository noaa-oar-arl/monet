"""
Error Metrics for Model Evaluation
"""

from typing import Optional, Union

import dask.array as da
import numpy as np
import xarray as xr
from numpy.typing import ArrayLike

from .utils_stats import circlebias, circlebias_m, matchmasks

############################################################
# 1. Basic Error Metrics
############################################################


def STDO(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Standard deviation of Observations

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model values (unused in this metric but kept for consistency).
    axis : int, optional
        Axis along which to compute the standard deviation.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Standard deviation of observations.
    """
    if hasattr(obs, "dims") and isinstance(obs, xr.DataArray):
        return obs.std(dim=obs.dims[axis] if axis is not None else None)
    else:
        return np.std(obs, axis=axis)


def STDP(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Standard deviation of Predictions

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values (unused in this metric but kept for consistency).
    mod : array-like or xarray.DataArray
        Predicted/model values.
    axis : int, optional
        Axis along which to compute the standard deviation.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Standard deviation of predictions.
    """
    if hasattr(mod, "dims") and isinstance(mod, xr.DataArray):
        return mod.std(dim=mod.dims[axis] if axis is not None else None)
    else:
        return np.std(mod, axis=axis)


def MNB(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Mean Normalized Bias (%)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int, optional
        Axis along which to compute the bias.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Mean normalized bias (percent).
    """
    if (
        hasattr(obs, "dims")
        and hasattr(mod, "dims")
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = obs.align(mod, join="inner")
        return ((mod - obs) / obs).mean(
            dim=obs.dims[axis] if axis is not None else None
        ) * 100.0
    else:
        return np.ma.masked_invalid((mod - obs) / obs).mean(axis=axis) * 100.0


def MNE(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Mean Normalized Gross Error (%)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int, optional
        Axis along which to compute the error.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Mean normalized gross error (percent).
    """
    if (
        hasattr(obs, "dims")
        and hasattr(mod, "dims")
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = obs.align(mod, join="inner")
        return (abs(mod - obs) / obs).mean(
            dim=obs.dims[axis] if axis is not None else None
        ) * 100.0
    else:
        return np.ma.masked_invalid(np.ma.abs(mod - obs) / obs).mean(axis=axis) * 100.0


def MdnNB(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Median Normalized Bias (%)

    Typical Use Cases
    -----------------
    - Assessing the central tendency of model bias relative to observations, less sensitive to outliers than mean.
    - Useful for robust model evaluation in the presence of skewed or non-normal error distributions.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int, optional
        Axis along which to compute the median.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Median normalized bias (percent).

    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return ((mod - obs) / obs).median(dim=axis) * 100.0
    else:
        return np.ma.median(np.ma.masked_invalid((mod - obs) / obs), axis=axis) * 100.0


def MdnNE(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Median Normalized Gross Error (%)

    Typical Use Cases
    -----------------
    - Evaluating the typical magnitude of model errors relative to observations, robust to outliers.
    - Useful for summarizing error magnitude in non-Gaussian or heavy-tailed error distributions.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int, optional
        Axis along which to compute the median.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Median normalized gross error (percent).

    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (abs(mod - obs) / obs).median(dim=axis) * 100.0
    else:
        return (
            np.ma.median(np.ma.masked_invalid(np.ma.abs(mod - obs) / obs), axis=axis)
            * 100.0
        )


def NMdnGE(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Normalized Median Gross Error (%)

    Typical Use Cases
    -----------------
    - Comparing the typical (median) error magnitude, normalized by the mean observation, for robust model evaluation.
    - Useful for inter-comparison of model performance across sites or variables with different scales.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int, optional
        Axis along which to compute the mean/median.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Normalized median gross error (percent).

    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (abs(mod - obs).mean(dim=axis) / obs.mean(dim=axis)) * 100.0
    else:
        return (
            np.ma.masked_invalid(
                np.ma.abs(mod - obs).mean(axis=axis) / obs.mean(axis=axis)
            )
            * 100.0
        )


def NO(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[int, np.ndarray, xr.DataArray]:
    """
    N Observations (#)

    Typical Use Cases
    -----------------
    - Counting the number of valid (non-masked) observations in a dataset.
    - Used to report sample size for statistical summaries and model evaluation.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model values (unused in this metric but kept for consistency).
    axis : int, optional
        Axis along which to count.

    Returns
    -------
    int or ndarray or xarray.DataArray
        Count of valid observations.

    """
    if isinstance(obs, xr.DataArray):
        return obs.count(dim=axis)
    else:
        return (~np.ma.getmaskarray(obs)).sum(axis=axis)


def NOP(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[int, np.ndarray, xr.DataArray]:
    """
    N Observations/Prediction Pairs (#)

    Typical Use Cases
    -----------------
    - Counting the number of valid observation-prediction pairs for paired statistical analysis.
    - Used to ensure sample size consistency in paired model evaluation metrics.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int, optional
        Axis along which to count.

    Returns
    -------
    int or ndarray or xarray.DataArray
        Count of valid pairs.

    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return obs.count(dim=axis)
    else:
        obsc, modc = matchmasks(obs, mod)
        return (~np.ma.getmaskarray(obsc)).sum(axis=axis)


def NP(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[int, np.ndarray, xr.DataArray]:
    """
    N Predictions (#)

    Typical Use Cases
    -----------------
    - Counting the number of valid (non-masked) model predictions in a dataset.
    - Used to report sample size for model output and for filtering invalid predictions.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values (unused in this metric but kept for consistency).
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int, optional
        Axis along which to count.

    Returns
    -------
    int or ndarray or xarray.DataArray
        Count of valid predictions.

    """
    if isinstance(mod, xr.DataArray):
        return mod.count(dim=axis)
    else:
        return (~np.ma.getmaskarray(mod)).sum(axis=axis)


def MO(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Mean Observations (obs unit)

    Typical Use Cases
    -----------------
    - Calculating the average value of observed data for baseline or climatological reference.
    - Used in normalization, anomaly calculation, and summary statistics.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model values (unused in this metric but kept for consistency).
    axis : int, optional
        Axis along which to compute the mean.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Mean of observations.

    """
    if isinstance(obs, xr.DataArray):
        return obs.mean(dim=axis)
    elif hasattr(obs, "mean"):
        return obs.mean(axis=axis)
    else:
        return np.mean(obs, axis=axis)


def MP(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Mean Predictions (model unit)

    Typical Use Cases
    -----------------
    - Calculating the average value of model predictions for baseline or climatological reference.
    - Used in normalization, anomaly calculation, and summary statistics for model output.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values (unused in this metric but kept for consistency).
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int, optional
        Axis along which to compute the mean.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Mean of predictions.

    """
    if isinstance(mod, xr.DataArray):
        return mod.mean(dim=axis)
    elif hasattr(mod, "mean"):
        return mod.mean(axis=axis)
    else:
        return np.mean(mod, axis=axis)


def MdnO(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Median Observations (obs unit)

    Typical Use Cases
    -----------------
    - Calculating the median value of observed data, robust to outliers.
    - Used in summary statistics and for non-parametric analyses of observed data.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model values (unused in this metric but kept for consistency).
    axis : int, optional
        Axis along which to compute the median.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Median of observations.

    """
    if isinstance(obs, da.Array):
        if axis is None:
            axis = 0
        return da.median(obs, axis=axis)
    if isinstance(obs, xr.DataArray):
        return obs.median(dim=axis)
    elif hasattr(obs, "median"):
        return obs.median(axis=axis)
    else:
        return np.median(obs, axis=axis)


def MdnP(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Median of Model Predictions (MdnP)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values (unused, for API consistency).
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the median.

    Returns
    -------
    float or ndarray or xarray.DataArray
        Median of model predictions.
    """
    if isinstance(mod, xr.DataArray):
        return mod.median(dim=axis)
    elif hasattr(mod, "median"):
        return np.median(mod, axis=axis)
    else:
        return np.ma.median(mod, axis=axis)


def RM(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray]:
    """
    Mean of Model Predictions (RM)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values (unused, for API consistency).
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the mean.

    Returns
    -------
    float or ndarray
        Mean of model predictions.
    """
    obs = np.asarray(obs)
    mod = np.asarray(mod)
    return np.mean(obs / mod)


def RMdn(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray]:
    """
    Median of Model Predictions (RMdn)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values (unused, for API consistency).
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the median.

    Returns
    -------
    float or ndarray
        Median of model predictions.
    """
    obs = np.asarray(obs)
    mod = np.asarray(mod)
    return np.median(obs / mod)


def MB(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Mean Bias (MB)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the mean bias.

    Returns
    -------
    float or xarray.DataArray
        Mean bias value(s).
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (mod - obs).mean(dim=axis)
    elif hasattr(mod, "mean") and hasattr(obs, "mean"):
        return np.mean(mod - obs, axis=axis)
    else:
        return np.ma.mean(mod - obs, axis=axis)


def MdnB(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Median Bias (MdnB)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the median bias.

    Returns
    -------
    float or xarray.DataArray
        Median bias value(s).
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (mod - obs).median(dim=axis)
    elif hasattr(mod, "median") and hasattr(obs, "median"):
        return np.median(mod - obs, axis=axis)
    else:
        return np.ma.median(mod - obs, axis=axis)


def WDMB_m(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Wind Direction Mean Bias (WDMB, robust version for masked arrays)

    This version uses circlebias_m, which is robust to masked arrays and missing data.
    Use this if your data may contain NaNs or masked values.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed wind direction values (degrees).
    mod : array-like or xarray.DataArray
        Model predicted wind direction values (degrees).
    axis : int or None, optional
        Axis along which to compute the mean bias.

    Returns
    -------
    float or xarray.DataArray
        Mean wind direction bias (degrees).
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return circlebias_m(mod - obs).mean(dim=axis)
    elif isinstance(mod, np.ndarray) and isinstance(obs, np.ndarray):
        return circlebias_m(mod - obs).mean(axis=axis)
    else:
        return np.ma.mean(circlebias_m(mod - obs), axis=axis)


def WDMB(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Wind Direction Mean Bias (WDMB, standard version)

    This version uses circlebias, which is not robust to masked arrays.
    Use this if your data are dense and do not contain missing values.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed wind direction values (degrees).
    mod : array-like or xarray.DataArray
        Model predicted wind direction values (degrees).
    axis : int or None, optional
        Axis along which to compute the mean bias.

    Returns
    -------
    float or xarray.DataArray
        Mean wind direction bias (degrees).
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return circlebias(mod - obs).mean(dim=axis)
    elif isinstance(mod, np.ndarray) and isinstance(obs, np.ndarray):
        return circlebias(mod - obs).mean(axis=axis)
    else:
        return np.ma.mean(circlebias(mod - obs), axis=axis)


def WDMdnB(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Wind Direction Median Bias (WDMdnB)

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed wind direction values (degrees).
    mod : array-like or xarray.DataArray
        Model predicted wind direction values (degrees).
    axis : int or None, optional
        Axis along which to compute the median bias.

    Returns
    -------
    float or xarray.DataArray
        Median wind direction bias (degrees).
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return circlebias(mod - obs).median(dim=axis)
    elif isinstance(mod, np.ndarray) and isinstance(obs, np.ndarray):
        return np.median(circlebias(mod - obs), axis=axis)
    else:
        return np.ma.median(circlebias(mod - obs), axis=axis)


def MAE(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Mean Absolute Error (MAE).

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of errors between model and observations, regardless of direction.
    - Used in model evaluation, forecast verification, and regression analysis.

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed values.
    mod : array_like or xarray.DataArray
        Model or predicted values.
    axis : int, optional
        Axis along which to compute MAE. Default is None (all elements).

    Returns
    -------
    mae : float or ndarray or xarray.DataArray
        Mean absolute error.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.MAE(obs, mod)
    0.6666666666666666
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return abs(mod - obs).mean(dim=axis)
    else:
        return np.ma.abs(mod - obs).mean(axis=axis)


def MedAE(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Median Absolute Error (MedAE).

    Typical Use Cases
    -----------------
    - Evaluating the typical magnitude of errors, robust to outliers and non-normal error distributions.
    - Used in robust regression, model evaluation, and forecast verification.

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed values.
    mod : array_like or xarray.DataArray
        Model or predicted values.
    axis : int, optional
        Axis along which to compute MedAE. Default is None (all elements).

    Returns
    -------
    medae : float or ndarray or xarray.DataArray
        Median absolute error.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.MedAE(obs, mod)
    1.0
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return abs(mod - obs).median(dim=axis)
    else:
        return np.ma.median(np.ma.abs(mod - obs), axis=axis)


def sMAPE(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Symmetric Mean Absolute Percentage Error (sMAPE).

    Typical Use Cases
    -----------------
    - Quantifying the average relative error between model and observations, normalized by their mean.
    - Used in time series forecasting, regression, and model evaluation for percentage-based error assessment.

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed values.
    mod : array_like or xarray.DataArray
        Model or predicted values.
    axis : int, optional
        Axis along which to compute sMAPE. Default is None (all elements).

    Returns
    -------
    smape : float or ndarray or xarray.DataArray
        Symmetric mean absolute percentage error (in percent).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.sMAPE(obs, mod)
    28.57142857142857
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        return (200 * abs(mod - obs) / (abs(mod) + abs(obs))).mean(dim=axis)
    else:
        return (200 * np.ma.abs(mod - obs) / (np.ma.abs(mod) + np.ma.abs(obs))).mean(
            axis=axis
        )


def CRMSE(
    obs: Union[ArrayLike, xr.DataArray],
    mod: Union[ArrayLike, xr.DataArray],
    axis: Optional[int] = None,
) -> Union[float, np.ndarray, xr.DataArray]:
    """
    Centered Root Mean Square Error (CRMSE).

    Typical Use Cases
    -----------------
    - Quantifying the error between anomalies (deviations from mean) of model and observations.
    - Used in Taylor diagrams, model evaluation, and forecast verification.

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed values.
    mod : array_like or xarray.DataArray
        Model or predicted values.
    axis : int, optional
        Axis along which to compute CRMSE. Default is None (all elements).

    Returns
    -------
    crmse : float or ndarray or xarray.DataArray
        Centered root mean square error.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.CRMSE(obs, mod)
    0.4714045207910317
    """
    if isinstance(obs, xr.DataArray) and isinstance(mod, xr.DataArray):
        obs, mod = xr.align(obs, mod, join="inner")
        o_ = obs - obs.mean(dim=axis)
        m_ = mod - mod.mean(dim=axis)
        return ((m_ - o_) ** 2).mean(dim=axis) ** 0.5
    else:
        o_ = obs - obs.mean(axis=axis)
        m_ = mod - mod.mean(axis=axis)
        return (np.ma.abs(m_ - o_) ** 2).mean(axis=axis) ** 0.5
