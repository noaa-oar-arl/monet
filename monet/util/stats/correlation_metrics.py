"""
Correlation and Agreement Metrics for Model Evaluation
"""

import numpy as np

from .utils_stats import circlebias, circlebias_m, matchedcompressed


def R2(obs, mod, axis=None):
    """
    Coefficient of Determination (R^2, unitless)

    Typical Use Cases
    -----------------
    - Quantifying how well model predictions explain the variance in observations.
    - Used in regression analysis, model skill assessment, and forecast verification.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic. Only None is supported.

    Returns
    -------
    float
        Coefficient of determination (R^2).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.R2(obs, mod)
    0.0
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    from scipy.stats import pearsonr

    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        if axis is None:
            axis = -1
        if isinstance(axis, int):
            dim = obs.dims[axis]
        else:
            dim = axis

        def _pearsonr2(a, b):
            r_val = pearsonr(a, b)
            # Always extract first element if tuple
            if isinstance(r_val, tuple):
                r = r_val[0]
            else:
                r = r_val
            if isinstance(r, tuple):
                raise TypeError(f"pearsonr returned a tuple unexpectedly: {r}")
            # If r is a numpy scalar, convert to float
            import numpy as np

            if isinstance(r, np.generic):
                r = r.item()
            if not isinstance(r, (float, int)):
                raise TypeError(f"pearsonr returned non-numeric type: {type(r)}")
            return float(r) ** 2

        r2 = xr.apply_ufunc(
            _pearsonr2,
            obs,
            mod,
            input_core_dims=[[dim], [dim]],
            output_core_dims=[[]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float],
        )
        return r2
    elif axis is None:
        obsc, modc = matchedcompressed(obs, mod)
        r_val = pearsonr(obsc, modc)
        if isinstance(r_val, tuple):
            r = r_val[0]
        else:
            r = r_val
        if isinstance(r, tuple):
            raise TypeError(f"pearsonr returned a tuple unexpectedly: {r}")
        import numpy as np

        if isinstance(r, np.generic):
            r = r.item()
        if not isinstance(r, (float, int)):
            raise TypeError(f"pearsonr returned non-numeric type: {type(r)}")
        return float(r) ** 2
    else:
        raise ValueError("Not ready yet")


def RMSE(obs, mod, axis=None):
    """
    Root Mean Square Error (RMSE, model unit)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of errors between model and observations.
    - Used in model evaluation, forecast verification, and regression analysis.

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
        Root mean square error value(s).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.RMSE(obs, mod)
    0.7071067811865476
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        return ((mod - obs) ** 2).mean(dim=axis) ** 0.5
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        return np.sqrt(np.mean((mod - obs) ** 2, axis=axis))
    else:
        return np.ma.sqrt(np.ma.mean((mod - obs) ** 2, axis=axis))


def WDRMSE_m(obs, mod, axis=None):
    """
    Wind Direction Root Mean Square Error (WDRMSE, model unit)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of wind direction errors, accounting for circularity, robust to masked arrays.
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
        Wind direction root mean square error (degrees).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([350, 10, 20])
    >>> mod = np.array([10, 20, 30])
    >>> stats.WDRMSE_m(obs, mod)
    20.0
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        arr = (circlebias_m(mod - obs)) ** 2
        if axis is None:
            return arr.mean() ** 0.5
        if isinstance(arr, xr.DataArray):
            if isinstance(axis, int):
                dim = arr.dims[axis]
            elif isinstance(axis, str):
                dim = axis
            else:
                raise ValueError("axis must be int or str for xarray.DataArray")
            if not isinstance(dim, (str, list, tuple)):
                raise TypeError(
                    "dim must be a string, list, or tuple for xarray.DataArray.mean"
                )
            return arr.mean(dim=dim) ** 0.5
        else:
            return arr.mean(axis=axis) ** 0.5
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        return np.sqrt(np.mean((circlebias_m(mod - obs)) ** 2, axis=axis))
    else:
        return np.ma.sqrt(np.ma.mean((circlebias_m(mod - obs)) ** 2, axis=axis))


def WDRMSE(obs, mod, axis=None):
    """
    Wind Direction Root Mean Square Error (WDRMSE, model unit)

    Typical Use Cases
    -----------------
    - Quantifying the average magnitude of wind direction errors, accounting for circularity.
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
        Wind direction root mean square error (degrees).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([350, 10, 20])
    >>> mod = np.array([10, 20, 30])
    >>> stats.WDRMSE(obs, mod)
    20.0
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        arr = (circlebias(mod - obs)) ** 2
        if axis is None:
            return arr.mean() ** 0.5
        if isinstance(arr, xr.DataArray):
            if isinstance(axis, int):
                dim = arr.dims[axis]
            elif isinstance(axis, str):
                dim = axis
            else:
                raise ValueError("axis must be int or str for xarray.DataArray")
            # Only allow str or list of str for dim
            if isinstance(dim, str):
                pass
            elif isinstance(dim, (tuple, list)):
                dim = [str(d) for d in dim]
                if not all(isinstance(d, str) for d in dim):
                    raise TypeError(
                        "All elements of dim must be str for xarray.DataArray.mean"
                    )
            else:
                raise TypeError(
                    "dim must be a string or list of strings for xarray.DataArray.mean"
                )
            if not (
                isinstance(dim, str)
                or (isinstance(dim, list) and all(isinstance(d, str) for d in dim))
            ):
                raise TypeError(
                    "dim must be a string or list of strings for xarray.DataArray.mean (final check)"
                )
            return arr.mean(dim=dim) ** 0.5  # type: ignore
        else:
            return arr.mean(axis=axis) ** 0.5
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        return np.sqrt(np.mean((circlebias(mod - obs)) ** 2, axis=axis))
    else:
        return np.ma.sqrt(np.ma.mean((circlebias(mod - obs)) ** 2, axis=axis))


def RMSEs(obs, mod, axis=None):
    """
    Root Mean Squared Error between observations and regression fit (RMSEs, model unit)

    Typical Use Cases
    -----------------
    - Quantifying the error between observations and a regression fit to the model predictions.
    - Used in model evaluation to assess how well a regression fit to the model matches the observations.

    Parameters
    ----------
    obs : array-like or xarray.DataArray
        Observed values.
    mod : array-like or xarray.DataArray
        Model predicted values.
    axis : int or None, optional
        Axis along which to compute the statistic. Only None is supported.

    Returns
    -------
    float or None
        Root mean squared error value(s), or None if regression fails.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.RMSEs(obs, mod)
    0.7071067811865476
    """
    if axis is None:
        try:
            from scipy.stats import linregress

            obsc, modc = matchedcompressed(obs, mod)
            m, b, rval, pval, stderr = linregress(obsc, modc)
            mod_hat = b + m * obs
            return RMSE(obs, mod_hat)
        except ValueError:
            return None
    else:
        raise ValueError("Not ready yet")


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
    >>> from monet.util import stats
    >>> a1 = np.ma.array([1, 2, 3], mask=[0, 1, 0])
    >>> a2 = np.ma.array([4, 5, 6], mask=[0, 0, 1])
    >>> stats.matchmasks(a1, a2)
    (masked_array(data=[1, --, 3], mask=[False,  True, False]),
     masked_array(data=[4, --, --], mask=[False, False,  True]))
    """
    mask = np.ma.getmaskarray(a1) | np.ma.getmaskarray(a2)
    return np.ma.masked_where(mask, a1), np.ma.masked_where(mask, a2)


def RMSEu(obs, mod, axis=None):
    """
    Root Mean Squared Error between regression fit (mod_hat) and model (mod).

    Typical Use Cases
    -----------------
    - Quantifying the error between a linear regression fit to observations and the model predictions.
    - Used in model evaluation to assess how well a regression fit to obs matches the model output.

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
    float or xarray.DataArray or None
        Root mean squared error value(s), or None if regression fails.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.RMSEu(obs, mod)
    0.7071067811865476
    """
    if axis is None:
        try:
            from scipy.stats import linregress

            obsc, modc = matchedcompressed(obs, mod)
            m, b, rval, pval, stderr = linregress(obsc, modc)
            mod_hat = b + m * obs
            return RMSE(mod_hat, mod)
        except ValueError:
            return None
    else:
        raise ValueError("Not ready yet")


def d1(obs, mod, axis=None):
    """
    Modified Index of Agreement (d1).

    Typical Use Cases
    -----------------
    - Quantifying the agreement between model and observations, less sensitive to outliers than IOA.
    - Used in model evaluation for robust skill assessment.

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
        Modified index of agreement (unitless, 0-1).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.d1(obs, mod)
    0.5
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
        num = abs(obs - mod).sum(dim=axis)
        mean_obs = obs.mean(dim=axis)
        denom = (abs(mod - mean_obs) + abs(obs - mean_obs)).sum(dim=axis)
        return 1.0 - (num / denom)
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        num = np.abs(obs - mod).sum(axis=axis)
        mean_obs = obs.mean(axis=axis)
        denom = (np.abs(mod - mean_obs) + np.abs(obs - mean_obs)).sum(axis=axis)
        return 1.0 - (num / denom)
    else:
        num = np.ma.abs(obs - mod).sum(axis=axis)
        mean_obs = obs.mean(axis=axis)
        denom = (np.ma.abs(mod - mean_obs) + np.ma.abs(obs - mean_obs)).sum(axis=axis)
        return 1.0 - (num / denom)


def E1(obs, mod, axis=None):
    """
    Modified Coefficient of Efficiency (E1).

    Typical Use Cases
    -----------------
    - Quantifying the efficiency of model predictions relative to observed mean, robust to outliers.
    - Used in hydrology, meteorology, and model skill assessment.

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
        Modified coefficient of efficiency (unitless, -inf to 1).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.E1(obs, mod)
    0.0
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
        num = abs(obs - mod).sum(dim=axis)
        denom = abs(obs - obs.mean(dim=axis)).sum(dim=axis)
        return 1.0 - (num / denom)
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        num = np.abs(obs - mod).sum(axis=axis)
        mean_obs = obs.mean(axis=axis)
        denom = np.abs(obs - mean_obs).sum(axis=axis)
        return 1.0 - (num / denom)
    else:
        num = np.ma.abs(obs - mod).sum(axis=axis)
        mean_obs = obs.mean(axis=axis)
        denom = np.ma.abs(obs - mean_obs).sum(axis=axis)
        return 1.0 - (num / denom)


def IOA_m(obs, mod, axis=None):
    """
    Index of Agreement (IOA), avoid single block error in np.ma.

    Typical Use Cases
    -----------------
    - Quantifying the agreement between model and observations, normalized by total deviation.
    - Used in model evaluation for skill assessment, robust to masked arrays.

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
        Index of agreement (unitless, 0-1).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.IOA_m(obs, mod)
    0.8
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
        obsmean = obs.mean(dim=axis)
        num = ((obs - mod) ** 2).sum(dim=axis)
        denom = ((abs(mod - obsmean) + abs(obs - obsmean)) ** 2).sum(dim=axis)
        return 1.0 - (num / denom)
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        obsmean = obs.mean(axis=axis)
        num = (np.abs(obs - mod) ** 2).sum(axis=axis)
        denom = ((np.abs(mod - obsmean) + np.abs(obs - obsmean)) ** 2).sum(axis=axis)
        return 1.0 - (num / denom)
    else:
        obsmean = obs.mean(axis=axis)
        num = (np.ma.abs(obs - mod) ** 2).sum(axis=axis)
        denom = ((np.ma.abs(mod - obsmean) + np.ma.abs(obs - obsmean)) ** 2).sum(
            axis=axis
        )
        return 1.0 - (num / denom)


def IOA(obs, mod, axis=None):
    """
    Index of Agreement (IOA).

    Typical Use Cases
    -----------------
    - Quantifying the agreement between model and observations, normalized by total deviation.
    - Used in model evaluation for skill assessment.

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
        Index of agreement (unitless, 0-1).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.IOA(obs, mod)
    0.8
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
        obsmean = obs.mean(dim=axis)
        num = ((obs - mod) ** 2).sum(dim=axis)
        denom = ((abs(mod - obsmean) + abs(obs - obsmean)) ** 2).sum(dim=axis)
        return 1.0 - (num / denom)
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        obsmean = obs.mean(axis=axis)
        num = (np.abs(obs - mod) ** 2).sum(axis=axis)
        denom = ((np.abs(mod - obsmean) + np.abs(obs - obsmean)) ** 2).sum(axis=axis)
        return 1.0 - (num / denom)
    else:
        obsmean = obs.mean(axis=axis)
        num = (np.ma.abs(obs - mod) ** 2).sum(axis=axis)
        denom = ((np.ma.abs(mod - obsmean) + np.ma.abs(obs - obsmean)) ** 2).sum(
            axis=axis
        )
        return 1.0 - (num / denom)


def WDIOA_m(obs, mod, axis=None):
    """
    Wind Direction Index of Agreement (WDIOA_m)

    Parameters
    ----------
    obs : array-like
        Observed wind direction values (degrees).
    mod : array-like
        Modeled wind direction values (degrees).
    axis : int, optional
        Axis along which to compute the metric. Only None (scalar output) is supported.

    Returns
    -------
    float or None
        WDIOA_m value or None if computation fails.
    """
    # obsmean = obs.mean(axis=axis)  # unused
    if axis is None:
        try:
            from scipy.stats import linregress

            obsc, modc = matchedcompressed(obs, mod)
            m, b, rval, pval, stderr = linregress(modc, obsc)
            mod_hat = b + m * mod
            return RMSE(mod_hat, mod)
        except ValueError:
            return None
    else:
        raise ValueError("RMSEu only supports axis=None (scalar output)")


def WDIOA(obs, mod, axis=None):
    """
    Wind Direction Index of Agreement (WDIOA)

    Parameters
    ----------
    obs : array-like
        Observed wind direction values (degrees).
    mod : array-like
        Modeled wind direction values (degrees).
    axis : int, optional
        Axis along which to compute the metric. Default is 0.

    Returns
    -------
    float or ndarray
        WDIOA value(s)
    """
    import numpy as np

    try:
        import xarray as xr
    except ImportError:
        xr = None
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        num = abs(obs - mod).sum(dim=axis)
        mean_obs = obs.mean(dim=axis)
        denom = (abs(mod - mean_obs) + abs(obs - mean_obs)).sum(dim=axis)
        return 1.0 - (num / denom)
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        num = np.abs(obs - mod).sum(axis=axis)
        mean_obs = np.mean(obs, axis=axis)
        denom = (np.abs(mod - mean_obs) + np.abs(obs - mean_obs)).sum(axis=axis)
        return 1.0 - (num / denom)
    else:
        num = np.ma.sum(np.ma.abs(obs - mod), axis=axis)
        mean_obs = np.ma.mean(obs, axis=axis)
        denom = np.ma.sum(
            np.ma.abs(mod - mean_obs) + np.ma.abs(obs - mean_obs), axis=axis
        )
        return 1.0 - (num / denom)


def AC(obs, mod, axis=None):
    """
    Anomaly Correlation (AC)

    Parameters
    ----------
    obs : array-like
        Observed values.
    mod : array-like
        Model predicted values.
    axis : int, optional
        Axis along which to compute the statistic.

    Returns
    -------
    float or ndarray
        Anomaly correlation coefficient (unitless, -1 to 1).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3, 4])
    >>> mod = np.array([2, 2, 2, 2])
    >>> stats.AC(obs, mod)
    0.0
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        obs_bar = obs.mean(dim=axis)
        mod_bar = mod.mean(dim=axis)
        p1 = ((mod - mod_bar) * (obs - obs_bar)).sum(dim=axis)
        p2 = (
            ((mod - mod_bar) ** 2).sum(dim=axis) * ((obs - obs_bar) ** 2).sum(dim=axis)
        ) ** 0.5
        return p1 / p2
    elif hasattr(obs, "mean") and hasattr(mod, "mean"):
        obs_bar = np.mean(obs, axis=axis)
        mod_bar = np.mean(mod, axis=axis)
        if axis is not None:
            obs_bar = np.expand_dims(obs_bar, axis=axis)
            mod_bar = np.expand_dims(mod_bar, axis=axis)
        p1 = ((mod - mod_bar) * (obs - obs_bar)).sum(axis=axis)
        p2 = (
            ((mod - mod_bar) ** 2).sum(axis=axis)
            * ((obs - obs_bar) ** 2).sum(axis=axis)
        ) ** 0.5
        return p1 / p2
    else:
        obs_bar = np.ma.mean(obs, axis=axis)
        mod_bar = np.ma.mean(mod, axis=axis)
        if axis is not None:
            obs_bar = np.ma.expand_dims(obs_bar, axis=axis)
            mod_bar = np.ma.expand_dims(mod_bar, axis=axis)
        p1 = ((mod - mod_bar) * (obs - obs_bar)).sum(axis=axis)
        p2 = (
            ((mod - mod_bar) ** 2).sum(axis=axis)
            * ((obs - obs_bar) ** 2).sum(axis=axis)
        ) ** 0.5
        return p1 / p2


def WDAC(obs, mod, axis=None):
    """
    Wind Direction Anomaly Correlation (WDAC)

    Parameters
    ----------
    obs : array-like
        Observed wind direction values (degrees).
    mod : array-like
        Modeled wind direction values (degrees).
    axis : int, optional
        Axis along which to compute the metric. Default is 0.

    Returns
    -------
    float or ndarray
        WDAC value(s)

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([350, 10, 20])
    >>> mod = np.array([10, 20, 30])
    >>> stats.WDAC(obs, mod)
    0.0
    """
    # Robust type-detection for xarray, numpy, masked arrays
    if hasattr(obs, "dims") and hasattr(mod, "dims"):
        # xarray DataArray
        obs_rad = obs * np.pi / 180.0
        mod_rad = mod * np.pi / 180.0
        obs_anom = obs_rad - obs_rad.mean(dim=obs.dims[axis])
        mod_anom = mod_rad - mod_rad.mean(dim=mod.dims[axis])
        numerator = (np.sin(obs_anom) * np.sin(mod_anom)).mean(dim=obs.dims[axis])
        denominator = np.sqrt(
            (np.sin(obs_anom) ** 2).mean(dim=obs.dims[axis])
            * (np.sin(mod_anom) ** 2).mean(dim=mod.dims[axis])
        )
        return numerator / denominator
    else:
        obs = np.asarray(obs)
        mod = np.asarray(mod)
        obs_rad = np.deg2rad(obs)
        mod_rad = np.deg2rad(mod)
        obs_anom = obs_rad - np.mean(obs_rad, axis=axis)
        mod_anom = mod_rad - np.mean(mod_rad, axis=axis)
        numerator = np.mean(np.sin(obs_anom) * np.sin(mod_anom), axis=axis)
        denominator = np.sqrt(
            np.mean(np.sin(obs_anom) ** 2, axis=axis)
            * np.mean(np.sin(mod_anom) ** 2, axis=axis)
        )
        return numerator / denominator


def taylor_skill(obs, mod, axis=None):
    """
    Taylor Skill Score (TSS)

    Typical Use Cases
    -----------------
    - Summarizing model performance in a single skill score for use in Taylor diagrams.
    - Used in climate, weather, and environmental model evaluation.

    Typical Values and Range
    ------------------------
    - Range: 0 to 1
    - 1: Perfect agreement between model and observations
    - 0: No skill

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed values.
    mod : array_like or xarray.DataArray
        Model or predicted values.
    axis : int, optional
        Axis along which to compute the skill score. Default is None (all elements).

    Returns
    -------
    skill : float or ndarray
        Taylor skill score (unitless, 0-1).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.taylor_skill(obs, mod)
    # Output: TSS value between 0 and 1
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        std_obs = float(obs.std(dim=axis))
        std_mod = float(mod.std(dim=axis))
        corr = float(xr.corr(obs, mod, dim=axis))
        num = 4.0 * corr * std_mod * std_obs
        denom = (std_mod**2 + std_obs**2) * (1.0 + corr) ** 2
        return num / denom
    else:
        std_obs = float(np.ma.std(obs, axis=axis))
        std_mod = float(np.ma.std(mod, axis=axis))
        from scipy.stats import pearsonr

        if np.ma.is_masked(obs):
            corr = float(pearsonr(obs.compressed(), mod.compressed())[0])  # type: ignore
        else:
            corr = float(pearsonr(obs, mod)[0])  # type: ignore
        return (4.0 * corr * std_mod * std_obs) / (
            (std_mod**2 + std_obs**2) * (1.0 + corr) ** 2
        )


def KGE(obs, mod, axis=None):
    """
    Kling-Gupta Efficiency (KGE)

    Typical Use Cases
    -----------------
    - Quantifying the overall agreement between model and observations, combining correlation, bias, and variability.
    - Used in hydrology, meteorology, and environmental model evaluation.

    Typical Values and Range
    ------------------------
    - Range: -∞ to 1
    - 1: Perfect agreement between model and observations
    - 0: Moderate skill
    - Negative values: Poor skill

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed values.
    mod : array_like or xarray.DataArray
        Model or predicted values.
    axis : int, optional
        Axis along which to compute KGE. Default is None (all elements).

    Returns
    -------
    kge : float or ndarray
        Kling-Gupta efficiency (unitless, -∞ to 1).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.KGE(obs, mod)
    # Output: KGE value between -∞ and 1
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        r = float(xr.corr(obs, mod, dim=axis))
        alpha = float(mod.std(dim=axis) / obs.std(dim=axis))
        beta = float(mod.mean(dim=axis) / obs.mean(dim=axis))
        return 1.0 - ((r - 1.0) ** 2 + (alpha - 1.0) ** 2 + (beta - 1.0) ** 2) ** 0.5
    else:
        from scipy.stats import pearsonr

        if np.ma.is_masked(obs):
            r = float(pearsonr(obs.compressed(), mod.compressed())[0])  # type: ignore
        else:
            r = float(pearsonr(obs, mod)[0])  # type: ignore
        alpha = float(np.ma.std(mod, axis=axis) / np.ma.std(obs, axis=axis))
        beta = float(np.ma.mean(mod, axis=axis) / np.ma.mean(obs, axis=axis))
        return 1.0 - ((r - 1.0) ** 2 + (alpha - 1.0) ** 2 + (beta - 1.0) ** 2) ** 0.5


def spearmanr(obs, mod, axis=None):
    """
    Spearman rank correlation coefficient.

    Parameters
    ----------
    obs : array_like
        Observed values.
    mod : array_like
        Model or predicted values.
    axis : int, optional
        Axis along which to compute the coefficient. Only None is supported.

    Returns
    -------
    rho : float
        Spearman rank correlation coefficient.

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> spearmanr(obs, mod)
    0.8660254037844387
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    from scipy.stats import spearmanr as _spearmanr

    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        if axis is None:
            axis = -1
        if isinstance(axis, int):
            dim = obs.dims[axis]
        else:
            dim = axis

        def _spearmanr_onlyrho(a, b):
            return _spearmanr(a, b)[0]

        rho = xr.apply_ufunc(
            _spearmanr_onlyrho,
            obs,
            mod,
            input_core_dims=[[dim], [dim]],
            output_core_dims=[[]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float],
        )
        return rho
    elif axis is None:
        return _spearmanr(obs, mod)[0]
    else:
        # Not implemented for axis, fallback to nan
        return np.nan


def kendalltau(obs, mod, axis=None):
    """
    Kendall rank correlation coefficient.

    This implementation is xarray- and dask-friendly: for xarray.DataArray inputs, it uses
    xarray.apply_ufunc to apply scipy.stats.kendalltau along the specified dimension, supporting dask-backed arrays.
    For numpy arrays, it falls back to scipy.stats.kendalltau.

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed values.
    mod : array_like or xarray.DataArray
        Model or predicted values.
    axis : int or str, optional
        Axis or dimension name along which to compute the coefficient. If None, uses the last dimension for xarray.

    Returns
    -------
    tau : float, ndarray, or xarray.DataArray
        Kendall rank correlation coefficient.

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> kendalltau(obs, mod)
    1.0
    >>> import xarray as xr
    >>> obs = xr.DataArray([1, 2, 3])
    >>> mod = xr.DataArray([2, 2, 4])
    >>> kendalltau(obs, mod)
    <xarray.DataArray ...>
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    from scipy.stats import kendalltau as _kendalltau

    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        # Default to last dimension if axis is None
        if axis is None:
            axis = -1
        # Get dimension name if axis is int
        if isinstance(axis, int):
            dim = obs.dims[axis]
        else:
            dim = axis

        def _kendalltau_onlytau(a, b):
            return _kendalltau(a, b)[0]

        tau = xr.apply_ufunc(
            _kendalltau_onlytau,
            obs,
            mod,
            input_core_dims=[[dim], [dim]],
            output_core_dims=[[]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float],
        )
        return tau
    else:
        if axis is None:
            return _kendalltau(obs, mod)[0]
        else:
            # Not implemented for axis, fallback to nan
            return np.nan
