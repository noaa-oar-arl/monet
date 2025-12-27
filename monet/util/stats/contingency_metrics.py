import numpy as np


def HSS(obs, mod, minval, maxval=None):
    """
    Heidke Skill Score (HSS)

    Typical Use Cases
    -----------------
    - Evaluating categorical forecast skill (e.g., precipitation, air quality events).
    - Used in meteorology and environmental modeling to assess binary event prediction accuracy.

    Typical Values and Range
    ------------------------
    - Range: -∞ to 1
    - 1: Perfect forecast
    - 0: No skill (random forecast)
    - Negative values: Worse than random

    Parameters
    ----------
    obs : array-like
        Observed values.
    mod : array-like
        Modeled values.
    minval : float
        Threshold value for contingency table.

    Returns
    -------
    float
        HSS value for the given threshold.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 0, 1, 0])
    >>> mod = np.array([1, 1, 0, 0])
    >>> stats.HSS(obs, mod, minval=0.5)
    # Output: HSS value between -∞ and 1
    """
    a, b, c, d = _contingency_table(obs, mod, minval, maxval)
    denom = (a + c) * (c + d) + (a + b) * (b + d)
    if denom > 0:
        return 2 * (a * d - b * c) / denom
    else:
        return np.nan


def ETS(obs, mod, minval, maxval=None):
    """
    Equitable Threat Score (ETS)

    Typical Use Cases
    -----------------
    - Evaluating forecast skill for rare events (e.g., precipitation, air quality exceedances).
    - Used in meteorology and environmental modeling to assess binary event prediction accuracy.

    Typical Values and Range
    ------------------------
    - Range: -1/3 to 1
    - 1: Perfect forecast
    - 0: No skill (random forecast)
    - Negative values: Worse than random

    Parameters
    ----------
    obs : array-like
        Observed values.
    mod : array-like
        Modeled values.
    minval : float
        Threshold value for contingency table.

    Returns
    -------
    float
        ETS value for the given threshold.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 0, 1, 0])
    >>> mod = np.array([1, 1, 0, 0])
    >>> stats.ETS(obs, mod, minval=0.5, maxval=None)
    # Output: ETS value between -1/3 and 1
    """
    # Compute contingency table
    a, b, c, d = _contingency_table(obs, mod, minval, maxval)
    total = a + b + c + d
    random_hits = ((a + b) * (a + c)) / total if total > 0 else 0
    denom = a + b + c - random_hits
    if denom > 0:
        return (a - random_hits) / denom
    else:
        return np.nan


def CSI(obs, mod, minval, maxval=None):
    """
    Critical Success Index (CSI)

    Typical Use Cases
    -----------------
    - Evaluating forecast skill for rare or binary events (e.g., precipitation, air quality exceedances).
    - Used in meteorology and environmental modeling to assess event prediction accuracy.

    Typical Values and Range
    ------------------------
    - Range: 0 to 1
    - 1: Perfect forecast
    - 0: No skill (no correct predictions)

    Parameters
    ----------
    obs : array-like
        Observed values.
    mod : array-like
        Modeled values.
    minval : float
        Threshold value for contingency table.
    maxval : float
        Maximum threshold value (not used in calculation).

    Returns
    -------
    float
        CSI value for the given threshold.

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 0, 1, 0])
    >>> mod = np.array([1, 1, 0, 0])
    >>> stats.CSI(obs, mod, minval=0.5, maxval=None)
    # Output: CSI value between 0 and 1
    """
    a, b, c, d = _contingency_table(obs, mod, minval, maxval)
    denom = a + b + c
    csi = a / denom if denom > 0 else np.nan
    return csi


def scores(obs, mod, minval, maxval=None):
    """Calculate scores using the new _contingency_table.

    Parameters
    ----------
    obs : array-like
        Observation values ("truth").
    mod : array-like
        Model values ("prediction").
        Should be the same size as `obs`.
    minval : float
        Threshold for event (used as threshold for _contingency_table).
    maxval : float, optional
        Unused, kept for compatibility.

    Returns
    -------
    a, b, c, d : float
        Counts of hits, misses, false alarms, and correct negatives.
    """
    return _contingency_table(obs, mod, minval, maxval)


def POD(obs, mod, minval, maxval=None):
    """
    Probability of Detection (POD) for a given event threshold.

    Typical Use Cases
    -----------------
    - Evaluating how well a model detects events above a critical threshold
      (e.g., pollution exceedances, precipitation events).
    - Used in contingency table analysis for categorical forecast verification.

    Parameters
    ----------
    obs : array_like
        Observed values.
    mod : array_like
        Model or predicted values.
    threshold : float
        Event threshold.

    Returns
    -------
    pod : float
        Probability of detection.

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.array([0, 1, 1, 0])
    >>> mod = np.array([1, 1, 0, 0])
    >>> POD(obs, mod, threshold=0.5)
    0.5
    """
    a, b, c, d = _contingency_table(obs, mod, minval, maxval)
    return a / (a + b) if (a + b) > 0 else np.nan


def FAR(obs, mod, minval, maxval=None):
    """
    False Alarm Rate (FAR) for a given event threshold.

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
        Kling-Gupta efficiency (unitless, -inf to 1).

    Examples
    --------
    >>> import numpy as np
    >>> from monet.util import stats
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> stats.KGE(obs, mod)
    ... # returns a value between -inf and 1
    """
    a, b, c, d = _contingency_table(obs, mod, minval, maxval)
    return c / (a + c) if (a + c) > 0 else np.nan


def FBI(obs, mod, minval, maxval=None):
    """
    Frequency Bias Index (FBI) for a given event threshold.

    Parameters
    ----------
    obs : array_like
        Observed values.
    mod : array_like
        Model or predicted values.
    threshold : float
        Event threshold.

    Returns
    -------
    fbi : float
        Frequency bias index.

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.array([0, 1, 1, 0])
    >>> mod = np.array([1, 1, 0, 0])
    >>> FBI(obs, mod, threshold=0.5)
    1.0
    """
    a, b, c, d = _contingency_table(obs, mod, minval, maxval)
    return (a + c) / (a + b) if (a + b) > 0 else np.nan


def TSS(obs, mod, minval, maxval=None):
    """
    Hanssen-Kuipers Discriminant (True Skill Statistic, TSS).

    Parameters
    ----------
    obs : array_like
        Observed values.
    mod : array_like
        Model or predicted values.
    threshold : float
        Event threshold.

    Returns
    -------
    tss : float
        True skill statistic.

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.array([0, 1, 1, 0])
    >>> mod = np.array([1, 1, 0, 0])
    >>> TSS(obs, mod, threshold=0.5)
    0.0
    """
    a, b, c, d = _contingency_table(obs, mod, minval, maxval)
    pod = a / (a + b) if (a + b) > 0 else np.nan
    pofd = c / (c + d) if (c + d) > 0 else np.nan
    return pod - pofd


def _contingency_table(obs, mod, minval, maxval=None):
    """
    Compute the 2x2 contingency table for event-based metrics.

    Parameters
    ----------
    obs : array_like
        Observed values.
    mod : array_like
        Model or predicted values.
    threshold : float
        Event threshold.

    Returns
    -------
    a : float
        Hits (obs >= threshold and mod >= threshold)
    b : float
        Misses (obs >= threshold and mod < threshold)
    c : float
        False alarms (obs < threshold and mod >= threshold)
    d : float
        Correct negatives (obs < threshold and mod < threshold)
    """
    import numpy as np

    try:
        import xarray as xr
    except ImportError:
        xr = None
    # Drop NaNs and align for xarray
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs, mod = xr.align(obs, mod, join="inner")
        mask = (~xr.ufuncs.isnan(obs)) & (~xr.ufuncs.isnan(mod))
        obs = obs.where(mask, drop=True)
        mod = mod.where(mask, drop=True)
        obs_vals = obs.values
        mod_vals = mod.values
    else:
        obs_vals = np.asarray(obs)
        mod_vals = np.asarray(mod)
        mask = ~np.isnan(obs_vals) & ~np.isnan(mod_vals)
        obs_vals = obs_vals[mask]
        mod_vals = mod_vals[mask]
    if maxval is not None:
        obs_event = (obs_vals >= minval) & (obs_vals < maxval)
        mod_event = (mod_vals >= minval) & (mod_vals < maxval)
    else:
        obs_event = obs_vals >= minval
        mod_event = mod_vals >= minval
    hits = int(np.logical_and(obs_event, mod_event).sum())
    misses = int(np.logical_and(obs_event, ~mod_event).sum())
    false_alarms = int(np.logical_and(~obs_event, mod_event).sum())
    correct_negatives = int(np.logical_and(~obs_event, ~mod_event).sum())
    return hits, misses, false_alarms, correct_negatives
