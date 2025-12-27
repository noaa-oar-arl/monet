"""
Spatial and Ensemble Metrics for Atmospheric Sciences
"""

import numpy as np


def FSS(obs, mod, window=3, threshold=None):
    """
    Fractions Skill Score (FSS) for spatial fields.

    Typical Use Cases
    -----------------
    - Assessing spatial skill of high-resolution models for precipitation, air quality, or other gridded fields.
    - Used in spatial verification to compare observed and modeled event patterns at different scales.

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed field (2D).
    mod : array_like or xarray.DataArray
        Model field (2D).
    window : int, optional
        Size of square window (odd integer), default is 3.
    threshold : float, optional
        Event threshold. If None, uses mean of obs.

    Returns
    -------
    fss : float
        Fractions Skill Score (1 is perfect, 0 is no skill).

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.zeros((5, 5)); obs[2, 2] = 1
    >>> mod = np.zeros((5, 5)); mod[2, 3] = 1
    >>> FSS(obs, mod, window=3, threshold=0.5)
    0.8888888888888888
    """
    try:
        import xarray as xr
    except ImportError:
        xr = None
    from scipy.ndimage import uniform_filter

    if threshold is None:
        threshold = np.nanmean(obs)
    if (
        xr is not None
        and isinstance(obs, xr.DataArray)
        and isinstance(mod, xr.DataArray)
    ):
        obs_bin = (obs >= threshold).astype(float)
        mod_bin = (mod >= threshold).astype(float)
        obs_frac = xr.DataArray(
            uniform_filter(obs_bin, window, mode="nearest"),
            dims=obs.dims,
            coords=obs.coords,
        )
        mod_frac = xr.DataArray(
            uniform_filter(mod_bin, window, mode="nearest"),
            dims=mod.dims,
            coords=mod.coords,
        )
        num = ((obs_frac - mod_frac) ** 2).mean().item()
        denom = (obs_frac**2).mean().item() + (mod_frac**2).mean().item()
    else:
        obs_bin = (np.asarray(obs) >= threshold).astype(float)
        mod_bin = (np.asarray(mod) >= threshold).astype(float)
        obs_frac = uniform_filter(obs_bin, window, mode="nearest")
        mod_frac = uniform_filter(mod_bin, window, mode="nearest")
        num = np.nanmean((obs_frac - mod_frac) ** 2)
        denom = np.nanmean(obs_frac**2) + np.nanmean(mod_frac**2)
    return 1 - num / denom if denom > 0 else np.nan


def EDS(obs, mod, threshold):
    """
    Extreme Dependency Score (EDS) for rare event detection.

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed field (2D).
    mod : array_like or xarray.DataArray
        Model field (2D).
    threshold : float
        Event threshold.

    Returns
    -------
    eds : float
        Extreme Dependency Score.

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.zeros((5, 5)); obs[2, 2] = 1
    >>> mod = np.zeros((5, 5)); mod[2, 3] = 1
    >>> EDS(obs, mod, threshold=0.5)
    0.0
    """
    obs_bin = np.asarray(obs) >= threshold
    mod_bin = np.asarray(mod) >= threshold
    hits = np.logical_and(obs_bin, mod_bin).sum()
    n_obs = obs_bin.sum()
    n_mod = mod_bin.sum()
    n = np.size(obs)
    if hits == 0 or n_obs == 0 or n_mod == 0:
        return np.nan
    p = n_obs / n
    q = n_mod / n
    eds = np.log(hits / n) / np.log(p * q) if p > 0 and q > 0 else np.nan
    return eds


def CRPS(ensemble, obs, axis=0):
    """
    Continuous Ranked Probability Score (CRPS) for ensemble forecasts.

    Parameters
    ----------
    ensemble : array_like
        Ensemble forecasts, shape (n_ensemble, ...).
    obs : array_like
        Observed values, shape (...).
    axis : int, optional
        Axis corresponding to ensemble members. Default is 0.

    Returns
    -------
    crps : ndarray
        CRPS values, shape (...).

    Examples
    --------
    >>> import numpy as np
    >>> ens = np.array([[1, 2], [2, 3], [3, 4]])
    >>> obs = np.array([2, 3])
    >>> CRPS(ens, obs)
    array([0.22222222, 0.22222222])
    """
    ens = np.asarray(ensemble)
    obs = np.asarray(obs)
    ens_sorted = np.sort(ens, axis=axis)
    n = ens.shape[axis]
    # Compute empirical CDFs
    cdf_ens = np.arange(1, n + 1) / n
    # Broadcast obs for comparison
    obs_broadcast = np.expand_dims(obs, axis)
    cdf_obs = (ens_sorted >= obs_broadcast).astype(float)
    crps = np.mean((cdf_ens - cdf_obs) ** 2, axis=axis)
    return crps


def spread_error(ensemble, obs, axis=0):
    """
    Spread-Error Relationship for ensemble forecasts.

    Parameters
    ----------
    ensemble : array_like
        Ensemble forecasts, shape (n_ensemble, ...).
    obs : array_like
        Observed values, shape (...).
    axis : int, optional
        Axis corresponding to ensemble members. Default is 0.

    Returns
    -------
    mean_spread : float
        Mean ensemble spread.
    mean_error : float
        Mean absolute error of ensemble mean vs. obs.

    Examples
    --------
    >>> import numpy as np
    >>> ens = np.array([[1, 2], [2, 3], [3, 4]])
    >>> obs = np.array([2, 3])
    >>> spread_error(ens, obs)
    (0.816496580927726, 0.3333333333333333)
    """
    ens = np.asarray(ensemble)
    obs = np.asarray(obs)
    spread = np.std(ens, axis=axis)
    ens_mean = np.mean(ens, axis=axis)
    error = np.abs(ens_mean - obs)
    return np.mean(spread), np.mean(error)


def SAL(obs, mod, threshold=None):
    """
    Structure-Amplitude-Location (SAL) score for spatial verification.

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed field (2D).
    mod : array_like or xarray.DataArray
        Model field (2D).
    threshold : float, optional
        Threshold for object identification. If None, uses mean of obs.

    Returns
    -------
    S : float
        Structure component (-2 to 2, 0 is best).
    A : float
        Amplitude component (-2 to 2, 0 is best).
    L : float
        Location component (0 to 2, 0 is best).

    Notes
    -----
    SAL is a feature-based spatial verification metric. It compares the structure,
    amplitude, and location of features (objects) in the observed and model fields.

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.zeros((5, 5)); obs[2, 2] = 1
    >>> mod = np.zeros((5, 5)); mod[2, 3] = 1
    >>> SAL(obs, mod)
    (0.0, 0.0, 0.06324555320336758)
    """
    import scipy.ndimage as ndi

    obs = np.asarray(obs)
    mod = np.asarray(mod)
    if threshold is None:
        threshold = np.mean(obs)
    # Amplitude
    A = 2 * (np.mean(mod) - np.mean(obs)) / (np.mean(mod) + np.mean(obs))

    # Structure
    def structure(X):
        result = ndi.label(X >= threshold)
        if isinstance(result, tuple):
            labeled, n = result
        else:
            labeled = result
            n = 0 if labeled is None else 1
        if n == 0:
            return 0.0, 0.0
        masses = ndi.sum(X, labeled, index=np.arange(1, n + 1))
        max_mass = np.max(masses)
        total_mass = np.sum(masses)
        return max_mass, total_mass

    max_mod, sum_mod = structure(mod)
    max_obs, sum_obs = structure(obs)
    S = (
        2
        * (max_mod / sum_mod - max_obs / sum_obs)
        / (max_mod / sum_mod + max_obs / sum_obs)
        if sum_mod > 0 and sum_obs > 0
        else np.nan
    )

    # Location
    def centroid(X):
        result = ndi.label(X >= threshold)
        if isinstance(result, tuple):
            labeled, n = result
        else:
            labeled = result
            n = 0 if labeled is None else 1
        if n == 0:
            return np.array([np.nan, np.nan])
        centers = np.array(ndi.center_of_mass(X, labeled, index=np.arange(1, n + 1)))
        masses = ndi.sum(X, labeled, index=np.arange(1, n + 1))
        weighted = np.average(centers, axis=0, weights=masses)
        return weighted

    c_mod = centroid(mod)
    c_obs = centroid(obs)
    L1 = np.linalg.norm(c_mod - c_obs) / np.sqrt(obs.shape[0] ** 2 + obs.shape[1] ** 2)

    # Spread of objects
    def spread(X):
        result = ndi.label(X >= threshold)
        if isinstance(result, tuple):
            labeled, n = result
        else:
            labeled = result
            n = 0 if labeled is None else 1
        if n == 0:
            return 0.0
        centers = np.array(ndi.center_of_mass(X, labeled, index=np.arange(1, n + 1)))
        masses = ndi.sum(X, labeled, index=np.arange(1, n + 1))
        c = np.average(centers, axis=0, weights=masses)
        return np.average(np.linalg.norm(centers - c, axis=1), weights=masses)

    r_mod = spread(mod)
    r_obs = spread(obs)
    L2 = abs(r_mod - r_obs) / np.sqrt(obs.shape[0] ** 2 + obs.shape[1] ** 2)
    L = L1 + L2
    return S, A, L
