def NSE(obs, mod, axis=None):
    """
    Nash-Sutcliffe Efficiency (NSE).

    Parameters
    ----------
    obs : array_like or xarray.DataArray
        Observed values.
    mod : array_like or xarray.DataArray
        Model or predicted values.
    axis : int, optional
        Axis along which to compute NSE. Default is None (all elements).

    Returns
    -------
    nse : float or ndarray
        Nash-Sutcliffe efficiency.

    Examples
    --------
    >>> import numpy as np
    >>> obs = np.array([1, 2, 3])
    >>> mod = np.array([2, 2, 4])
    >>> NSE(obs, mod)
    0.5
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
        num = ((mod - obs) ** 2).sum(dim=axis)
        denom = ((obs - obs.mean(dim=axis)) ** 2).sum(dim=axis)
    else:
        num = ((mod - obs) ** 2).sum(axis=axis)
        denom = ((obs - obs.mean(axis=axis)) ** 2).sum(axis=axis)
    return 1 - num / denom
