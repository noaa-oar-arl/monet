"""
Vertical coordinate utilities for MONET.
"""

import numpy as np
import xarray as xr

from .constants import R_d, g


def calc_fv3_pressure(
    ak: xr.DataArray | np.ndarray,
    bk: xr.DataArray | np.ndarray,
    ps: xr.DataArray | np.ndarray,
    dim: str = "z",
) -> xr.DataArray:
    """Calculate pressure from FV3 hybrid coordinate coefficients.

    The pressure at a given level is calculated using the formula:
    P = ak + bk * ps

    Parameters
    ----------
    ak : xarray.DataArray or numpy.ndarray
        Hybrid coordinate coefficient 'ak'. Usually in Pa.
    bk : xarray.DataArray or numpy.ndarray
        Hybrid coordinate coefficient 'bk'.
    ps : xarray.DataArray or numpy.ndarray
        Surface pressure. Usually in Pa.
    dim : str, optional
        Name of the vertical dimension. Default is 'z'.

    Returns
    -------
    xarray.DataArray
        The calculated pressure.

    Notes
    -----
    This function is backend-agnostic and supports both NumPy and Dask-backed
    xarray objects.
    """
    if not isinstance(ak, xr.DataArray):
        ak = xr.DataArray(ak, dims=[dim])
    if not isinstance(bk, xr.DataArray):
        bk = xr.DataArray(bk, dims=[dim])
    if not isinstance(ps, xr.DataArray):
        ps = xr.DataArray(ps, dims=[f"__dim_{i}" for i in range(ps.ndim)])

    def _p_logic(a, b, s):
        # Core dimensions are moved to the end by apply_ufunc.
        # a, b: (..., N), s: (...)
        return a + b * s[..., np.newaxis]

    res = xr.apply_ufunc(
        _p_logic,
        ak,
        bk,
        ps,
        input_core_dims=[[dim], [dim], []],
        output_core_dims=[[dim]],
        dask="parallelized",
    )

    if isinstance(res, xr.DataArray):
        # Move vertical dim to the front to match standard MONET convention
        all_dims = [dim] + [d for d in res.dims if d != dim]
        res = res.transpose(*all_dims)

        from .conventions import update_history

        update_history(res, "Calculated pressure via calc_fv3_pressure")
        res.name = "pressure"
        if hasattr(ps, "attrs") and "units" in ps.attrs:
            res.attrs["units"] = ps.attrs["units"]

    return res


def calc_fv3_height(
    temp: xr.DataArray | np.ndarray,
    phalf: xr.DataArray | np.ndarray,
    hsfc: xr.DataArray | np.ndarray = 0.0,
    dim: str = "z",
) -> xr.DataArray:
    """Calculate geopotential height using the hypsometric equation.

    Calculates geopotential height at layer interfaces (phalf levels) by
    integrating the hypsometric equation from the surface upwards.

    Parameters
    ----------
    temp : xarray.DataArray or numpy.ndarray
        Air temperature at layer centers (K). Shape: (n_layers, ...)
    phalf : xarray.DataArray or numpy.ndarray
        Pressure at layer interfaces (Pa). Shape: (n_layers + 1, ...)
    hsfc : xarray.DataArray or numpy.ndarray, default 0.0
        Surface geopotential height (m).
    dim : str, optional
        Name of the vertical dimension. Default is 'z'.

    Returns
    -------
    xarray.DataArray
        Geopotential height at interfaces (m). Shape: (n_layers + 1, ...)

    Notes
    -----
    This function assumes the vertical dimension is ordered from top to bottom
    (index 0 is the top of the atmosphere, last index is the surface).
    It is backend-agnostic and supports both NumPy and Dask-backed xarray objects.
    """
    if not isinstance(temp, xr.DataArray):
        temp = xr.DataArray(temp, dims=[dim] + [f"__dim_{i}" for i in range(temp.ndim - 1)])
    if not isinstance(phalf, xr.DataArray):
        phalf = xr.DataArray(phalf, dims=[dim] + [f"__dim_{i}" for i in range(phalf.ndim - 1)])

    if not isinstance(hsfc, xr.DataArray):
        # Attempt to match spatial dimensions of temp if hsfc is not a DataArray
        spatial_dims = [d for d in temp.dims if d != dim]
        if hsfc.ndim == len(spatial_dims):
            hsfc = xr.DataArray(hsfc, dims=spatial_dims)
        else:
            hsfc = xr.DataArray(hsfc)

    def _hydrostatic_logic(t, p_int, h_s):
        # Core dimensions are moved to the end.
        # t: (..., N), p_int: (..., N+1), h_s: (...)
        p_up = p_int[..., :-1]
        p_lo = p_int[..., 1:]
        # dz = (R_d * T / g) * ln(p_lo / p_up)
        dz = (R_d * t / g) * np.log(p_lo / p_up)

        h_int = np.empty_like(p_int)
        h_int[..., -1] = h_s
        # Summing from bottom up (along the last dimension)
        h_int[..., :-1] = h_s[..., np.newaxis] + np.cumsum(dz[..., ::-1], axis=-1)[..., ::-1]
        return h_int

    # Rename dims to allow different sizes in apply_ufunc
    temp_renamed = temp.rename({dim: "__layer"})
    phalf_renamed = phalf.rename({dim: "__interface"})

    res = xr.apply_ufunc(
        _hydrostatic_logic,
        temp_renamed,
        phalf_renamed,
        hsfc,
        input_core_dims=[["__layer"], ["__interface"], []],
        output_core_dims=[["__interface"]],
        dask="parallelized",
        output_dtypes=[temp.dtype],
    )

    res = res.rename({"__interface": dim})

    if isinstance(res, xr.DataArray):
        # Move vertical dim to the front to match standard MONET convention
        all_dims = [dim] + [d for d in res.dims if d != dim]
        res = res.transpose(*all_dims)

        from .conventions import update_history

        update_history(res, "Calculated geopotential height via calc_fv3_height")
        res.name = "height"
        res.attrs["units"] = "m"

    return res
