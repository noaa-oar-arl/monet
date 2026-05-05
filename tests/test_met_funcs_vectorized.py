import dask.array as da
import numpy as np
import xarray as xr

from monet.met_funcs import calc_c_p, calc_Psi_H


def test_calc_c_p_vectorized():
    """Verify calc_c_p follows Vectorized Protocol."""
    p_val = 1013.25
    ea_val = 10.0

    # Eager (NumPy)
    res_eager = calc_c_p(p_val, ea_val)
    # Check that it returns a scalar or array-like
    assert isinstance(res_eager, np.ndarray | np.floating | float)

    # Lazy (Dask/Xarray)
    p_lazy = xr.DataArray(da.from_array([p_val], chunks=1), dims="x")
    ea_lazy = xr.DataArray(da.from_array([ea_val], chunks=1), dims="x")

    res_lazy = calc_c_p(p_lazy, ea_lazy)

    # Check if it stayed lazy
    assert hasattr(res_lazy, "data")
    assert isinstance(res_lazy.data, da.Array), "Result should be a Dask array to preserve laziness"

    # Compare values
    np.testing.assert_allclose(res_eager, res_lazy.compute())

    # Check history
    assert "history" in res_lazy.attrs
    assert "Computed heat capacity (c_p) via monet.met_funcs" in res_lazy.attrs["history"]


def test_calc_Psi_H_vectorized():
    """Verify calc_Psi_H follows Vectorized Protocol."""
    zoL_vals = np.array([-1.0, 0.0, 1.0])

    # Eager (NumPy)
    res_eager = calc_Psi_H(zoL_vals)
    assert isinstance(res_eager, np.ndarray)

    # Lazy (Dask/Xarray)
    zoL_lazy = xr.DataArray(da.from_array(zoL_vals, chunks=2), dims="x")

    res_lazy = calc_Psi_H(zoL_lazy)

    assert hasattr(res_lazy, "data")
    assert isinstance(res_lazy.data, da.Array), "Result should be a Dask array to preserve laziness"

    # Compare values
    np.testing.assert_allclose(res_eager, res_lazy.compute())

    # Check history
    assert "history" in res_lazy.attrs
    assert "Computed adiabatic correction factor (heat) via monet.met_funcs" in res_lazy.attrs["history"]
