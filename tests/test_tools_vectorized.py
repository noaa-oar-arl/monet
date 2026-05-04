import dask.array as da
import numpy as np
import xarray as xr

from monet.util.tools import calc_13_category_usda_soil_type, get_relhum, wsdir2uv


def test_wsdir2uv_vectorized():
    """Verify wsdir2uv follows Vectorized Protocol."""
    ws_val = 10.0
    wdir_val = 90.0  # East wind (meteorological) -> U = -10, V = 0

    # Eager (NumPy)
    u_eager, v_eager = wsdir2uv(np.array([ws_val]), np.array([wdir_val]))
    assert isinstance(u_eager, np.ndarray)
    assert isinstance(v_eager, np.ndarray)

    # Lazy (Dask/Xarray)
    ws_lazy = xr.DataArray(da.from_array([ws_val], chunks=1), dims="x")
    wdir_lazy = xr.DataArray(da.from_array([wdir_val], chunks=1), dims="x")

    u_lazy, v_lazy = wsdir2uv(ws_lazy, wdir_lazy)

    # Check if it stayed lazy
    assert hasattr(u_lazy, "data")
    assert isinstance(u_lazy.data, da.Array), "U result should be a Dask array"
    assert hasattr(v_lazy, "data")
    assert isinstance(v_lazy.data, da.Array), "V result should be a Dask array"

    # Compare values
    np.testing.assert_allclose(u_eager, u_lazy.compute())
    np.testing.assert_allclose(v_eager, v_lazy.compute())

    # Check history
    assert "history" in u_lazy.attrs
    assert "Computed U and V wind components" in u_lazy.attrs["history"]


def test_get_relhum_vectorized():
    """Verify get_relhum follows Vectorized Protocol."""
    temp = 300.0
    press = 1013.25
    vap = 10.0

    # Eager (NumPy)
    rh_eager = get_relhum(np.array([temp]), np.array([press]), np.array([vap]))
    assert isinstance(rh_eager, np.ndarray)

    # Lazy (Dask/Xarray)
    temp_lazy = xr.DataArray(da.from_array([temp], chunks=1), dims="x")
    press_lazy = xr.DataArray(da.from_array([press], chunks=1), dims="x")
    vap_lazy = xr.DataArray(da.from_array([vap], chunks=1), dims="x")

    rh_lazy = get_relhum(temp_lazy, press_lazy, vap_lazy)

    # Check if it stayed lazy
    assert hasattr(rh_lazy, "data")
    assert isinstance(rh_lazy.data, da.Array), "RH result should be a Dask array"

    # Compare values
    np.testing.assert_allclose(rh_eager, rh_lazy.compute())

    # Check history
    assert "history" in rh_lazy.attrs
    assert "Computed relative humidity" in rh_lazy.attrs["history"]


def test_calc_13_category_usda_soil_type_vectorized():
    """Verify calc_13_category_usda_soil_type follows Vectorized Protocol."""
    clay = 20.0
    sand = 40.0
    silt = 40.0

    # Eager (NumPy)
    res_eager = calc_13_category_usda_soil_type(np.array([clay]), np.array([sand]), np.array([silt]))
    assert isinstance(res_eager, np.ndarray)

    # Lazy (Dask/Xarray)
    clay_lazy = xr.DataArray(da.from_array([clay], chunks=1), dims="x")
    sand_lazy = xr.DataArray(da.from_array([sand], chunks=1), dims="x")
    silt_lazy = xr.DataArray(da.from_array([silt], chunks=1), dims="x")

    res_lazy = calc_13_category_usda_soil_type(clay_lazy, sand_lazy, silt_lazy)

    # Check if it stayed lazy
    assert hasattr(res_lazy, "data")
    assert isinstance(res_lazy.data, da.Array), "Result should be a Dask array"

    # Compare values
    np.testing.assert_allclose(res_eager, res_lazy.compute())

    # Check history
    assert "history" in res_lazy.attrs
    assert "Computed USDA soil type" in res_lazy.attrs["history"]
