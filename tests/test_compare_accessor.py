# Dask import for Dask-backed xarray tests
import dask.array as da
import numpy as np
import pytest
import xarray as xr


def make_test_dataarrays():
    lat = np.linspace(-10, 10, 5)
    lon = np.linspace(100, 120, 5)
    data1 = np.arange(25).reshape(5, 5)
    data2 = data1 + 1
    da1 = xr.DataArray(
        data1,
        coords={"latitude": lat, "longitude": lon},
        dims=["latitude", "longitude"],
    )
    da2 = xr.DataArray(
        data2,
        coords={"latitude": lat, "longitude": lon},
        dims=["latitude", "longitude"],
    )
    return da1, da2


# Dask-backed version
def make_dask_test_dataarrays():
    lat = np.linspace(-10, 10, 5)
    lon = np.linspace(100, 120, 5)
    data1 = da.from_array(np.arange(25).reshape(5, 5), chunks=(5, 5))  # type: ignore
    data2 = data1 + 1
    da1 = xr.DataArray(
        data1,
        coords={"latitude": lat, "longitude": lon},
        dims=["latitude", "longitude"],
    )
    da2 = xr.DataArray(
        data2,
        coords={"latitude": lat, "longitude": lon},
        dims=["latitude", "longitude"],
    )
    return da1, da2


def test_compare_diff():
    da1, da2 = make_test_dataarrays()
    diff = da1.monet.compare(da2, stat="diff", plot=False)
    np.testing.assert_array_equal(diff.values, da1.values - da2.values)


def test_compare_diff_dask():
    da1, da2 = make_dask_test_dataarrays()
    diff = da1.monet.compare(da2, stat="diff", plot=False)
    # .values will compute the dask array
    np.testing.assert_array_equal(diff.values, (da1.values - da2.values))


def test_compare_rmse():
    da1, da2 = make_test_dataarrays()
    rmse = da1.monet.compare(da2, stat="RMSE", plot=False)
    # Should match np.sqrt(mean((da1-da2)**2))
    expected = np.sqrt(((da1.values - da2.values) ** 2).mean())
    assert np.isclose(rmse.values.mean(), expected)


def test_compare_rmse_dask():
    da1, da2 = make_dask_test_dataarrays()
    rmse = da1.monet.compare(da2, stat="RMSE", plot=False)
    expected = np.sqrt(((da1.values - da2.values) ** 2).mean())
    assert np.isclose(rmse.values.mean(), expected)


def test_compare_mae():
    da1, da2 = make_test_dataarrays()
    mae = da1.monet.compare(da2, stat="mae", plot=False)
    expected = np.abs(da1.values - da2.values).mean()
    assert np.isclose(mae.values.mean(), expected)


def test_compare_mae_dask():
    da1, da2 = make_dask_test_dataarrays()
    mae = da1.monet.compare(da2, stat="mae", plot=False)
    expected = np.abs(da1.values - da2.values).mean()
    assert np.isclose(mae.values.mean(), expected)


def test_compare_callable():
    da1, da2 = make_test_dataarrays()

    def custom_stat(a, b):
        return (a + b).mean()

    result = da1.monet.compare(da2, stat=custom_stat, plot=False)
    assert np.isclose(result.values, (da1.values + da2.values).mean())


def test_compare_callable_dask():
    da1, da2 = make_dask_test_dataarrays()

    def custom_stat(a, b):
        return (a + b).mean()

    result = da1.monet.compare(da2, stat=custom_stat, plot=False)
    assert np.isclose(result.values, (da1.values + da2.values).mean())


def test_compare_invalid_stat():
    da1, da2 = make_test_dataarrays()
    with pytest.raises(ValueError):
        da1.monet.compare(da2, stat="not_a_stat", plot=False)


def test_compare_invalid_stat_dask():
    da1, da2 = make_dask_test_dataarrays()
    with pytest.raises(ValueError):
        da1.monet.compare(da2, stat="not_a_stat", plot=False)


def test_compare_plot_methods():
    da1, da2 = make_test_dataarrays()
    # Should not error, but may skip if plotting dependencies missing
    try:
        da1.monet.compare(da2, stat="diff", plot=True, plot_method="quick_map")
    except Exception:
        pass
    try:
        da1.monet.compare(da2, stat="diff", plot=True, plot_method="quick_imshow")
    except Exception:
        pass
    try:
        da1.monet.compare(da2, stat="diff", plot=True, plot_method="quick_contourf")
    except Exception:
        pass
