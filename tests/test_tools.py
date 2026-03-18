import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monet.util.interp_util import lonlat_to_dataset, points_to_dataset
from monet.util.tools import (
    findclosest,
    get_epa_region_df,
    get_giorgi_region_df,
    linregress,
    nearest,
    search_listinlist,
)


def test_search_listinlist_correctness():
    """Tests the correctness of the search_listinlist function."""
    array1 = np.array([1, 2, 3, 4, 5])
    array2 = np.array([3, 5, 6, 7, 8])
    index1, index2 = search_listinlist(array1, array2)
    np.testing.assert_array_equal(index1, np.array([2, 4]))
    np.testing.assert_array_equal(index2, np.array([0, 1]))


@pytest.mark.skipif("benchmark" not in dir(), reason="pytest-benchmark not available")
def test_search_listinlist_benchmark(benchmark):
    """Benchmarks the search_listinlist function."""
    array1 = np.arange(1000)
    array2 = np.arange(500, 1500)
    benchmark(search_listinlist, array1, array2)


def test_get_giorgi_region_df():
    """Tests the vectorized get_giorgi_region_df function for pandas."""
    # Test data including points inside a region, outside any region,
    # and on a boundary.
    data = {
        "latitude": [40, 0, 30],
        "longitude": [-95, -150, -83],
    }
    df = pd.DataFrame(data)

    # Expected results:
    # 1. Central North America (CNA)
    # 2. No region (should be None/NaN)
    # 3. Central America (CAM) - on the boundary
    expected_indices = [7.0, np.nan, 5.0]
    # We allow both None and nan for missing acronyms due to variations in pandas/numpy behavior
    expected_acros = ["CNA", np.nan, "CAM"]

    result_df = get_giorgi_region_df(df)

    # Check that the columns were added
    assert "GIORGI_INDEX" in result_df.columns
    assert "GIORGI_ACRO" in result_df.columns

    # Check the values
    np.testing.assert_array_equal(result_df["GIORGI_INDEX"].values, np.array(expected_indices))
    result_acros = result_df["GIORGI_ACRO"].tolist()
    for r, e in zip(result_acros, expected_acros):
        if pd.isna(e):
            assert pd.isna(r)
        else:
            assert r == e


def test_get_giorgi_region_ds():
    """Tests the vectorized get_giorgi_region_df function for xarray."""
    # Create a sample dataset grid
    lats = np.array([40, 0])
    lons = np.array([-95, -150])
    ds = xr.Dataset(coords={"latitude": lats, "longitude": lons})

    # Expected results for the 2x2 grid:
    # (40, -95): Central North America (CNA) -> 7.0
    # (40, -150): No region
    # (0, -95): No region
    # (0, -150): No region
    expected_indices = np.array([[7.0, np.nan], [np.nan, np.nan]])
    expected_acros = np.array([["CNA", np.nan], [np.nan, np.nan]], dtype=object)

    result_ds = get_giorgi_region_df(ds)

    # Check that the data variables were added
    assert "GIORGI_INDEX" in result_ds
    assert "GIORGI_ACRO" in result_ds

    # Check the values
    np.testing.assert_array_equal(result_ds["GIORGI_INDEX"].values, expected_indices)
    # Compare object arrays with possible nans/None
    result_acros = result_ds["GIORGI_ACRO"].values
    assert result_acros.shape == expected_acros.shape
    for r, e in zip(result_acros.flat, expected_acros.flat):
        if pd.isna(e):
            assert pd.isna(r)
        else:
            assert r == e


def test_get_epa_region_df():
    """Tests the vectorized get_epa_region_df function for pandas."""
    # Test data including points inside a region, outside any region,
    # and on a boundary.
    data = {
        "latitude": [42.0, 30.0, 40.0],
        "longitude": [-90.0, -100.0, -125.0],
    }
    df = pd.DataFrame(data)

    # Expected results:
    # 1. Region 5 (R5)
    # 2. Region 6 (R6)
    # 3. No region (should be None/NaN)
    expected_indices = [5.0, 6.0, np.nan]
    expected_acros = ["R5", "R6", np.nan]

    result_df = get_epa_region_df(df)

    # Check that the columns were added
    assert "EPA_INDEX" in result_df.columns
    assert "EPA_ACRO" in result_df.columns

    # Check the values
    np.testing.assert_array_equal(result_df["EPA_INDEX"].values, np.array(expected_indices))
    result_acros = result_df["EPA_ACRO"].tolist()
    for r, e in zip(result_acros, expected_acros):
        if pd.isna(e):
            assert pd.isna(r)
        else:
            assert r == e


def test_get_epa_region_ds():
    """Tests the vectorized get_epa_region_df function for xarray."""
    # Create a sample dataset grid
    lats = np.array([42.0, 30.0])
    lons = np.array([-90.0, -125.0])
    ds = xr.Dataset(coords={"latitude": lats, "longitude": lons})

    # Expected results for the 2x2 grid:
    # (42.0, -90.0): Region 5 (R5) -> 5.0
    # (42.0, -125.0): No region
    # (30.0, -90.0): Region 4 (R4) -> 4.0 (Checking another region)
    # (30.0, -125.0): No region
    expected_indices = np.array([[5.0, np.nan], [4.0, np.nan]])
    expected_acros = np.array([["R5", np.nan], ["R4", np.nan]], dtype=object)

    result_ds = get_epa_region_df(ds)

    # Check that the data variables were added
    assert "EPA_INDEX" in result_ds
    assert "EPA_ACRO" in result_ds

    # Check the values
    np.testing.assert_array_equal(result_ds["EPA_INDEX"].values, expected_indices)
    # Compare object arrays with possible nans/None
    result_acros = result_ds["EPA_ACRO"].values
    assert result_acros.shape == expected_acros.shape
    for r, e in zip(result_acros.flat, expected_acros.flat):
        if pd.isna(e):
            assert pd.isna(r)
        else:
            assert r == e


def test_get_giorgi_region_dask():
    """Verifies that get_giorgi_region_df works with both Eager and Lazy data."""
    lats = np.array([40.0, 0.0])
    lons = np.array([-95.0, -150.0])

    # 1. Eager (NumPy) path
    ds_eager = xr.Dataset(coords={"latitude": lats, "longitude": lons})
    result_eager = get_giorgi_region_df(ds_eager)

    # 2. Lazy (Dask) path
    # We use data variables to ensure they stay lazy, as xarray often computes coordinates
    ds_lazy = xr.Dataset(
        data_vars={
            "latitude": (["lat_dim"], da.from_array(lats, chunks=1)),
            "longitude": (["lon_dim"], da.from_array(lons, chunks=1)),
        }
    )
    result_lazy = get_giorgi_region_df(ds_lazy)

    # Assertions
    assert not hasattr(result_eager.GIORGI_INDEX.data, "chunks")
    assert hasattr(result_lazy.GIORGI_INDEX.data, "chunks")

    # Values should be identical after compute
    np.testing.assert_allclose(result_eager.GIORGI_INDEX.values, result_lazy.GIORGI_INDEX.compute().values)


def test_linregress_aero():
    """Verifies that linregress works with both Eager and Lazy data."""
    # Create sample data
    x_data = np.linspace(0, 10, 100)
    # y = 2.5x + 1.0
    y_data = 2.5 * x_data + 1.0

    # 1. Eager (NumPy) path
    slope_e, intercept_e, r2_e, stderr_e = linregress(x_data, y_data)

    # 2. Lazy (Dask) path with xarray
    x_da = xr.DataArray(x_data, dims=["time"]).chunk({"time": 100})
    y_da = xr.DataArray(y_data, dims=["time"]).chunk({"time": 100})

    slope_l, intercept_l, r2_l, stderr_l = linregress(x_da, y_da)

    # Assertions for Laziness
    assert hasattr(slope_l.data, "chunks")
    assert hasattr(intercept_l.data, "chunks")

    # Compute results
    slope_l_c = slope_l.compute()
    intercept_l_c = intercept_l.compute()

    # Assertions for Correctness
    np.testing.assert_allclose(slope_e, slope_l_c)
    np.testing.assert_allclose(intercept_e, intercept_l_c)

    # Verify values match expectations
    assert np.isclose(slope_e, 2.5)
    assert np.isclose(intercept_e, 1.0)
    assert np.isclose(r2_e, 1.0)

    # Test with multi-dimensional data
    x_2d = np.tile(x_data, (2, 1))  # (2, 100)
    y_2d = np.tile(y_data, (2, 1))

    slope_2d, intercept_2d, r2_2d, stderr_2d = linregress(x_2d, y_2d)
    assert slope_2d.shape == (2,)
    np.testing.assert_allclose(slope_2d, [slope_e, slope_e])

    # Test with 2D DataArray (Dask)
    x_da_2d = xr.DataArray(x_2d, dims=["site", "time"]).chunk({"site": 1, "time": 100})
    y_da_2d = xr.DataArray(y_2d, dims=["site", "time"]).chunk({"site": 1, "time": 100})

    slope_l_2d, intercept_l_2d, r2_l_2d, stderr_l_2d = linregress(x_da_2d, y_da_2d)
    assert hasattr(slope_l_2d.data, "chunks")
    assert slope_l_2d.shape == (2,)
    np.testing.assert_allclose(slope_l_2d.compute(), [slope_e, slope_e])


def test_findclosest_aero():
    """Verifies findclosest works with both Eager and Lazy data."""
    arr = np.array([0, 10, 20, 30, 40, 50])
    val = 22.5

    # Eager path
    idx_e, res_e = findclosest(arr, val)
    assert idx_e == 2
    assert res_e == 20

    # Lazy path with multiple values
    arr_da = xr.DataArray(arr, dims=["search_dim"]).chunk({"search_dim": 3})
    val_da = xr.DataArray([22.5, 38.0], dims=["val_dim"]).chunk({"val_dim": 1})

    idx_l, res_l = findclosest(arr_da, val_da)

    assert hasattr(idx_l.data, "chunks")
    assert hasattr(res_l.data, "chunks")

    np.testing.assert_array_equal(idx_l.compute().values, [2, 4])
    np.testing.assert_array_equal(res_l.compute().values, [20, 40])


def test_nearest_aero():
    """Verifies nearest works with both Eager and Lazy data."""
    arr = np.array([0, 10, 20, 30, 40, 50])
    val = 22.5

    res_e = nearest(arr, val)
    assert res_e == 20

    arr_da = xr.DataArray(arr, dims=["search_dim"]).chunk({"search_dim": 3})
    res_l = nearest(arr_da, val)

    assert hasattr(res_l.data, "chunks")
    assert res_l.compute() == 20


def test_lonlat_to_dataset_aero():
    """Verifies lonlat_to_dataset preserves laziness."""
    lon = xr.DataArray(np.linspace(0, 360, 10), dims=["x"]).chunk({"x": 5})
    lat = xr.DataArray(np.linspace(-90, 90, 5), dims=["y"]).chunk({"y": 5})

    ds = lonlat_to_dataset(lon, lat)

    assert hasattr(ds.lon.data, "chunks")
    assert hasattr(ds.lat.data, "chunks")
    assert ds.lon.shape == (5, 10)
    assert ds.lat.shape == (5, 10)

    # Verify correctness against eager path
    ds_e = lonlat_to_dataset(lon.values, lat.values)
    np.testing.assert_allclose(ds.lon.compute().values, ds_e.lon.values)


def test_points_to_dataset_aero():
    """Verifies points_to_dataset preserves laziness."""
    lon = xr.DataArray(np.linspace(0, 360, 10), dims=["site"]).chunk({"site": 5})
    lat = xr.DataArray(np.linspace(-90, 90, 10), dims=["site"]).chunk({"site": 5})

    ds = points_to_dataset(lon, lat)

    assert hasattr(ds.lon.data, "chunks")
    assert hasattr(ds.lat.data, "chunks")
    assert ds.lon.shape == (10, 1)

    # Verify correctness against eager path
    ds_e = points_to_dataset(lon.values, lat.values)
    np.testing.assert_allclose(ds.lon.compute().values, ds_e.lon.values)
