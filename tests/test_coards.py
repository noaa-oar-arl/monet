import numpy as np
import pytest
import xarray as xr

from monet.monet_accessor import _coards_to_netcdf, _dataarray_coards_to_netcdf

lonmin, latmin, lonmax, latmax = [0, 0, 10, 10]


def make_ds(*, nx=8, ny=5, lat_lon_dims=True, time=False):
    data = np.arange(nx * ny).reshape((ny, nx))
    assert data.flags["C_CONTIGUOUS"], "xregrid requires C-contiguous arrays"

    if lat_lon_dims:
        lat_dim, lon_dim = "lat", "lon"
    else:
        lat_dim, lon_dim = "y", "x"
    dims = (lat_dim, lon_dim)
    coords = {
        "lat": (lat_dim, np.linspace(latmin, latmax, ny)),
        "lon": (lon_dim, np.linspace(lonmin, lonmax, nx)),
    }

    if time:
        time_coord = np.arange(2)
        data = np.stack([data, data + 100], axis=0)
        dims = ("time",) + dims
        coords["time"] = ("time", time_coord)

    return xr.Dataset(
        data_vars={"data": (dims, data)},
        coords=coords,
    )


@pytest.mark.parametrize(
    "lat_lon_dims",
    [
        pytest.param(True, id="lat-lon-dims"),
        pytest.param(False, id="x-y-dims"),
    ],
)
@pytest.mark.parametrize(
    "time",
    [
        pytest.param(False, id="no-time"),
        pytest.param(True, id="with-time"),
    ],
)
def test_ds_coards_conv(lat_lon_dims, time):
    ds = make_ds(lat_lon_dims=lat_lon_dims, time=time)

    if lat_lon_dims:
        expected_ds_dims = ("lat", "lon")
    else:
        expected_ds_dims = ("y", "x")
    expected_ds_coords = {"lat", "lon"}
    expected_out_dims = ("y", "x")
    expected_out_coords = {"latitude", "longitude", "x", "y"}
    expected_indexes = ["y", "x"]
    if time:
        expected_ds_dims = ("time",) + expected_ds_dims
        expected_ds_coords = {"time"} | expected_ds_coords
        expected_out_dims = ("time",) + expected_out_dims
        expected_out_coords = {"time"} | expected_out_coords
        expected_indexes = ["time", "y", "x"]

    assert ds["lat"].ndim == ds["lon"].ndim == 1
    try:
        assert tuple(ds.dims) == expected_ds_dims
    except AssertionError:
        assert set(ds.dims) == set(expected_ds_dims)
    if lat_lon_dims:
        assert not {"x", "y"} <= ds.variables.keys()
    assert set(ds.coords) == expected_ds_coords

    ds2 = _coards_to_netcdf(ds)

    # x/y dims
    try:
        assert tuple(ds2.dims) == expected_out_dims
    except AssertionError:
        assert set(ds2.dims) == set(expected_out_dims)

    # lat/lon name normalization
    # 2-D lat/lon coords
    assert ds2["latitude"].ndim == ds2["longitude"].ndim == 2
    assert set(ds2.coords) == expected_out_coords

    # correct lat/lon values after meshgrid expansion
    assert (ds2["latitude"].values == ds["lat"].values[:, np.newaxis]).all()
    assert (ds2["longitude"].values == ds["lon"].values[np.newaxis, :]).all()

    # data values preserved
    np.testing.assert_array_equal(ds2["data"].values, ds["data"].values)

    # index set for x and y
    assert list(ds2.indexes) == expected_indexes


@pytest.mark.parametrize(
    "lat_lon_dims",
    [
        pytest.param(True, id="lat-lon-dims"),
        pytest.param(False, id="x-y-dims"),
    ],
)
@pytest.mark.parametrize(
    "time",
    [
        pytest.param(False, id="no-time"),
        pytest.param(True, id="with-time"),
    ],
)
def test_da_coards_conv(lat_lon_dims, time):
    da = make_ds(lat_lon_dims=lat_lon_dims, time=time)["data"]
    da2 = _dataarray_coards_to_netcdf(da)
    assert isinstance(da2, xr.DataArray)

    expected_dims = ("y", "x")
    expected_coords = {"latitude", "longitude", "x", "y"}
    expected_indexes = ["y", "x"]
    if time:
        expected_dims = ("time",) + expected_dims
        expected_coords = {"time"} | expected_coords
        expected_indexes = ["time", "y", "x"]

    # x/y dims
    assert da2.dims == expected_dims

    # lat/lon name normalization
    # 2-D lat/lon coords
    assert set(da2.coords) == expected_coords
    assert da2["latitude"].ndim == da2["longitude"].ndim == 2

    # correct lat/lon values after meshgrid expansion
    assert (da2["latitude"].values == da["lat"].values[:, np.newaxis]).all()
    assert (da2["longitude"].values == da["lon"].values[np.newaxis, :]).all()

    # data values preserved
    np.testing.assert_array_equal(da2.values, da.values)

    # index set for x and y
    assert list(da2.indexes) == expected_indexes
