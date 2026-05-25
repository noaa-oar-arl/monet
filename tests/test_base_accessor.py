import numpy as np
import xarray as xr

from monet.accessors.base import BaseAccessor


def make_rect_ds():
    lat = np.linspace(30, 40, 5)
    lon = np.linspace(-90, -80, 4)
    data = np.random.rand(5, 4)
    ds = xr.Dataset({"var": (("lat", "lon"), data)}, coords={"lat": lat, "lon": lon})
    return ds


def make_da():
    lat = np.linspace(30, 40, 5)
    lon = np.linspace(-90, -80, 4)
    data = np.random.rand(5, 4)
    da = xr.DataArray(data, coords={"lat": lat, "lon": lon}, dims=("lat", "lon"), name="var")
    return da


def test_check_kwargs_and_set_defaults():
    defaults = BaseAccessor._check_kwargs_and_set_defaults()
    assert "method" in defaults and defaults["method"] == "bilinear"
    custom = BaseAccessor._check_kwargs_and_set_defaults(method="nearest")
    assert custom["method"] == "nearest"


def test_rename_latlon():
    ds = make_rect_ds()
    ds2 = BaseAccessor._rename_latlon(ds)
    assert "lat" in ds2.coords and "lon" in ds2.coords


def test_rename_to_monet_latlon():
    ds = make_rect_ds()
    ds2 = BaseAccessor._rename_to_monet_latlon(ds)
    assert "latitude" in ds2.coords and "longitude" in ds2.coords


def test_detect_latlon_names():
    ds = make_rect_ds()
    lat_name, lon_name = BaseAccessor._detect_latlon_names(ds)
    assert lat_name in ds.coords and lon_name in ds.coords


def test_dataset_to_monet():
    ds = make_rect_ds()
    monet_ds = BaseAccessor._dataset_to_monet(ds)
    assert "latitude" in monet_ds.coords and "longitude" in monet_ds.coords


def test_dataarray_to_monet():
    da = make_da()
    monet_da = BaseAccessor._dataset_to_monet(da)
    assert "latitude" in monet_da.coords and "longitude" in monet_da.coords


def test_coards_to_netcdf():
    ds = make_rect_ds()
    result = BaseAccessor._coards_to_netcdf(ds)
    assert "latitude" in result.coords and "longitude" in result.coords
    assert result["latitude"].ndim == 2 and result["longitude"].ndim == 2


def test_dataarray_coards_to_netcdf():
    da = make_da()
    result = BaseAccessor._dataarray_coards_to_netcdf(da)
    assert "latitude" in result.coords and "longitude" in result.coords
    assert result["latitude"].ndim == 2 and result["longitude"].ndim == 2
