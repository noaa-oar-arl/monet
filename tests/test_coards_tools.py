
import pytest
import numpy as np
import xarray as xr
import pandas as pd
from monet.util import coards_tools

def make_rectilinear_ds():
    lat = np.linspace(30, 40, 5)
    lon = np.linspace(-90, -80, 4)
    data = np.random.rand(5, 4)
    ds = xr.Dataset({
        'var': (('lat', 'lon'), data)
    }, coords={'lat': lat, 'lon': lon})
    ds['lat'].attrs['standard_name'] = 'latitude'
    ds['lon'].attrs['standard_name'] = 'longitude'
    return ds

def make_curvilinear_ds():
    y, x = np.meshgrid(np.arange(3), np.arange(4), indexing='ij')
    lat2d = 30 + y + 0.1 * x
    lon2d = -90 + x + 0.1 * y
    data = np.random.rand(3, 4)
    ds = xr.Dataset({
        'var': (('y', 'x'), data)
    }, coords={'latitude': (('y', 'x'), lat2d), 'longitude': (('y', 'x'), lon2d)})
    return ds

def test_is_coards_compliant_rectilinear():
    ds = make_rectilinear_ds()
    ds.attrs['Conventions'] = 'CF-1.8'
    assert coards_tools.is_coards_compliant(ds)

def test_is_coards_compliant_curvilinear():
    ds = make_curvilinear_ds()
    ds.attrs['Conventions'] = 'COARDS'
    assert coards_tools.is_coards_compliant(ds)

def test_extract_latlon_dataset():
    ds = make_rectilinear_ds()
    result = coards_tools.extract_latlon_dataset(ds)
    if len(result) == 2:
        lat, lon = result
    else:
        lat, lon, _, _ = result
    np.testing.assert_allclose(lat, ds['lat'])
    np.testing.assert_allclose(lon, ds['lon'])

def test_extract_latlon_dataarray():
    ds = make_rectilinear_ds()
    da = ds['var']
    da = da.assign_coords(lat=ds['lat'], lon=ds['lon'])
    result = coards_tools.extract_latlon_dataarray(da)
    if len(result) == 2:
        lat, lon = result
    else:
        lat, lon, _, _ = result
    np.testing.assert_allclose(lat, ds['lat'])
    np.testing.assert_allclose(lon, ds['lon'])

def test_is_curvilinear_grid():
    ds = make_curvilinear_ds()
    assert coards_tools.is_curvilinear_grid(ds)
    ds_rect = make_rectilinear_ds()
    assert not coards_tools.is_curvilinear_grid(ds_rect)

def test_convert_coards_to_monet_format():
    ds = make_rectilinear_ds()
    from monet.accessors.base import BaseAccessor
    # Should not raise
    monet_ds = coards_tools.convert_coards_to_monet_format(ds)
    assert isinstance(monet_ds, xr.Dataset)

def test_add_cf_attributes():
    ds = make_rectilinear_ds()
    ds2 = coards_tools.add_cf_attributes(ds, foo='bar')
    assert ds2.attrs['Conventions'] == 'CF-1.8'
    assert ds2.attrs['foo'] == 'bar'
    assert 'history' in ds2.attrs

def test_monet_to_coards_rectilinear():
    ds = make_rectilinear_ds()
    ds_cf = coards_tools.monet_to_coards(ds)
    assert 'Conventions' in ds_cf.attrs
    assert 'lat' in ds_cf.coords and 'lon' in ds_cf.coords
    assert ds_cf['lat'].attrs['standard_name'] == 'latitude'
    assert ds_cf['lon'].attrs['standard_name'] == 'longitude'

def test_monet_to_coards_curvilinear():
    ds = make_curvilinear_ds()
    ds_cf = coards_tools.monet_to_coards(ds)
    assert 'Conventions' in ds_cf.attrs
    assert 'latitude' in ds_cf.coords and 'longitude' in ds_cf.coords
    assert ds_cf['latitude'].attrs['standard_name'] == 'latitude'
    assert ds_cf['longitude'].attrs['standard_name'] == 'longitude'

def test_add_cf_standard_names():
    ds = make_rectilinear_ds()
    ds2 = coards_tools.add_cf_standard_names(ds)
    assert 'standard_name' in ds2['var'].attrs
    # Custom mapping
    ds3 = coards_tools.add_cf_standard_names(ds, name_mapping={'var': 'custom_name'})
    assert ds3['var'].attrs['standard_name'] == 'custom_name'
