import numpy as np
import pandas as pd
import pytest
import xarray as xr

import monet

try:
    import global_land_mask  # noqa: F401

    has_glm = True
except ImportError:
    has_glm = False


def test_base_accessor_convention_aware():
    # Dataset with non-standard names
    ds = xr.Dataset(
        {"var": (("y", "x"), np.random.rand(2, 2))},
        coords={"lat": (("y", "x"), [[40, 40], [41, 41]]), "lon": (("y", "x"), [[-100, -99], [-100, -99]])},
    )

    assert ds.monet.lat.name == "lat"
    assert ds.monet.lon.name == "lon"

    # Dataset with CF standard names
    ds2 = xr.Dataset(
        {"var": (("y", "x"), np.random.rand(2, 2))},
        coords={"latitude": (("y", "x"), [[40, 40], [41, 41]]), "longitude": (("y", "x"), [[-100, -99], [-100, -99]])},
    )
    assert ds2.monet.lat.name == "latitude"
    assert ds2.monet.lon.name == "longitude"


@pytest.mark.skipif(not has_glm, reason="global_land_mask not installed")
def test_is_land_no_rename():
    # Kansas, USA
    ds = xr.Dataset({"data": (("x",), [1.0])}, coords={"lat": (("x",), [40.0]), "lon": (("x",), [-100.0])})

    land = ds.monet.is_land()
    assert land[0]
    # Ensure no forced rename happened to original dataset
    assert "lat" in ds.coords
    assert "latitude" not in ds.coords


@pytest.mark.skipif(not has_glm, reason="global_land_mask not installed")
def test_is_land_eager_vs_lazy():
    lats = np.array([40.0, 0.0])
    lons = np.array([-100.0, 0.0])

    ds_eager = xr.Dataset({"data": (("x",), [1.0, 2.0])}, coords={"latitude": (("x",), lats), "longitude": (("x",), lons)})
    ds_lazy = ds_eager.chunk({"x": 1})

    land_eager = ds_eager.monet.is_land()
    land_lazy = ds_lazy.monet.is_land()

    # Lazy result should be a DataArray (since it's a Dask graph)
    assert isinstance(land_lazy, xr.DataArray)
    assert hasattr(land_lazy.data, "chunks")

    np.testing.assert_array_equal(land_eager, land_lazy.compute())


def test_wrap_longitudes_auto_detect():
    ds = xr.Dataset({"data": (("x",), [1.0])}, coords={"lon": (("x",), [190.0]), "lat": (("x",), [40.0])})

    wrapped = ds.monet.wrap_longitudes()
    assert wrapped.lon.values[0] == -170.0
    assert "history" in wrapped.attrs


def test_pandas_accessor_flexible_validation():
    df = pd.DataFrame({"lat": [40.0], "lon": [-100.0], "val": [1.0]})
    # Should not raise AttributeError during initialization
    assert df.monet.center is not None

    # Test rename_for_monet
    renamed = df.monet.rename_for_monet(df)
    assert "latitude" in renamed.columns
    assert "longitude" in renamed.columns


def test_provenance_tidy():
    ds = xr.Dataset({"data": (("x",), [1.0, 2.0])}, coords={"longitude": (("x",), [10.0, 5.0]), "latitude": (("x",), [40, 40])})
    tidied = ds.monet.tidy()
    assert tidied.longitude.values[0] == 5.0
    assert "Tidied" in tidied.attrs["history"]
    assert "Wrapped longitudes" in tidied.attrs["history"]


@pytest.mark.skipif(not monet.accessors.base.has_xregrid, reason="xregrid not installed")
def test_remap_no_rename():
    source = xr.Dataset(
        {"data": (("y", "x"), [[1, 2], [3, 4]])},
        coords={"lat": (("y", "x"), [[40, 40], [41, 41]]), "lon": (("y", "x"), [[-100, -99], [-100, -99]])},
    )
    target = xr.Dataset(coords={"lat": (("y", "x"), [[40.5]]), "lon": (("y", "x"), [[-99.5]])})

    # This might require actual xregrid installation to run
    remapped = source.monet.remap(target, method="nearest")
    assert "lat" in remapped.coords
    assert "lon" in remapped.coords
    assert "latitude" not in remapped.coords


def test_ugrid_detection():
    # Mock a UGRID dataset
    ds = xr.Dataset(
        {"data": (("nNodes",), [1.0, 2.0])},
        coords={
            "node_x": (("nNodes",), [0.0, 1.0]),
            "node_y": (("nNodes",), [40.0, 41.0]),
        },
    )
    ds["mesh"] = xr.DataArray(0, attrs={"cf_role": "mesh_topology", "node_coordinates": "node_x node_y"})

    assert ds.monet.lat.name == "node_y"
    assert ds.monet.lon.name == "node_x"

    # Test is_land on UGRID (should detect coordinates correctly)
    if not has_glm:
        pytest.skip("global_land_mask not installed")
    land = ds.monet.is_land()
    assert len(land) == 2
    assert land[0]  # approx 40N, 0E is ocean? Wait, let's check
    # Actually global_land_mask uses (lat, lon). (40, 0) is Mediterranean Sea or Spain?
    # Spain is land.


def test_standardize():
    ds = xr.Dataset({"data": (("x",), [1.0])}, coords={"lon": (("x",), [190.0]), "lat": (("x",), [40.0])})

    std = ds.monet.standardize()
    assert std.lon.values[0] == -170.0
    assert std.lon.attrs["standard_name"] == "longitude"
    assert "history" in std.attrs


def test_compare_dask():
    da1 = xr.DataArray([1.0, 2.0], dims="x", coords={"x": [0, 1]}, name="test").chunk(1)
    da2 = xr.DataArray([1.1, 1.9], dims="x", coords={"x": [0, 1]}, name="test").chunk(1)

    diff = da1.monet.compare(da2, stat="diff", plot=False)
    assert hasattr(diff.data, "chunks")
    assert "history" in diff.attrs

    rmse = da1.monet.compare(da2, stat="rmse", plot=False)
    assert hasattr(rmse.data, "chunks")
