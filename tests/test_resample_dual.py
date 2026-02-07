import sys

import numpy as np
import pytest
import xarray as xr

# Remove the MagicMock for monet_regrid if it exists (from conftest.py)
if "monet_regrid" in sys.modules and hasattr(sys.modules["monet_regrid"], "_mock_methods"):
    del sys.modules["monet_regrid"]

from monet.accessors.base import has_monet_regrid, has_xregrid
from monet.util.resample import resample


def make_test_data(nx=10, ny=10, dask=False):
    ds = xr.Dataset(
        {"var1": (("y", "x"), np.random.rand(ny, nx))},
        coords={
            "latitude": (("y", "x"), np.meshgrid(np.linspace(0, 10, nx), np.linspace(0, 10, ny))[1]),
            "longitude": (("y", "x"), np.meshgrid(np.linspace(0, 10, nx), np.linspace(0, 10, ny))[0]),
        },
    )
    if dask:
        ds = ds.chunk({"y": 5, "x": 5})
    return ds


def make_target_grid(nx=5, ny=5):
    ds = xr.Dataset(
        coords={
            "latitude": (("y", "x"), np.meshgrid(np.linspace(0, 10, nx), np.linspace(0, 10, ny))[1]),
            "longitude": (("y", "x"), np.meshgrid(np.linspace(0, 10, nx), np.linspace(0, 10, ny))[0]),
        },
    )
    return ds


@pytest.mark.skipif(not (has_xregrid or has_monet_regrid), reason="No regridding backend available")
def test_resample_eager():
    source = make_test_data(dask=False)
    target = make_target_grid()

    # Test nearest
    out_nearest = resample(source, target, method="nearest")
    assert out_nearest["var1"].shape == (5, 5)
    assert isinstance(out_nearest["var1"].values, np.ndarray)
    if not has_xregrid and has_monet_regrid:
        assert "monet_regrid" in out_nearest.attrs.get("history", "")

    # Test bilinear
    out_linear = resample(source, target, method="bilinear")
    assert out_linear["var1"].shape == (5, 5)
    assert isinstance(out_linear["var1"].values, np.ndarray)
    if not has_xregrid and has_monet_regrid:
        assert "monet_regrid" in out_linear.attrs.get("history", "")


@pytest.mark.skipif(not (has_xregrid or has_monet_regrid), reason="No regridding backend available")
def test_resample_lazy():
    source = make_test_data(dask=True)
    target = make_target_grid()

    # Test nearest
    out_nearest = resample(source, target, method="nearest")
    assert out_nearest["var1"].shape == (5, 5)
    # Check if it is a dask-backed array
    assert hasattr(out_nearest["var1"].data, "chunks")

    # Test bilinear
    out_linear = resample(source, target, method="bilinear")
    assert out_linear["var1"].shape == (5, 5)
    assert hasattr(out_linear["var1"].data, "chunks")


@pytest.mark.skipif(not (has_xregrid or has_monet_regrid), reason="No regridding backend available")
def test_resample_dataarray():
    source = make_test_data(dask=False)["var1"]
    target = make_target_grid()

    out = resample(source, target, method="nearest")
    assert out.shape == (5, 5)
    assert isinstance(out, xr.DataArray)
