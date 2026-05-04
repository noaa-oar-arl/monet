import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401

# Check if pytspack is available
try:
    import pytspack  # noqa: F401

    PYTSPACK_AVAILABLE = True
except ImportError:
    PYTSPACK_AVAILABLE = False

# Skip all tests if pytspack is not available
skip_if_no_pytspack = pytest.mark.skipif(
    not PYTSPACK_AVAILABLE,
    reason="pytspack not installed",
)


@pytest.fixture(scope="module", params=[False, True], ids=["no-dask", "dask"])
def model(request):
    # "Model" data, profiles, but with dims (z, y, x)
    xv = yv = np.r_[0]
    zv = np.linspace(0, 1, 5)

    x, y = np.meshgrid(xv, yv)
    data1 = np.empty((zv.size, yv.size, xv.size))
    data2 = np.empty_like(data1)

    for i in range(data1.shape[0]):
        data1[i] = i + 0.2 * (1 - y)
        data2[i] = 0.5 * i - 0.2 * (1 - y)

    ds = xr.Dataset(
        data_vars={
            "data1": (("z", "y", "x"), data1),
            "data2": (("z", "y", "x"), data2),
        },
        coords={
            "z": ("z", zv),
            "lat": ("y", yv),
            "lon": ("x", xv),
        },
    )

    if request.param:
        ds = ds.chunk({"z": 1})

    return ds


@skip_if_no_pytspack
def test_interpolate_vertical_da(model):
    from pytspack import interpolate_vertical

    da = model.data1
    target_levels = np.linspace(0, 1, 10)
    result = interpolate_vertical(da, target_levels, level_dim="z")

    assert result.dims == ("z", "y", "x")
    assert result.z.size == 10
    assert result.name == da.name


@skip_if_no_pytspack
def test_interpolate_vertical_ds(model):
    from pytspack import interpolate_vertical

    target_levels = np.linspace(0, 1, 10)
    result = interpolate_vertical(model, target_levels, level_dim="z")

    assert isinstance(result, xr.Dataset)
    assert result["z"].size == 10
    assert "data1" in result.data_vars
    assert "data2" in result.data_vars


@skip_if_no_pytspack
def test_accessor_interpolate_vertical_da(model):
    da = model.data1
    target_levels = np.linspace(0, 1, 10)
    result = da.monet.interpolate_vertical(target_levels, level_dim="z")

    assert result.dims == ("z", "y", "x")
    assert result.z.size == 10
    assert result.name == da.name


@skip_if_no_pytspack
def test_accessor_interpolate_vertical_ds(model):
    target_levels = np.linspace(0, 1, 10)
    result = model.monet.interpolate_vertical(target_levels, level_dim="z")

    assert isinstance(result, xr.Dataset)
    assert result["z"].size == 10
    assert "data1" in result.data_vars
    assert "data2" in result.data_vars


@skip_if_no_pytspack
def test_accessor_stratify_deprecated(model):
    """stratify() still works but raises DeprecationWarning."""
    da = model.data1
    target_levels = xr.DataArray(data=np.linspace(0, 1, 10), dims="z")
    with pytest.warns(DeprecationWarning, match="interpolate_vertical"):
        result = da.monet.stratify(levels=target_levels, vertical="z", axis=0)
    assert result.z.size == 10


@skip_if_no_pytspack
def test_resample_stratify_deprecated(model):
    """resample_stratify() still works but raises DeprecationWarning."""
    from monet.util.resample import resample_stratify

    da = model.data1
    target_levels = np.linspace(0, 1, 10)
    with pytest.warns(DeprecationWarning, match="pytspack.interpolate_vertical"):
        result = resample_stratify(da, target_levels, "z", axis=0)
    assert result.z.size == 10
