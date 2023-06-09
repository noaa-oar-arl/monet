import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401
from monet.util.resample import resample_stratify


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
            "height": (("z", "y", "x"), zv[:, None, None]),
        },
        coords={
            "lev": ("z", zv),
            "lat": ("y", yv),
            "lon": ("x", xv),
        },
    )

    if request.param:
        ds = ds.chunk({"z": 1})

    return ds


def test_resample_stratify(model):
    da = model.data1
    old_coord = model.height
    new_coord_vals = xr.DataArray(data=np.linspace(0, 1, 10), dims="z")
    da_interped = resample_stratify(da, new_coord_vals, old_coord, axis=0)

    assert da_interped.dims == ("z", "y", "x")
    assert da_interped.z.size == 10
    assert da_interped.coords == da.reset_coords("lev").coords
    assert da_interped.name == da.name
    assert da_interped.isel(z=0) == da.isel(z=0), "same lb"
    assert da_interped.isel(z=-1) == da.isel(z=-1), "same ub"


def test_accessor_stratify_da(model):
    da = model.data1
    old_coord = model.height
    new_coord_vals = xr.DataArray(data=np.linspace(0, 1, 10), dims="z")
    da_interped = da.monet.stratify(levels=new_coord_vals, vertical=old_coord, axis=0)

    assert da_interped.dims == ("z", "y", "x")
    assert da_interped.z.size == 10
    assert da_interped.coords == da.reset_coords("lev").coords
    assert da_interped.name == da.name


def test_accessor_stratify_ds(model):
    ds = model
    old_coord = model.height
    new_coord_vals = xr.DataArray(data=np.linspace(0, 1, 10), dims="z")
    ds_interped = model.monet.stratify(levels=new_coord_vals, vertical=old_coord, axis=0)

    assert set(ds_interped.dims) == {"z", "y", "x"}
    assert ds_interped.z.size == 10
    assert ds_interped.coords == ds.reset_coords("lev").coords
    assert set(ds_interped.data_vars) == {"data1", "data2"}
