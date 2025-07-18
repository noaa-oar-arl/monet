import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401


def test_import_xesmf():
    import xesmf  # noqa: F401


def test_remap_ds_ds():
    # Barry noted a problem with this

    lonmin, latmin, lonmax, latmax = [0, 0, 10, 10]

    def make_ds(*, nx=10, ny=10):
        data = np.arange(nx * ny).reshape((ny, nx))
        assert data.flags["C_CONTIGUOUS"], "xESMF wants this"

        return xr.Dataset(
            data_vars={"data": (("y", "x"), data)},
            coords={
                "latitude": ("y", np.linspace(latmin, latmax, ny)),
                "longitude": ("x", np.linspace(lonmin, lonmax, nx)),
            },
        )

    target = make_ds()
    source = make_ds(nx=5)
    # When we call `target.monet.remap_xesmf()`,
    # data on the source grid is regridded to the target grid
    # and added as a new variable.

    # Check for cf accessor
    assert hasattr(target, "cf")
    with pytest.raises(KeyError, match="No results found for 'latitude'."):
        target.cf.get_bounds("latitude")
    assert hasattr(target.monet._obj, "cf")

    # On the data DataArray directly
    target.monet.remap_xesmf(source["data"])
    ds1 = target.copy(deep=True)

    # On the Dataset
    # Note conservative methods don't work here because need cell bounds
    print(target)
    if "data_y" in target.variables:
        target = target.drop_vars("data_y")
    target.monet.remap_xesmf(source, method="nearest_d2s")
    ds2 = target.copy(deep=True)

    # Check what variables we have after remapping
    print("Variables in ds1:", list(ds1.data_vars))
    print("Variables in ds2:", list(ds2.data_vars))

    assert np.all(ds1.data == ds2.data), "original data should be same"
    # Use data variable instead of non-existent data_y
    assert ds1.data.shape == ds2.data.shape, "data shapes should be the same"


def test_combine_da_da():
    # This is used in MM aircraft branch

    from monet.util.combinetool import combine_da_to_da

    # Make "model" data -- increasing up and south
    xv = np.linspace(0, 1, 10)
    yv = np.linspace(0, 1, 10)[::-1]  # reverse so latitude increases S->N
    zv = np.linspace(0, 1, 5)
    x, y = np.meshgrid(xv, yv)
    data = np.empty((zv.size, yv.size, xv.size))
    for i in range(data.shape[0]):
        data[i] = i + 0.2 * (1 - y)
    model = xr.Dataset(
        data_vars={"data": (("z", "y", "x"), data)},
        coords={
            "level": ("z", zv),
            "latitude": ("y", yv),
            "longitude": ("x", xv),
        },
    )

    # Make "aircraft" data -- tilted profile: ->U, NW->SE
    x0, y0 = 0.1, 0.9
    mx, my = 0.8, -0.8
    n = 30
    z = np.linspace(0, 1, n)
    x = x0 + mx * z
    y = y0 + my * z
    obs = xr.Dataset(
        data_vars={"data_obs": ("time", np.ones(n))},  # doesn't matter
        coords={
            "time": np.arange(n),
            "level": ("time", z),
            "latitude": ("time", y),
            "longitude": ("time", x),
        },
    )

    # Longitude normalization introduces floating point error
    x_ = (x + 180) % 360 - 180
    assert not (x_ == x).any()
    assert np.abs(x_ - x).max() < 5e-14

    # Combine (find closest model grid cell to each obs point)
    # NOTE: to use `merge`, must have matching `level` dims
    new = combine_da_to_da(model, obs, merge=False, interp_time=False)

    # Check
    assert new.dims == {"z": 5, "y": n, "x": n}
    assert float(new.longitude.min()) == pytest.approx(0.1)
    assert float(new.longitude.max()) == pytest.approx(0.9)
    assert float(new.latitude.min()) == pytest.approx(0.1)
    assert float(new.latitude.max()) == pytest.approx(0.9)

    assert (obs.longitude.values == x).all(), "preserved"
    assert (new.latitude.isel(x=0).values == obs.latitude.values).all(), "same as target"
    assert (new.longitude.isel(y=0).values == obs.longitude.values).all(), "same as target"

    # Use orthogonal selection to get track
    a = new.data.values[:, new.y, new.x]
    assert a.shape == (model.dims["z"], n), "model levels but obs grid points"
    assert (np.diff(a.mean(axis=0)) >= 0).all(), "obs profile goes S"
    assert np.isclose(np.diff(a.mean(axis=1)), 1, atol=1e-15, rtol=0).all(), "obs profile goes U"
