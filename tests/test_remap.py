import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401

# Try to import cf_xarray to ensure accessor is registered
try:
    import cf_xarray  # noqa: F401
except ImportError:
    pass

# Check if xesmf and esmpy are available
try:
    import esmpy  # noqa: F401
    import xesmf  # noqa: F401

    has_xesmf = True
except ImportError:
    has_xesmf = False

# Check if xregrid and esmpy are available
try:
    import esmpy  # noqa: F401
    import xregrid  # noqa: F401

    has_xregrid = True
except ImportError:
    has_xregrid = False


@pytest.mark.skipif(not has_xesmf, reason="xesmf not installed")
def test_import_xesmf():
    import xesmf  # noqa: F401


@pytest.mark.skipif(not has_xesmf and not has_xregrid, reason="xesmf/xregrid not installed")
def test_remap_ds_ds():
    # Barry noted a problem with this

    lonmin, latmin, lonmax, latmax = [0, 0, 10, 10]

    def make_ds(*, nx=10, ny=10):
        data = np.arange(nx * ny).reshape((ny, nx))

        return xr.Dataset(
            data_vars={"data": (("y", "x"), data)},
            coords={
                "latitude": ("y", np.linspace(latmin, latmax, ny)),
                "longitude": ("x", np.linspace(lonmin, lonmax, nx)),
            },
        )

    target = make_ds()
    source = make_ds(nx=5)

    # On the data DataArray directly
    target.monet.remap_xesmf(source["data"])

    # On the Dataset
    if "data_y" in target.variables:
        target = target.drop_vars("data_y")

    # Use remap instead of remap_xesmf for new tests generally, but testing backward compat here
    target.monet.remap_xesmf(source, method="nearest_d2s")


@pytest.mark.skipif(not has_xregrid, reason="xregrid not installed")
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

    a = new["data"]
    assert a.shape == (
        model.dims["z"],
        n,
        n,
    ), "model levels but obs grid points (expanded)"
