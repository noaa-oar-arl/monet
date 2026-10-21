import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401

# Try to import cf_xarray to ensure accessor is registered
try:
    import cf_xarray  # noqa: F401
except ImportError:
    pass

# Check if AXIS are available
try:
    import axis  # noqa: F401

    has_axis = True
except ImportError:
    has_axis = False


@pytest.mark.skipif(not has_axis, reason="AXIS not installed")
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
    target.monet.remap(source["data"])

    # On the Dataset
    if "data_y" in target.variables:
        target = target.drop_vars("data_y")

    target.monet.remap(source, method="nearest_d2s")


@pytest.mark.skipif(not has_axis, reason="AXIS not installed")
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
    assert new.sizes == {"z": 5, "y": n, "x": n}
    assert float(new.longitude.min()) == pytest.approx(0.1)
    assert float(new.longitude.max()) == pytest.approx(0.9)
    assert float(new.latitude.min()) == pytest.approx(0.1)
    assert float(new.latitude.max()) == pytest.approx(0.9)

    assert (obs.longitude.values == x).all(), "preserved"
    assert (new.latitude.isel(x=0).values == obs.latitude.values).all(), "same as target"
    assert (new.longitude.isel(y=0).values == obs.longitude.values).all(), "same as target"

    # Use orthogonal selection to get track
    a = new.data.values[:, new.y, new.x]
    assert a.shape == (model.sizes["z"], n), "model levels but obs grid points"
    assert (np.diff(a.mean(axis=0)) >= 0).all(), "obs profile goes S"
    assert np.isclose(np.diff(a.mean(axis=1)), 1, atol=1e-15, rtol=0).all(), "obs profile goes U"


@pytest.mark.skipif(not has_axis, reason="AXIS not installed")
@pytest.mark.parametrize("method", ["nearest", "bilinear", "conservative"])
def test_axis_legacy_parity(method):
    """References captured with xregrid/ESMPy; see data/regrid_reference/provenance.json.

    Conservative methods infer different edges, so compare correlation and
    area-integrated mass to contract C10 instead of requiring pointwise equality.
    """
    from pathlib import Path

    from monet.util.resample import resample

    root = Path(__file__).parent / "data" / "regrid_reference"
    with xr.open_dataset(root / f"{method}_source.nc") as ds:
        source = ds.load()
    with xr.open_dataset(root / f"{method}_target.nc") as ds:
        target = ds.load()
    with xr.open_dataset(root / f"{method}_expected.nc") as ds:
        expected = ds.load()
    actual = resample(source, target, method=method)
    if method == "conservative":
        assert np.corrcoef(actual.field.values.ravel(), expected.field.values.ravel())[0, 1] >= 0.999
        area = np.cos(np.deg2rad(target.lat))
        old_mass = float((expected.field * area).sum())
        new_mass = float((actual.field * area).sum())
        assert abs(new_mass / old_mass - 1) < 1e-3
    else:
        np.testing.assert_allclose(actual.field, expected.field, rtol=1e-6, atol=1e-8)
