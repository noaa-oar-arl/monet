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
    target = target.drop_vars("data_y")
    target.monet.remap_xesmf(source, method="nearest_d2s")
    ds2 = target.copy(deep=True)

    assert np.all(ds1.data == ds2.data), "original data should be same"
    assert not np.all(ds1.data_y == ds2.data_y), "remapped data should be different"
