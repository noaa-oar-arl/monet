import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401


def test_remap_ds_ds():
    # Barry noted a problem with this

    lonmin, latmin, lonmax, latmax = [0, 0, 10, 10]

    def make_ds(*, nx=10, ny=10):
        data = np.arange(nx * ny).reshape((ny, nx))
        assert data.flags["C_CONTIGUOUS"]

        return xr.Dataset(
            data_vars={"data": (("y", "x"), data)},
            coords={
                "latitude": ("y", np.linspace(latmin, latmax, ny)),
                "longitude": ("x", np.linspace(lonmin, lonmax, nx)),
            },
        )

    target = make_ds()
    source = make_ds(nx=5)

    # Check for cf accessor
    assert hasattr(target, "cf")
    with pytest.raises(KeyError, match="No results found for 'latitude'."):
        target.cf.get_bounds("latitude")
    assert hasattr(target.monet._obj, "cf")

    # On the DataArray directly works fine
    target.monet.remap_xesmf(source["data"])
    ds1 = target.copy(deep=True)

    # On the Dataset complains
    # Note conservative methods don't work here because need cell bounds
    target.monet.remap_xesmf(source, method="nearest_d2s")
    ds2 = target.copy(deep=True)

    assert np.all(ds1.data == ds2.data), "original data should be same"
    assert not np.all(ds1.data_y == ds2.data_y), "remapped data should be different"
