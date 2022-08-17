import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401


def test_remap_ds_ds():
    # Barry noted a problem with this

    lonmin, latmin, lonmax, latmax = [0, 0, 10, 10]
    nx = ny = 20

    data = np.ones((ny, nx), dtype=float)
    assert data.flags["C_CONTIGUOUS"]

    ds1 = xr.Dataset(
        data_vars={"data": (("x", "y"), data)},
        coords={
            "latitude": ("y", np.linspace(latmin, latmax, ny)),
            "longitude": ("x", np.linspace(lonmin, lonmax, nx)),
        },
    )

    # Check for cf accessor
    assert hasattr(ds1, "cf")
    with pytest.raises(KeyError, match="No results found for 'latitude'."):
        ds1.cf.get_bounds("latitude")
    assert hasattr(ds1.monet._obj, "cf")

    # On the DataArray directly works fine
    _ = ds1.monet.remap_xesmf(ds1["data"])

    # On the Dataset complains
    _ = ds1.monet.remap_xesmf(ds1)
