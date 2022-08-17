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

    # On the DataArray directly works fine
    _ = ds1.monet.remap_xesmf(ds1["data"])

    # On the Dataset complains
    with pytest.raises(
        ValueError, match="Unsupported key-type <class 'xarray.core.variable.Variable'>"
    ):
        _ = ds1.monet.remap_xesmf(ds1)
