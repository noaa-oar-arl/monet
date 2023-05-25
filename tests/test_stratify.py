import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401
from monet.util.resample import resample_stratify


def test_resample_stratify():
    # Model data, profile, increasing up
    xv = yv = np.r_[0]
    zv = np.linspace(0, 1, 5)
    x, y = np.meshgrid(xv, yv)
    data = np.empty((zv.size, yv.size, xv.size))
    for i in range(data.shape[0]):
        data[i] = i + 0.2 * (1 - y)
    model = xr.Dataset(
        data_vars={
            "data": (("z", "y", "x"), data),
            "height": (("z", "y", "x"), zv[:, None, None]),
        },
        coords={
            # "level": ("z", zv),
            "latitude": ("y", yv),
            "longitude": ("x", xv),
        },
    )

    da = model.data
    old_coord = model.height
    new_coord_vals = xr.DataArray(data=np.linspace(0, 1, 10), dims="z")
    da_interped = resample_stratify(da, new_coord_vals, old_coord, axis=0)
