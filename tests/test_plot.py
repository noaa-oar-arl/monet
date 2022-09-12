import cartopy
import pytest
import xarray as xr
from packaging.version import Version

import monet  # noqa: F401

cartopy_version = Version(cartopy.__version__)

da = xr.tutorial.load_dataset("air_temperature").air.isel(time=1)


@pytest.mark.parametrize("which", ["imshow", "map", "contourf"])
def test_quick_(which):
    getattr(da.monet, f"quick_{which}")()


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    test_quick_("map")

    plt.show()
