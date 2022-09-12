import cartopy
import xarray as xr
from packaging.version import Version

import monet  # noqa: F401

cartopy_version = Version(cartopy.__version__)

da = xr.tutorial.load_dataset("air_temperature").air


def test_quick_map():
    da.isel(time=1).monet.quick_map()


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    test_quick_map()

    plt.show()
