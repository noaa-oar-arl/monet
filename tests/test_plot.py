import cartopy
import pytest
import xarray as xr
from packaging.version import Version

import monet  # noqa: F401

cartopy_version = Version(cartopy.__version__)

da = xr.tutorial.load_dataset("air_temperature").air


@pytest.mark.xfail(
    cartopy_version >= Version("0.21.0"), reason="removed `GeoAxes.outline_patch` attr"
)
def test_quick_map():
    da.monet.quick_map()
