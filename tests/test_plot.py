import pytest
import matplotlib.pyplot as plt
import xarray as xr
from packaging.version import Version

try:
    import cartopy
    import cartopy.crs as ccrs
    cartopy_version = Version(cartopy.__version__)
    CARTOPY_AVAILABLE = True
except ImportError:
    CARTOPY_AVAILABLE = False

import monet  # noqa: F401
from monet.plots.mapgen import draw_map

da = xr.tutorial.load_dataset("air_temperature").air.isel(time=1)

@pytest.mark.parametrize("which", ["imshow", "map", "contourf"])
@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_quick_with_cartopy_ax(which):
    if not CARTOPY_AVAILABLE:
        pytest.skip("Cartopy is not installed")
    import cartopy.crs as ccrs
    proj = tran = ccrs.PlateCarree()
    _, ax = plt.subplots(subplot_kw=dict(projection=proj))
    getattr(da.monet, f"quick_{which}")(ax=ax, transform=tran)

@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_draw_map_counties():
    _ = draw_map(counties=True, extent=[-110.5, -101, 36, 42])

if __name__ == "__main__":
    test_quick_with_cartopy_ax("map")
    test_draw_map_counties()
    plt.show()

