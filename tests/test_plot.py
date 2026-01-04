import matplotlib.pyplot as plt
import pytest
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
import numpy as np
import pandas as pd

from monet.plots import plots as p
from monet.plots.mapgen import draw_map

da = xr.tutorial.load_dataset("air_temperature").air.isel(time=1)
lons, lats = np.meshgrid(da.lon, da.lat)
df = pd.DataFrame(
    {
        "latitude": lats.flatten(),
        "longitude": lons.flatten(),
        "CMAQ": da.values.flatten() * 0.9,
        "Obs": da.values.flatten() * 1.1,
        "datetime": pd.to_datetime("2013-01-01 01:00:00"),
    }
)


@pytest.mark.parametrize("which", ["imshow", "map", "contourf"])
@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_quick_with_cartopy_ax(which):
    if not CARTOPY_AVAILABLE:
        pytest.skip("Cartopy is not installed")

    proj = tran = ccrs.PlateCarree()
    _, ax = plt.subplots(subplot_kw=dict(projection=proj))
    getattr(da.monet, f"quick_{which}")(ax=ax, transform=tran)


@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_draw_map_counties():
    _ = draw_map(counties=True, extent=[-110.5, -101, 36, 42])


@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_spatial_plot():
    fig, ax = p.spatial_plot(da)
    assert isinstance(fig, plt.Figure)
    assert isinstance(ax, plt.Axes)


@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_spatial_bias_scatter():
    fig, ax = p.spatial_bias_scatter(df, date=pd.to_datetime("2013-01-01 01:00:00"))
    assert isinstance(fig, plt.Figure)
    assert isinstance(ax, plt.Axes)


@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_spatial_imshow():
    fig, ax = p.spatial_imshow(da)
    assert isinstance(fig, plt.Figure)
    assert isinstance(ax, plt.Axes)


@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_spatial_contourf():
    fig, ax = p.spatial_contourf(da, cmap="viridis", levels=5)
    assert isinstance(fig, plt.Figure)
    assert isinstance(ax, plt.Axes)


@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_wind_quiver():
    u, v = da, da
    fig, ax = p.wind_quiver(u, v)
    assert isinstance(fig, plt.Figure)
    assert isinstance(ax, plt.Axes)


@pytest.mark.skipif(not CARTOPY_AVAILABLE, reason="Cartopy is not installed")
def test_wind_barbs():
    u, v = da, da
    fig, ax = p.wind_barbs(u, v)
    assert isinstance(fig, plt.Figure)
    assert isinstance(ax, plt.Axes)


if __name__ == "__main__":
    test_quick_with_cartopy_ax("map")
    test_draw_map_counties()
    test_spatial_plot()
    test_spatial_bias_scatter()
    test_spatial_imshow()
    test_spatial_contourf()
    test_wind_quiver()
    test_wind_barbs()
    plt.show()
