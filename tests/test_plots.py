import typing as t

import cartopy.crs as ccrs
import matplotlib.axes
import matplotlib.figure
import matplotlib.pyplot as plt
import numpy as np
import pytest
import xarray as xr

from monet.plots import plots
from monet.plots.plots import _thin_data


@pytest.fixture
def wind_data():
    """Create sample wind component data."""
    lat = np.arange(40, 50, 0.5)
    lon = np.arange(-100, -90, 0.5)
    u = xr.DataArray(
        np.random.rand(len(lat), len(lon)),
        coords=[("lat", lat), ("lon", lon)],
    )
    v = xr.DataArray(
        np.random.rand(len(lat), len(lon)),
        coords=[("lat", lat), ("lon", lon)],
    )
    return u, v


def test_thin_data(wind_data):
    """Test the _thin_data helper function."""
    u, v = wind_data
    thin = 5

    u_thinned, v_thinned, lon2d, lat2d = _thin_data(u, v, thin=thin)

    # Check that the dimensions are thinned correctly
    expected_lat_len = len(np.arange(len(u.lat))[::thin])
    expected_lon_len = len(np.arange(len(u.lon))[::thin])
    assert u_thinned.sizes["lat"] == expected_lat_len
    assert u_thinned.sizes["lon"] == expected_lon_len
    assert v_thinned.sizes["lat"] == expected_lat_len
    assert v_thinned.sizes["lon"] == expected_lon_len

    # Check that the meshgrid dimensions are correct
    assert lon2d.shape == (expected_lat_len, expected_lon_len)
    assert lat2d.shape == (u_thinned.sizes["lat"], u_thinned.sizes["lon"])

    # Check that the first and last longitude values in the meshgrid match the thinned coordinates
    assert lon2d[0, 0] == u_thinned.lon.values[0]
    assert lon2d[0, -1] == u_thinned.lon.values[-1]

    # Check that the first and last latitude values in the meshgrid match the thinned coordinates
    assert lat2d[0, 0] == u_thinned.lat.values[0]
    assert lat2d[-1, 0] == u_thinned.lat.values[-1]


@pytest.fixture
def wind_data_yx() -> t.Tuple[xr.DataArray, xr.DataArray]:
    """Create sample wind component data with y/x dimensions."""
    y = np.arange(40, 50, 0.5)
    x = np.arange(-100, -90, 0.5)
    u = xr.DataArray(
        np.random.rand(len(y), len(x)),
        coords=[("y", y), ("x", x)],
    )
    v = xr.DataArray(
        np.random.rand(len(y), len(x)),
        coords=[("y", y), ("x", x)],
    )
    return u, v


def test_thin_data_yx(wind_data_yx: t.Tuple[xr.DataArray, xr.DataArray]) -> None:
    """Test the _thin_data helper function with non-standard y/x dimensions."""
    u, v = wind_data_yx
    thin = 5

    u_thinned, v_thinned, x2d, y2d = _thin_data(u, v, thin=thin)

    # Check that the dimensions are thinned correctly
    expected_y_len = len(np.arange(len(u.y))[::thin])
    expected_x_len = len(np.arange(len(u.x))[::thin])
    assert u_thinned.sizes["y"] == expected_y_len
    assert u_thinned.sizes["x"] == expected_x_len
    assert v_thinned.sizes["y"] == expected_y_len
    assert v_thinned.sizes["x"] == expected_x_len

    # Check that the meshgrid dimensions are correct
    assert x2d.shape == (expected_y_len, expected_x_len)
    assert y2d.shape == (u_thinned.sizes["y"], u_thinned.sizes["x"])

    # Check that the first and last longitude values in the meshgrid match the thinned coordinates
    assert x2d[0, 0] == u_thinned.x.values[0]
    assert x2d[0, -1] == u_thinned.x.values[-1]

    # Check that the first and last latitude values in the meshgrid match the thinned coordinates
    assert y2d[0, 0] == u_thinned.y.values[0]
    assert y2d[-1, 0] == u_thinned.y.values[-1]


@pytest.fixture
def spatial_data() -> xr.DataArray:
    """Create a sample DataArray for spatial plots."""
    lat = np.arange(40, 50, 1)
    lon = np.arange(-100, -90, 1)
    data = np.random.rand(len(lat), len(lon))
    return xr.DataArray(
        data,
        coords=[("lat", lat), ("lon", lon)],
        name="sample_variable",
    )


def test_spatial_no_ax(spatial_data: xr.DataArray) -> None:
    """Test the spatial function when no ax is provided."""
    fig, ax = plots.spatial(spatial_data)

    assert isinstance(fig, matplotlib.figure.Figure)
    assert isinstance(ax, matplotlib.axes.Axes)
    plt.close(fig)


def test_spatial_with_ax(spatial_data: xr.DataArray) -> None:
    """Test the spatial function when an ax is provided."""
    fig_in = plt.figure()
    ax_in = fig_in.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    initial_children = len(ax_in.get_children())

    fig_out, ax_out = plots.spatial(spatial_data, ax=ax_in)

    assert fig_out is fig_in
    assert ax_out is ax_in
    # Check that some plotting has occurred on the axes
    assert len(ax_out.get_children()) > initial_children
    plt.close(fig_in)
