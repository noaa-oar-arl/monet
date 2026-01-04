import typing as t

from cartopy.mpl.feature_artist import FeatureArtist
from cartopy.mpl.gridliner import Gridliner
import cartopy.crs as ccrs
import matplotlib.axes
import matplotlib.figure
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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
    fig, ax = plots.spatial_plot(spatial_data)

    assert isinstance(fig, matplotlib.figure.Figure)
    assert isinstance(ax, matplotlib.axes.Axes)
    plt.close(fig)


def test_spatial_with_ax(spatial_data: xr.DataArray) -> None:
    """Test the spatial function when an ax is provided."""
    fig_in = plt.figure()
    ax_in = fig_in.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    initial_children = len(ax_in.get_children())

    fig_out, ax_out = plots.spatial_plot(spatial_data, ax=ax_in)

    assert fig_out is fig_in
    assert ax_out is ax_in
    # Check that some plotting has occurred on the axes
    assert len(ax_out.get_children()) > initial_children
    plt.close(fig_in)


def test_spatial_deprecation_warning(spatial_data: xr.DataArray) -> None:
    """Test that the `spatial` function raises a DeprecationWarning."""
    with pytest.warns(DeprecationWarning, match="The function `spatial` is deprecated"):
        plots.spatial(spatial_data)


def test_spatial_map_features(spatial_data: xr.DataArray) -> None:
    """Test that the `spatial` function adds coastlines and gridlines."""
    with pytest.warns(DeprecationWarning):
        fig, ax = plots.spatial(spatial_data)

    # Check for coastlines by inspecting the collections on the axes
    assert any(isinstance(artist, FeatureArtist) for artist in ax.collections), (
        "Coastline artist not found on the axes."
    )

    # Check for gridlines by inspecting the `artists` list on the axes
    assert any(isinstance(artist, Gridliner) for artist in ax.artists), (
        "Gridliner artist not found on the axes."
    )

    plt.close(fig)


def test_spatial_imshow_no_ax(spatial_data: xr.DataArray) -> None:
    """Test the spatial_imshow function when no ax is provided."""
    fig, ax = plots.spatial_imshow(spatial_data)

    assert isinstance(fig, matplotlib.figure.Figure)
    assert isinstance(ax, matplotlib.axes.Axes)
    plt.close(fig)


def test_spatial_imshow_with_ax(spatial_data: xr.DataArray) -> None:
    """Test the spatial_imshow function when an ax is provided."""
    fig_in = plt.figure()
    ax_in = fig_in.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())

    fig_out, ax_out = plots.spatial_imshow(spatial_data, ax=ax_in)

    assert fig_out is fig_in
    assert ax_out is ax_in
    plt.close(fig_in)


@pytest.fixture
def bias_scatter_data() -> t.Tuple[pd.DataFrame, pd.Timestamp]:
    """Create a sample DataFrame for spatial_bias_scatter."""
    data = {
        "latitude": [34.0, 35.0, 36.0],
        "longitude": [-118.0, -119.0, -120.0],
        "CMAQ": [10.0, 12.0, 15.0],
        "Obs": [8.0, 11.0, 16.0],
        "datetime": pd.to_datetime(["2023-01-01", "2023-01-01", "2023-01-01"]),
    }
    df = pd.DataFrame(data)
    date = pd.to_datetime("2023-01-01")
    return df, date


def test_spatial_bias_scatter_with_ax(bias_scatter_data) -> None:
    """Test the spatial_bias_scatter function when an ax is provided."""
    df, date = bias_scatter_data

    # Create a figure and axes with a projection
    fig_in = plt.figure()
    ax_in = fig_in.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    initial_collections = len(ax_in.collections)

    # Call the function with the provided axes
    result = plots.spatial_bias_scatter(df, date, ax=ax_in)

    assert isinstance(result, tuple)
    assert len(result) == 2
    fig_out, ax_out = result

    # Assert that the returned figure and axes are the same as the ones provided
    assert fig_out is fig_in
    assert ax_out is ax_in

    # Check that a scatter plot was added to the axes
    assert len(ax_out.collections) > initial_collections
    plt.close(fig_in)


def test_spatial_bias_scatter_no_ax(bias_scatter_data) -> None:
    """Test the spatial_bias_scatter function when no ax is provided."""
    df, date = bias_scatter_data

    # Call the function without providing an axes
    result = plots.spatial_bias_scatter(df, date)

    # Assert that a new figure and axes are created and returned
    assert isinstance(result, tuple)
    assert len(result) == 2
    fig, ax = result
    assert isinstance(fig, matplotlib.figure.Figure)
    assert isinstance(ax, matplotlib.axes.Axes)

    # Check that a scatter plot was actually created
    assert len(ax.collections) > 0
    plt.close(fig)


def test_spatial_contourf_no_ax(spatial_data: xr.DataArray) -> None:
    """Test the spatial_contourf function when no ax is provided."""
    fig, ax = plots.spatial_contourf(spatial_data)

    assert isinstance(fig, matplotlib.figure.Figure)
    assert isinstance(ax, matplotlib.axes.Axes)
    plt.close(fig)


def test_spatial_contourf_with_ax(spatial_data: xr.DataArray) -> None:
    """Test the spatial_contourf function when an ax is provided."""
    fig_in = plt.figure()
    ax_in = fig_in.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())

    fig_out, ax_out = plots.spatial_contourf(spatial_data, ax=ax_in)

    assert fig_out is fig_in
    assert ax_out is ax_in
    plt.close(fig_in)
