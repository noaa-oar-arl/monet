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
def bias_scatter_data_xr() -> xr.Dataset:
    """Create a sample xarray.Dataset for spatial_bias_scatter."""
    data = {
        "latitude": ("station", [34.0, 35.0, 36.0]),
        "longitude": ("station", [-118.0, -119.0, -120.0]),
        "model": ("station", [10.0, 12.0, 15.0]),
        "obs": ("station", [8.0, 11.0, 16.0]),
    }
    ds = xr.Dataset(data)
    ds = ds.set_coords(["latitude", "longitude"])
    return ds


def test_spatial_bias_scatter_xr(bias_scatter_data_xr: xr.Dataset) -> None:
    """Test the xarray-native spatial_bias_scatter function."""
    ds = bias_scatter_data_xr

    # --- Test case 1: No ax provided ---
    fig_out, ax_out = plots.spatial_bias_scatter(ds)
    assert isinstance(fig_out, matplotlib.figure.Figure)
    assert isinstance(ax_out, matplotlib.axes.Axes)
    assert len(ax_out.collections) > 0, "Scatter plot should be added"
    plt.close(fig_out)

    # --- Test case 2: Pre-existing ax provided ---
    fig_in = plt.figure()
    ax_in = fig_in.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    initial_collections = len(ax_in.collections)

    fig_out_2, ax_out_2 = plots.spatial_bias_scatter(ds, ax=ax_in)

    assert fig_out_2 is fig_in
    assert ax_out_2 is ax_in
    assert len(ax_out_2.collections) > initial_collections, (
        "Scatter plot should be added to existing axes"
    )
    plt.close(fig_in)


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


@pytest.fixture
def timeseries_df():
    """Create a sample DataFrame for timeseries plotting."""
    dates = pd.to_datetime(
        [
            "2024-08-01 00:00:00",
            "2024-08-01 01:00:00",
            "2024-08-01 02:00:00",
            "2024-08-01 00:00:00",
            "2024-08-01 01:00:00",
            "2024-08-01 02:00:00",
        ]
    )
    data = {
        "time": dates,
        "obs": [1.0, 1.5, 2.0, 1.2, 1.7, 2.2],
        "model": [0.9, 1.6, 2.1, 1.1, 1.8, 2.3],
        "variable": ["O3"] * 6,
        "units": ["ppb"] * 6,
    }
    return pd.DataFrame(data)


def test_timeseries_plot(timeseries_df):
    """Test the timeseries plotting function."""
    ax = plots.timeseries(timeseries_df, title="Ozone Timeseries", label="Observation")

    assert isinstance(ax, plt.Axes)
    assert ax.get_title() == "Ozone Timeseries"
    assert ax.get_ylabel() == "O3 (ppb)"
    assert len(ax.get_lines()) > 0
    legend = ax.get_legend()
    assert legend is not None
    assert legend.get_texts()[0].get_text() == "Observation"
    plt.close(ax.figure)


@pytest.fixture
def taylor_data() -> t.Tuple[pd.Series, pd.Series, pd.Series]:
    """Create sample data for a Taylor diagram."""
    obs = pd.Series(np.random.rand(100) * 10, name="obs")
    mod1 = pd.Series(np.random.rand(100) * 10 + 2, name="mod1")
    mod2 = pd.Series(np.random.rand(100) * 10 - 2, name="mod2")
    return obs, mod1, mod2


def test_create_taylor_diagram(taylor_data):
    """Test the create_taylor_diagram function."""
    obs, mod1, mod2 = taylor_data

    # --- Test case 1: Create a new diagram ---
    dia = plots.create_taylor_diagram(obs, mod1, model_label="Model 1")

    assert dia is not None
    # The constructor adds the reference point, and create_taylor_diagram adds the model
    assert len(dia.samplePoints) == 2
    labels = [p.get_label() for p in dia.samplePoints]
    assert "Model 1" in labels
    assert "Reference" in labels  # Default obs_label
    initial_fig = dia.fig

    # --- Test case 2: Add a second model to the existing diagram ---
    dia = plots.create_taylor_diagram(obs, mod2, model_label="Model 2", dia=dia)

    assert dia.fig is initial_fig  # Should be the same figure
    assert len(dia.samplePoints) == 3  # Ref + 2 models
    labels = [p.get_label() for p in dia.samplePoints]
    assert "Model 1" in labels
    assert "Model 2" in labels
    assert "Reference" in labels
    plt.close(dia.fig)
