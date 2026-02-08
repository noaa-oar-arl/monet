"""
Visualization Example
=====================

This example demonstrates the Two-Track Rule for Visualization.
"""

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr


def visualize_example():
    """Demonstrate the Two-Track Rule for Visualization."""

    # Create sample data
    nx, ny = 100, 50
    lon = np.linspace(-180, 180, nx)
    lat = np.linspace(-90, 90, ny)
    lons, lats = np.meshgrid(lon, lat)
    data = np.sin(np.deg2rad(lons)) * np.cos(np.deg2rad(lats))

    da = xr.DataArray(data, coords=[("lat", lat), ("lon", lon)], name="sample_data")
    da.attrs["units"] = "dimensionless"

    # Track A: Publication (Matplotlib + Cartopy)
    fig = plt.figure(figsize=(10, 6))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    da.plot(ax=ax, transform=ccrs.PlateCarree(), cbar_kwargs={"label": "Sample Units"})
    ax.coastlines()
    ax.set_title("Track A: Publication-Quality Map")
    plt.savefig("track_a_publication.png")
    print("Track A saved to track_a_publication.png")

    # Track B: Exploration (HvPlot / Geoviews)
    # Note: This is commented out as it requires a browser context,
    # but provided as a code example.
    """
    import hvplot.xarray
    plot = da.hvplot.quadmesh(
        'lon', 'lat',
        projection=ccrs.PlateCarree(),
        rasterize=True,
        cmap='viridis'
    )
    hvplot.save(plot, 'track_b_exploration.html')
    """
    print("Track B: Use hvplot for interactive exploration (e.g., da.hvplot.quadmesh(rasterize=True))")


if __name__ == "__main__":
    visualize_example()
