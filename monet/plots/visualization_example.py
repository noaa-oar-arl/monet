"""Plot a sample field before and after MONET horizontal regridding."""

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from monet.util.resample import resample


def visualize_example():
    """Compare the original field with every supported regridding method."""
    lon = np.linspace(-145.0, 145.0, 72)
    lat = np.linspace(-60.0, 60.0, 36)
    lon_regular, lat_regular = np.meshgrid(lon, lat)
    # Use a gently warped 2D grid so conservative2nd sees great-circle cell
    # edges. AXIS does not yet support its linear reconstruction on
    # constant-latitude edges from a 1D rectilinear grid.
    lons = lon_regular + 1.5 * np.sin(np.deg2rad(lat_regular))
    lats = lat_regular + 0.75 * np.sin(np.deg2rad(lon_regular)) * np.cos(np.deg2rad(lat_regular))
    data = np.sin(np.deg2rad(lons)) * np.cos(np.deg2rad(lats))

    source = xr.DataArray(
        data,
        dims=("lat", "lon"),
        coords={"lat": (("lat", "lon"), lats), "lon": (("lat", "lon"), lons)},
        name="sample_data",
        attrs={"units": "dimensionless"},
    )
    target = xr.Dataset(
        coords={
            "lat": (("lat", "lon"), lats[::2, ::2]),
            "lon": (("lat", "lon"), lons[::2, ::2]),
        }
    )
    for grid in (source, target):
        grid.lat.attrs.update(standard_name="latitude", units="degrees_north")
        grid.lon.attrs.update(standard_name="longitude", units="degrees_east")

    methods = ("nearest", "bilinear", "conservative", "bicubic", "patch", "conservative2nd")
    fields = [("Initial field", source)]
    fields.extend((method, resample(source, target, method=method)) for method in methods)

    fig, axes = plt.subplots(
        4,
        2,
        figsize=(14, 16),
        subplot_kw={"projection": ccrs.PlateCarree()},
        constrained_layout=True,
    )
    for ax, (title, field) in zip(axes.flat, fields):
        field.plot.pcolormesh(
            ax=ax,
            transform=ccrs.PlateCarree(),
            x="lon",
            y="lat",
            vmin=-1,
            vmax=1,
            cmap="coolwarm",
            add_colorbar=True,
            cbar_kwargs={"label": "Sample units"},
        )
        ax.coastlines()
        ax.set_title(title)

    # Hide the unused eighth panel.
    axes.flat[-1].set_visible(False)
    fig.savefig("regridding_methods.png", dpi=150)
    print("Regridding comparison saved to regridding_methods.png")


if __name__ == "__main__":
    visualize_example()
