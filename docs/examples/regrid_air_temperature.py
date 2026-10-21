"""Regrid xarray's air_temperature toy dataset with MONET.

Run from an environment with MONET's AXIS backend installed:

    python docs/examples/regrid_air_temperature.py

The tutorial dataset is downloaded by xarray on first use. Its four selected
time steps are interpolated to a larger 360-by-720 source grid, then lazily
regridded to a 180-by-360 target grid with Dask.
"""

import os

# AXIS initializes Kokkos natively. Set conservative worker defaults before
# importing Dask or MONET, while allowing the caller to override them.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OMP_PROC_BIND", "false")

import dask
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from distributed import Client, LocalCluster, wait

from monet.util.resample import create_regridder


def main():
    # Use several times as independent Dask batches. AXIS needs each horizontal
    # core dimension in one chunk, while time stays split for parallel work.
    toy = xr.tutorial.load_dataset("air_temperature")["air"].isel(time=slice(0, 4)).sortby("lat")
    toy = toy.chunk({"time": 1, "lat": -1, "lon": -1})

    # Densify the toy field before regridding: 360 x 720 source, 180 x 360 target.
    source_lat = np.linspace(float(toy.lat[0]), float(toy.lat[-1]), 360)
    source_lon = np.linspace(float(toy.lon[0]), float(toy.lon[-1]), 720)
    source = toy.interp(lat=source_lat, lon=source_lon).chunk({"time": 1, "lat": -1, "lon": -1})
    target = xr.Dataset(
        coords={
            "lat": np.linspace(float(source.lat[0]), float(source.lat[-1]), 180),
            "lon": np.linspace(float(source.lon[0]), float(source.lon[-1]), 360),
        }
    )

    # Use process workers, each with one thread, as in the AXIS scaling example.
    # Persist the input before regridding so source preparation is separate from
    # the regridding graph. Keep the regridder alive until all lazy tasks finish:
    # it owns the weights registered with the workers.
    with LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        dashboard_address=None,
        memory_limit=0,
    ) as cluster:
        with Client(cluster) as client:
            client.wait_for_workers(2)
            worker_source = source.persist()
            wait(worker_source.data)

            regridder = create_regridder(worker_source, target, method="bilinear")
            coarse = regridder(worker_source)
            if coarse.chunks is None:
                raise RuntimeError("Expected AXIS to preserve Dask laziness")
            print(f"Dask source shape/chunks: {worker_source.shape} / {worker_source.chunks}")
            print(f"Dask target shape/chunks: {coarse.shape} / {coarse.chunks}")

            source_plot, coarse_plot = dask.compute(
                worker_source.isel(time=0),
                coarse.isel(time=0),
                scheduler=client,
            )

    fig, axes = plt.subplots(1, 2, figsize=(12, 4), constrained_layout=True)
    source_plot.plot.pcolormesh(ax=axes[0], x="lon", y="lat", add_colorbar=True)
    axes[0].set_title("Air temperature on 360 × 720 source grid")
    coarse_plot.plot.pcolormesh(ax=axes[1], x="lon", y="lat", add_colorbar=True)
    axes[1].set_title("Bilinear regrid to 180 × 360 target")
    fig.savefig("air_temperature_regridded.png", dpi=150)
    print("Wrote air_temperature_regridded.png")


if __name__ == "__main__":
    main()
