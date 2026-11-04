# MONET User Guide

This guide is for users of MONET. It provides an overview, installation instructions, and practical examples for common workflows. For a hands-on tutorial, see [Tutorial](tutorial.md).

## Overview

MONET is a Python package for meteorological and air quality data analysis, providing accessors for xarray and pandas objects, utilities for regridding, plotting, and more.

## Installation

The recommended way to install MONET is via conda/mamba:

```bash
conda install -c conda-forge monet
```

Or using pip:

```bash
pip install monet
```

For development:

```bash
git clone https://github.com/noaa-oar-arl/monet.git
cd monet
pip install -e .
```


## Note on Efficient Dask Computation

When working with Dask arrays, you can compute multiple results efficiently by calling `dask.compute(a, b, ...)` instead of computing each separately. This allows Dask to share common computations, which is especially useful for related calculations like climatology/anomaly or mean/standard deviation.

## Quickstart

### Regridding Model Output

MONET uses AXIS for spatial remapping.

```python
import monet
import xarray as xr

ds = xr.open_dataset("model_output.nc")
obs = xr.open_dataset("obs_data.nc")

# Remap model to obs grid using AXIS
regridded = obs.monet.remap(ds)
```

### Regridding and UGRID Support

MONET supports both standard gridded data (CF/COARDS) and unstructured grids (UGRID).

```python
# Remap gridded data to target points or grid
regridded = obs.monet.remap(ds, method="bilinear")

# UGRID support: Unstructured grids are automatically detected
ugrid_paired = ugrid_ds.monet.remap(obs_points, method="nearest")
```

Methods include `"bilinear"`, `"nearest"`, `"conservative"`, etc.

### Named NCEP GRIB1 grids

AXIS provides meshes for the NCEP named grids it supports. MONET exposes each
as a coordinate-based xarray Dataset by Table B number, ready to use as a normal MONET
target grid:

```python
from monet.util import get_ncep_grid

target_grid = get_ncep_grid("G003")
regridded = target_grid.monet.remap(source, method="bilinear")
```

`get_ncep_grid` exports AXIS mesh cell centers as xarray coordinates. Set
`as_xarray=False` to get the native `axis.Mesh` for operations that need its
exact cell topology. The xarray form is coordinate based, like other MONET
grids; decoded xarray data continues to use its own coordinates. Coordinate
export uses a temporary Gmsh file and may take extra memory for large grids.
AXIS must support the requested named grid.

### Pairing Model and Observations

The `pair` utility is the primary way to match model data (usually gridded) with observations (usually points). It supports Xarray and Pandas/Dask DataFrames.

#### Pairing with DataFrames (Fixed Sites)

When pairing with a DataFrame, MONET automatically detects latitude, longitude, and site ID columns.

```python
import monet

# Pair model Dataset with observation DataFrame
paired_df = monet.pair(model_ds, obs_df, method="bilinear")

# Also available via accessor
paired_df = model_ds.monet.pair(obs_df)
# or
paired_df = obs_df.monet.pair(model_ds)
```

#### Trajectory Pairing (Moving Platforms)

For moving platforms (like aircraft or ships) where coordinates vary with time, MONET aligns the time dimension before spatial remapping.

```python
# model_ds: (time, y, x)
# obs_ds: (time,) with time-varying 'latitude' and 'longitude' coordinates
paired_traj = monet.pair(model_ds, obs_ds, interp_time=True)
```

#### Gridded-to-Gridded Pairing

You can also pair two gridded datasets. The model will be remapped to the observation grid.

```python
paired_grid = monet.pair(model_ds, obs_gridded_ds)
```

### Plotting Data on a Map

```python
import monet
import xarray as xr

ds = xr.open_dataset("model_output.nc")
ds["O3"].monet.quick_map()

# Other plotting options:
ds["O3"].monet.quick_imshow()
ds["O3"].monet.quick_contourf(levels=10)
ds["O3"].monet.quick_facet_time_map(time_dim="time", ncols=4)
```

### Working with Pandas DataFrames

```python
import monet
import pandas as pd

df = pd.read_csv("obs_points.csv")
df.monet.plot_points_map()
```

### Comparison and Difference Plots

```python
# Compare two DataArrays
diff = ds["O3"].monet.compare(obs["O3"], stat="diff", plot=True)

# For Datasets
ds.monet.compare(obs, stat="diff", plot=True)
```

## COARDS/CF and UGRID Conventions

MONET is convention-aware and provides utilities to work with both standard gridded data (COARDS/CF) and unstructured grids (UGRID).

### Convention-Awareness

Most MONET functions automatically detect latitude and longitude coordinates. This means you can often skip manual renaming steps. Supported detection includes:

*   **Common Names**: `lat`, `latitude`, `lon`, `longitude`, `XLAT`, `XLONG`, etc.
*   **CF Standard Names**: `latitude`, `longitude`, `grid_latitude`, `grid_longitude`.
*   **Units**: Coordinates with units of `degrees_north` or `degrees_east`.
*   **UGRID**: Detects mesh topology and associated node, face, or edge coordinates.

### Standardizing for MONET

If you need to explicitly transform a dataset to use MONET-standard coordinate names (`'latitude'`, `'longitude'`), you can use:

```python
# Rename coordinates to 'latitude'/'longitude' and wrap longitudes to [-180, 180)
ds_std = ds.monet.standardize()
```

For legacy support or specific dimension renaming to `x`/`y`:

```python
# Deprecated: Renames dimensions to x/y and coords to latitude/longitude
ds_std = ds.monet.structure_for_monet()
```

### UGRID Support

MONET's `remap` and `pair` routines are fully UGRID-compliant when using the AXIS backend.

```python
# UGRID datasets are automatically recognized if they have a mesh topology variable
ds_ugrid = xr.open_dataset("unstructured_mesh.nc")
paired = ds_ugrid.monet.pair(obs_df)
```

## Performance & Provenance

MONET is designed for high-performance scientific workflows:

1. **Backend Agnostic (Laziness):** Routines are designed to work with both NumPy and Dask backends, maintaining laziness on Dask-backed arrays to support large-scale data processing.
2. **Provenance Tracking:** Computational steps and transformations automatically update the dataset's `history` attribute, ensuring reproducibility.
3. **Optimized Vectorization:** Core routines leverage `xarray.apply_ufunc` for efficient, parallelized execution across spatial dimensions.

```python
# Check history to see transformations
print(regridded.attrs["history"])
```

## Statistics Utilities (monet-stats)

MONET integrates with `monet-stats` for comprehensive metrics:

- **MB**: Mean Bias
- **RMSE**: Root Mean Square Error
- **MAE**: Mean Absolute Error
- **IOA**: Index of Agreement
- **NMB**: Normalized Mean Bias

Most statistical functions are backend-agnostic and will maintain Dask laziness if the input DataArrays are Dask-backed.

```python
from monet.util import stats
import numpy as np

# Compute metrics (lazily if using Dask)
mb = stats.MB(obs, mod)
rmse = stats.RMSE(obs, mod)

# For Dask-backed results, use .compute() when you need the value
# print(rmse.compute())
```


## Reusing AXIS weights

```python
from monet.util.resample import create_regridder

regridder = create_regridder(model, observations, method="conservative")
out = regridder(model)
regridder.to_file("weights.bin")
loaded = create_regridder(model, observations, method="conservative", weights_file="weights.bin")
out_again = loaded(model)
```

Reuse a weight file only with the same source grid, target grid, and method; the binary
file does not validate grid identity. Dask fields remain lazy while mesh coordinates
and weights are constructed eagerly. Use `skipna=True` for NaN-aware weight
renormalization; unmapped cells follow AXIS's `unmapped` policy.

`linear` maps to `bilinear`. Both legacy `nearest_s2d` and `nearest_d2s` map to AXIS's
nearest source-to-target lookup; the historical destination-to-source accumulation
semantics are not provided. Additional methods are `bicubic`, `patch`, and
`conservative2nd`. Legacy `radius_of_influence` and `n_processes` kwargs are ignored.

## AXIS advanced grids

Pass a full Dataset for UGRID/MPAS so AXIS receives its topology, connectivity,
coordinate metadata, and bounds. UGRID meshes use `cf_role="mesh_topology"`,
`node_coordinates`, `face_node_connectivity`, and connectivity `start_index` metadata.
MPAS datasets include `latVertex`, `lonVertex`, `verticesOnCell`, and cell coordinates.
Cubed-sphere latitude/longitude arrays use `(tile, y, x)` dimensions. Keep the full
mesh Dataset when constructing a persistent regridder, then apply it to its fields.
Periodic/dateline cases can use `periodic=True`.

## Vertical interpolation

```python
profile = model.monet.interpolate_vertical([850, 700, 500], level_dim="pressure", tension=0.0)
```

Vertical interpolation uses AXIS's TSPACK tension spline solver. Dask-backed data
remains lazy, with the complete vertical column evaluated in each task.

Paired horizontal vector components can be regridded together so AXIS can apply
grid-frame rotation:

```python
u_out, v_out = target_grid.monet.remap_vector(u, v, method="bilinear")
```

For rotated grids, pass source and destination angle arrays in radians as
`src_alpha` and `dst_alpha`. Vector regridding supports `bilinear` and `nearest`.
