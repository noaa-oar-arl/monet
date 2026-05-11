# MONET User Guide

This guide is for users of MONET. It provides an overview, installation instructions, and practical examples for common workflows. For a hands-on tutorial, see [Tutorial](tutorial.md).

## Overview

MONET is a Python package for meteorological and air quality data analysis, providing accessors for xarray and pandas objects, utilities for regridding, plotting, and more.

## Installation

The recommended way to install MONET is via conda/mamba:

```bash
conda install -c conda-forge monet
```

For development:

```bash
git clone https://github.com/noaa-oar-arl/monet.git
cd monet
pip install -e .
```

## Quickstart

### Regridding Model Output

MONET uses `xregrid` (based on ESMF/ESMPy) for spatial remapping.

```python
import monet
import xarray as xr

ds = xr.open_dataset('model_output.nc')
obs = xr.open_dataset('obs_data.nc')

# Remap model to obs grid using ESMF (via xregrid)
regridded = ds.monet.remap(obs)
```

### Regridding and UGRID Support

MONET supports both standard gridded data (CF/COARDS) and unstructured grids (UGRID).

```python
# Remap gridded data to target points or grid
regridded = ds.monet.remap(obs, method="bilinear")

# UGRID support: Unstructured grids are automatically detected
ugrid_paired = ugrid_ds.monet.remap(obs_points, method="nearest")
```

Methods include `"bilinear"`, `"nearest"`, `"conservative"`, etc.

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

ds = xr.open_dataset('model_output.nc')
ds['O3'].monet.quick_map()

# Other plotting options:
ds['O3'].monet.quick_imshow()
ds['O3'].monet.quick_contourf(levels=10)
ds['O3'].monet.quick_facet_time_map(time_dim='time', ncols=4)
```

### Working with Pandas DataFrames

```python
import monet
import pandas as pd

df = pd.read_csv('obs_points.csv')
df.monet.plot_points_map()
```

### Comparison and Difference Plots

```python
# Compare two DataArrays
diff = ds['O3'].monet.compare(obs['O3'], stat='diff', plot=True)

# For Datasets
ds.monet.compare(obs, stat='diff', plot=True)
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

MONET's `remap` and `pair` routines are fully UGRID-compliant when using the `xregrid` backend.

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
print(regridded.attrs['history'])
```

## Statistics Utilities (monet-stats)

MONET integrates with `monet-stats` for comprehensive metrics:

- **MB**: Mean Bias
- **RMSE**: Root Mean Square Error
- **MAE**: Mean Absolute Error
- **IOA**: Index of Agreement
- **NMB**: Normalized Mean Bias

```python
from monet.util import stats
import numpy as np

mb = stats.MB(obs, mod)
rmse = stats.RMSE(obs, mod)
```
