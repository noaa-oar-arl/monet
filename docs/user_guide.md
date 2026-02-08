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
regridded = ds.monet.remap_xesmf(obs)
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

MONET provides utilities to standardize spatial coordinates to consistent names (`'latitude'`, `'longitude'`).

```python
# Standardize coordinate names and detect UGRID meshes
ds_std = ds.monet.structure_for_monet()
```

## The Aero Protocol (Performance & Provenance)

MONET follows the **Aero Protocol**:

1. **Optional Dask (Laziness):** Routines are backend-agnostic, maintaining laziness on Dask-backed arrays.
2. **Provenance Tracking:** Transformations automatically update the dataset's `history` attribute.
3. **Strict Typing:** Core functions use strict type hints and NumPy-style docstrings.

```python
# Check history
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
