# Tutorial

This tutorial demonstrates common workflows in MONET, including data loading, regridding, plotting, statistics, and how to use the MONET accessors.

## Step-by-Step Tutorial: Using MONET with Xarray Toy Datasets

Xarray provides built-in toy datasets perfect for experimenting with MONET.

```python
import monet
import xarray as xr

# Load a toy dataset
ds = xr.tutorial.load_dataset('air_temperature')

# Use MONET accessors for quick visualization
ds['air'].monet.quick_map()

# Find the nearest point to a location
nearest = ds['air'].monet.nearest_latlon(lat=40.0, lon=-100.0)

# Regrid to a coarser grid (for demonstration) using nearest neighbor
coarse = ds['air'].coarsen(time=1, lat=2, lon=2, boundary='trim').mean()
regridded = ds['air'].monet.remap_nearest(coarse)

# Calculate statistics
from monet.util import stats
rmse = stats.RMSE(ds['air'], regridded)
print(f"RMSE: {rmse}")
```

## MONET Accessor Overview

MONET adds tools to xarray and pandas through accessors. These are accessed via the `.monet` attribute after `import monet`.

### Initializing the Accessor

```python
import monet
import xarray as xr
import pandas as pd

ds = xr.open_dataset('model_output.nc')
df = pd.read_csv('obs_points.csv')
```

### Key Accessor Features

- **Plotting:** `quick_map`, `quick_imshow`, `quick_contourf`, `quick_facet_time_map`, `plot_points_map`, `plot_lines_map`
- **Regridding/Interpolation:** `remap_xesmf` (via xregrid), `remap_nearest`, `interp_constant_lat`, `interp_constant_lon`, `stratify`
- **Geospatial utilities:** `nearest_latlon`, `window`, `is_land`, `is_ocean`, `wrap_longitudes`, `tidy`, `structure_for_monet`
- **Combining data:** `combine_point`, `combine_point_esmf`, `combine_da_to_df`

## Common Workflows

### 1. Loading Data
```python
import monet
import xarray as xr
import pandas as pd
ds = xr.open_dataset('model_output.nc')
df = pd.read_csv('obs_points.csv')
```

### 2. Regridding and Interpolation
```python
# Regrid model to obs grid using ESMF (via xregrid)
regridded = ds.monet.remap_xesmf(obs)

# Nearest neighbor regridding
regridded_nn = ds.monet.remap_nearest(obs)

# Interpolate to a constant latitude or longitude
lat_slice = ds['O3'].monet.interp_constant_lat(lat=40.0)

# Stratify by pressure levels
stratified = ds['O3'].monet.stratify(levels=[850, 700, 500], vertical='level')
```

### 3. Quick Map and Faceted Plots
```python
# Quick map for a single time
ds['O3'].isel(time=0).monet.quick_map()

# Faceted map by time
ds['O3'].monet.quick_facet_time_map(time_dim='time', ncols=4)
```

### 4. Taylor Diagram
```python
import numpy as np
from monet.plots.taylordiagram import TaylorDiagram
ref = np.random.normal(0, 1, 100)
model = ref + np.random.normal(0, 0.5, 100)
fig = TaylorDiagram(ref.std())
fig.add_sample(model.std(), np.corrcoef(ref, model)[0, 1], marker='o', label='Model')
```

### 5. Land/Ocean Masking
```python
# Mask land points
land_mask = ds.monet.is_land(return_xarray=True)
ds_land = ds.where(land_mask)
```

### 6. Combining Model and Observations
```python
from monet.util.combinetool import combine_da_to_df
combined = combine_da_to_df(ds['O3'], df)
# Now you can perform direct comparisons
```
