# MONET Xarray Accessor

MONET adds georeferencing and analysis tools to xarray's data structures through [their accessor mechanism](https://docs.xarray.dev/en/stable/internals/extending-xarray.html). These tools can be accessed via a special `.monet` attribute, available for both `xarray.DataArray` and `xarray.Dataset` objects after importing `monet`.

## Initializing the Accessor

All you have to do is import `monet` and `xarray`.

```python
import monet
import xarray as xr

# Example opening a dataset (requires monetio or a standard NetCDF)
# ds = xr.open_dataset('model_output.nc')

# After import, the .monet accessor is available
# ds.monet.quick_map()
```

## Interpolation Methods

The MONET accessor provides several useful interpolation routines including:

- Finding the nearest point to a given latitude and longitude
- Interpolating to a constant latitude or longitude
- Vertical interpolation (stratification)
- Remapping entire 2D `DataArray` or `Dataset` using `xregrid`.

### Find Nearest Point

To find the nearest latitude/longitude point, use the `nearest_latlon` method.

```python
# Find the closest grid point to lat=20.5, lon=-157.4
ds.monet.nearest_latlon(lat=20.5, lon=-157.4)
```

If you wanted to only find the nearest location for a single variable, you can use the accessor on the `DataArray`:

```python
ds.O3.monet.nearest_latlon(lat=20.5, lon=-157.4)
```

### Faceted Time Map

Create a grid of map plots for each time slice of a variable.

```python
# Create a 4-column facet grid of Ozone maps
fig, axes = ds.O3.monet.quick_facet_time_map(ncols=4)
```

### Pairing Data

The `.pair()` method provides a high-level interface for matching model data with observations. It is available on all MONET accessors (DataArray, Dataset, and DataFrame).

```python
# Pair model Dataset with observation DataFrame
paired = ds.monet.pair(obs_df)

# Pair model DataArray with observation Dataset (e.g. for trajectory)
paired_traj = ds.O3.monet.pair(obs_ds, interp_time=True)
```

## Convention-Aware Coordinate Detection

MONET is designed to work with various data conventions without requiring destructive renaming of dimensions or coordinates. The accessor automatically detects latitude and longitude based on common naming patterns, CF standard names, units, and UGRID mesh topologies.

### Supported Naming Patterns

MONET searches for a wide range of common names including:
- **Latitude**: `latitude`, `lat`, `y`, `XLAT`, `grid_yt`, `nav_lat`, etc.
- **Longitude**: `longitude`, `lon`, `x`, `XLONG`, `grid_xt`, `nav_lon`, etc.

### CF Convention Support

MONET also checks for:
- `standard_name` attributes like `latitude` or `grid_latitude`.
- `units` attributes such as `degrees_north` or `degrees_east`.

### UGRID Convention Support

For unstructured grids, MONET detects the `mesh_topology` variable (using `cf_role="mesh_topology"`) and identifies coordinates using UGRID attributes:
- `node_coordinates`
- `face_coordinates`
- `edge_coordinates`

### Accessing Coordinates

You can access the detected latitude and longitude objects through the `.monet.lat` and `.monet.lon` properties:

```python
# Access detected latitude coordinate/variable
lat_da = ds.monet.lat

# Access detected longitude coordinate/variable
lon_da = ds.monet.lon
```

These properties work for both Xarray (Dataset/DataArray) and Pandas (DataFrame) objects.

## Utility Methods

### Standardize

The `standardize` method ensures that coordinates have standard CF attributes and that longitudes are wrapped to the `[-180, 180)` range. It also updates the dataset's history.

```python
ds_std = ds.monet.standardize()
```

### Wrap Longitudes

Explicitly wrap longitudes to `[-180, 180)` without modifying other attributes.

```python
ds_wrapped = ds.monet.wrap_longitudes()
```

### Tidy

Applies both longitude wrapping and sorting by longitude.

```python
ds_tidied = ds.monet.tidy()
```

## Comparison and Analysis

### Difference and Statistics

The `compare` method allows for quick comparison between two `DataArray` objects. It automatically aligns the data and can compute various statistics or a simple difference.

```python
# Compute difference (da1 - da2) and plot automatically
ds.O3.monet.compare(obs.O3, stat='diff', plot=True)

# Compute RMSE and return a DataArray (lazily if using Dask)
rmse_da = ds.O3.monet.compare(obs.O3, stat='RMSE', plot=False)
```

Supported statistics include metrics from `monet-stats` (e.g., `RMSE`, `MB`, `MAE`, `NMB`, `IOA`).

### Vertical Interpolation

The `interpolate_vertical` method provides vertical interpolation using pytspack tension splines, following the same API as `pytspack.interpolate_vertical`.

```python
# Interpolate O3 to specific altitude levels
# 'altitude' is the name of the vertical dimension in the dataset
interped = ds.O3.monet.interpolate_vertical([100, 500, 1000], level_dim='altitude')
```

For `Dataset` objects, `interpolate_vertical` will interpolate all variables that have the specified vertical dimension.

```python
# Interpolate entire dataset to pressure levels
ds_interp = ds.monet.interpolate_vertical([850, 700, 500], level_dim='pres')
```

!!! note
    `stratify()` is still available as a deprecated backward-compatibility alias.

## Land and Ocean Masking

MONET includes built-in support for land/ocean masking based on Natural Earth data.

```python
# Get a boolean mask (True for land)
is_land = ds.monet.is_land()

# Return the dataset masked by ocean (preserving only ocean points)
ds_ocean = ds.monet.is_ocean(return_xarray=True)
```
