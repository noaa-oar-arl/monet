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
