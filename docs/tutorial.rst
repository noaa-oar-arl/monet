********
Tutorial
********


Step-by-Step Tutorial
Using MONET with Xarray Toy Datasets
====================================

Xarray provides several built-in toy datasets that are perfect for experimenting with MONET's accessors and plotting features, even if you don't have your own data files yet.

For example, you can use the 'air_temperature' dataset:

.. code-block:: python

   import monet
   import xarray as xr

   # Load a toy dataset
   ds = xr.tutorial.load_dataset('air_temperature')
   print(ds)

   # Use MONET accessors for quick visualization and analysis
   ds['air'].monet.quick_map()
   # Find the nearest point to a location
   nearest = ds['air'].monet.nearest_latlon(lat=40.0, lon=-100.0)
   print(nearest)

   # Regrid to a coarser grid (for demonstration)
   coarse = ds['air'].coarsen(time=1, lat=2, lon=2, boundary='trim').mean()
   regridded = ds['air'].monet.remap_nearest(coarse)

   # Calculate statistics
   from monet.util import stats
   rmse = stats.RMSE(ds['air'], regridded)
   print(f"RMSE: {rmse}")

This approach works with any xarray toy dataset (see `xr.tutorial`), and is a great way to learn MONET's API interactively.
===========================================================================================================================

This tutorial demonstrates common and advanced workflows in MONET, including data loading, regridding, plotting, statistics, and especially how to use the MONET accessors for xarray and pandas objects.

MONET Accessor Overview
=======================

MONET adds georeferencing and analysis tools to xarray's data structures and pandas DataFrames through the accessor mechanism. These tools are accessed via the special `.monet` attribute, available for both `xarray.DataArray`, `xarray.Dataset`, and `pandas.DataFrame` objects after a simple `import monet`.

Initializing the Accessor
-------------------------

All you have to do is import monet and xarray (or pandas):

.. code-block:: python

   import monet
   import xarray as xr
   import pandas as pd

   ds = xr.open_dataset('model_output.nc')
   df = pd.read_csv('obs_points.csv')

You can now use `.monet` on your xarray and pandas objects.

Key Accessor Features
---------------------

- **Plotting:** `quick_map`, `quick_imshow`, `quick_contourf`, `quick_facet_time_map`, `plot_points_map`, `plot_lines_map`
- **Regridding/Interpolation:** `remap_xesmf`, `remap_nearest`, `interp_constant_lat`, `interp_constant_lon`, `stratify`
- **Geospatial utilities:** `nearest_latlon`, `window`, `is_land`, `is_ocean`, `wrap_longitudes`, `tidy`, `structure_for_monet`
- **Combining data:** `combine_point`, `combine_point_esmf`, `combine_da_to_df`


1. Loading Data and Initializing Accessors
------------------------------------------
.. code-block:: python

   import monet
   import xarray as xr
   import pandas as pd
   ds = xr.open_dataset('model_output.nc')
   obs = xr.open_dataset('obs_data.nc')
   df = pd.read_csv('obs_points.csv')
   # Now you can use .monet on ds, obs, df, and any DataArray

2. Regridding and Interpolation
-------------------------------
.. code-block:: python

   # Regrid model to obs grid using ESMF/xesmf
   regridded = ds.monet.remap_xesmf(obs)

   # Nearest neighbor regridding
   regridded_nn = ds.monet.remap_nearest(obs)

   # Interpolate to a constant latitude or longitude
   lat_slice = ds['O3'].monet.interp_constant_lat(lat=40.0)
   lon_slice = ds['O3'].monet.interp_constant_lon(lon=-75.0)

   # Stratify by pressure levels
   stratified = ds['O3'].monet.stratify(levels=[850, 700, 500], vertical='level')

3. Quick Map and Faceted Plots
------------------------------
.. code-block:: python

   # Quick map for a single time
   ds['O3'].isel(time=0).monet.quick_map()

   # Faceted map by time
   ds['O3'].monet.quick_facet_time_map(time_dim='time', ncols=4)

4. Custom Map with Cartopy and Projections
------------------------------------------
.. code-block:: python

   from monet.plots import mapgen
   import cartopy.crs as ccrs
   # Custom projection and extent
   ax = mapgen.draw_map(projection=ccrs.LambertConformal(), extent=[-100, -80, 30, 50], coastlines=True, states=True)
   ds['O3'].isel(time=0).monet.quick_map(ax=ax)

5. Taylor Diagram
-----------------
.. code-block:: python

   import numpy as np
   from monet.plots.taylordiagram import TaylorDiagram
   ref = np.random.normal(0, 1, 100)
   model = ref + np.random.normal(0, 0.5, 100)
   stddev = model.std()
   corr = np.corrcoef(ref, model)[0, 1]
   fig = TaylorDiagram(ref.std())
   fig.add_sample(stddev, corr, marker='o', label='Model')

6. Finding Nearest Points and Windows
-------------------------------------
.. code-block:: python

   # Find the nearest grid point to a lat/lon
   nearest = ds.monet.nearest_latlon(lat=40.5, lon=-75.2)
   # Or for a single variable
   nearest_O3 = ds['O3'].monet.nearest_latlon(lat=40.5, lon=-75.2)

   # Extract a spatial window
   window = ds.monet.window(lat_min=39, lon_min=-77, lat_max=41, lon_max=-74)

7. Land/Ocean Masking
---------------------
.. code-block:: python

   # Mask land points
   land_mask = ds.monet.is_land(return_xarray=True)
   ds_land = ds.where(land_mask)

   # Mask ocean points
   ocean_mask = ds.monet.is_ocean(return_xarray=True)
   ds_ocean = ds.where(ocean_mask)

8. Pandas DataFrame Accessor Examples
-------------------------------------
.. code-block:: python

   # Plot points on a map
   df.monet.plot_points_map()

   # Plot lines (e.g., trajectories)
   df.monet.plot_lines_map(group_col='trajectory_id')

   # Remap point data to a grid
   gridded = df.monet.remap_nearest(ds)

9. Calculating Statistics and Chaining Accessors
------------------------------------------------
.. code-block:: python

   from monet.util import stats
   # Calculate RMSE, bias, IOA, etc.
   rmse = stats.RMSE(ds['O3'], obs['O3'])
   mb = stats.MB(ds['O3'], obs['O3'])
   ioa = stats.IOA(ds['O3'], obs['O3'])

   # Chain accessors for advanced workflows
   ds['O3'].monet.tidy().monet.quick_map()

For more, see the :doc:`user_guide` and API documentation.


10. Time Series and Faceted Plots
---------------------------------
.. code-block:: python

   # Plot a time series for a single site or region
   import matplotlib.pyplot as plt
   ds['O3'].sel(latitude=40.0, longitude=-75.0, method='nearest').plot()
   plt.title('Ozone Time Series at Site')
   plt.show()

   # Faceted map plots by time
   ds['O3'].monet.quick_facet_time_map(time_dim='time', ncols=4)

11. Custom Statistics and Accessor Chaining
-------------------------------------------
.. code-block:: python

   # Calculate multiple statistics in one line
   mean = ds['O3'].mean(dim='time')
   std = ds['O3'].std(dim='time')
   max_diff = (ds['O3'] - obs['O3']).max()

   # Chain accessors for advanced workflows
   ds['O3'].monet.tidy().monet.quick_map()

12. Spatial and Ensemble Metrics
--------------------------------
.. code-block:: python

   from monet.util import stats
   # Fractions Skill Score (FSS)
   fss = stats.FSS(ds['O3'], obs['O3'], window=5, threshold=0.5)

   # Continuous Ranked Probability Score (CRPS) for ensemble forecasts
   import numpy as np
   ensemble = np.random.rand(10, 100)  # 10 ensemble members, 100 samples
   obs_arr = np.random.rand(100)
   crps = stats.CRPS(ensemble, obs_arr)

13. Contingency Table Metrics
-----------------------------
.. code-block:: python

   # Heidke Skill Score (HSS), Equitable Threat Score (ETS), Probability of Detection (POD), False Alarm Ratio (FAR)
   hss = stats.HSS(ds['O3'], obs['O3'], minval=0.5)
   ets = stats.ETS(ds['O3'], obs['O3'], minval=0.5)
   pod = stats.POD(ds['O3'], obs['O3'], minval=0.5)
   far = stats.FAR(ds['O3'], obs['O3'], minval=0.5)

14. Using Custom Projections and Map Features
---------------------------------------------
.. code-block:: python

   import cartopy.crs as ccrs
   from monet.plots import mapgen
   ax = mapgen.draw_map(projection=ccrs.LambertConformal(), extent=[-100, -80, 30, 50], coastlines=True, states=True)
   ds['O3'].isel(time=0).monet.quick_map(ax=ax)

15. Combining Model and Observations
------------------------------------
.. code-block:: python

   # Combine model output and point observations for direct comparison
   from monet.util.combinetool import combine_da_to_df
   combined = combine_da_to_df(ds['O3'], df)
   # Now you can plot model vs. obs scatter, time series, etc.

16. Advanced: Working with Unstructured Grids
---------------------------------------------
.. code-block:: python

   # Unstructured grid data (UGRID) is automatically handled by remap
   regridded = ds.monet.remap(obs, method="nearest")

For more advanced workflows, see the :doc:`user_guide` and API documentation.


Accessor Plotting and Comparison Functions
==========================================

MONET provides a suite of plotting functions directly on xarray DataArrays and Datasets via the `.monet` accessor. These make it easy to visualize geospatial data with minimal code.

Plotting Examples
-----------------
.. code-block:: python

   # Quick map (with coastlines, states, etc.)
   ds['O3'].isel(time=0).monet.quick_map()

   # Quick imshow (pixel-based plot)
   ds['O3'].isel(time=0).monet.quick_imshow()

   # Quick filled contour plot
   ds['O3'].isel(time=0).monet.quick_contourf(levels=10, cmap='viridis')

   # Faceted map by time (multiple time steps)
   ds['O3'].monet.quick_facet_time_map(time_dim='time', ncols=4)

   # For Datasets, you can also use the accessor (e.g., ds.monet.quick_facet_time_map(var='O3', ...))

Comparison Example: Using the `compare` Function
------------------------------------------------
.. code-block:: python

   # Compare two DataArrays (e.g., model vs. obs)
   diff = ds['O3'].monet.compare(obs['O3'], stat='diff', plot=True, plot_method='quick_map')
   # This will plot the difference and return the result as a DataArray

   # You can also use other statistics: 'ratio', 'rmse', etc.
   ratio = ds['O3'].monet.compare(obs['O3'], stat='ratio', plot=True)

   # For Datasets, you can compare all variables at once (if they match)
   ds.monet.compare(obs, stat='diff', plot=True)

COARDS/CF Format and Renaming Utilities
=======================================

Many geoscience datasets use different names for latitude/longitude or have different conventions (e.g., 'lat', 'latitude', 'XLAT', etc.). MONET provides utilities to standardize these to COARDS/CF-compliant names ('latitude', 'longitude'), which is required for many MONET functions.

What is COARDS/CF?
------------------
COARDS and CF are conventions for naming and structuring geospatial data in NetCDF files. Using these conventions ensures compatibility with many tools and libraries.

Standardizing Coordinates with MONET
------------------------------------
.. code-block:: python

   # Suppose your dataset uses non-standard names:
   ds = xr.open_dataset('model_output.nc')
   print(ds.coords)  # Might show 'lat', 'lon', or 'XLAT', 'XLONG', etc.

   # Use MONET to standardize:
   ds_std = ds.monet.structure_for_monet()  # Renames to 'latitude', 'longitude' as needed
   print(ds_std.coords)  # Now has 'latitude', 'longitude'

   # You can also use the lower-level utility directly:
   from monet.accessors.base import BaseAccessor
   ds_std = BaseAccessor._dataset_to_monet(ds)

   # If you need to convert back to COARDS/CF-compliant NetCDF:
   ds_coards = BaseAccessor._coards_to_netcdf(ds_std)

   # For DataArrays:
   da_std = BaseAccessor._dataarray_coards_to_netcdf(ds['O3'])

   # These utilities ensure your data is compatible with all MONET features and with other geoscience tools.

For more details, see the API documentation and the source code in `monet/accessors/base.py`.
