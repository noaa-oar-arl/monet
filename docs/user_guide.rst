MONET User Guide
================

This guide is for users of MONET. It provides an overview, installation instructions, and practical examples for common workflows. For a hands-on tutorial, see :doc:`tutorial`.

Overview
--------
MONET is a Python package for meteorological and air quality data analysis, providing accessors for xarray and pandas objects, utilities for regridding, plotting, and more.

Installation
------------

.. code-block:: bash

   pip install monet

Or, for development:

.. code-block:: bash

   git clone https://github.com/bbakernoaa/monet.git
   cd monet
   pip install -e .

Quickstart
----------

Regridding Model Output
~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   import monet
   import xarray as xr

   ds = xr.open_dataset('model_output.nc')
   obs = xr.open_dataset('obs_data.nc')
   regridded = ds.monet.remap_xesmf(obs)


Regridding with xESMF and Pyresample
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
MONET supports two main regridding engines:

- **xESMF** (ESMF/ESMPy): Flexible, supports many interpolation methods (bilinear, conservative, etc.).
- **pyresample**: Fast nearest-neighbor regridding, especially useful for large or irregular grids.

You can choose the method by calling the appropriate accessor function:

.. code-block:: python

   # xESMF (requires xesmf and ESMF/ESMPy installed)
   regridded_xesmf = ds.monet.remap_xesmf(obs, method="bilinear")  # or "nearest_s2d", "conservative", etc.

   # Pyresample (nearest neighbor)
   regridded_nn = ds.monet.remap_nearest(obs, radius_of_influence=1e5)  # radius in meters

   # For DataArrays:
   regridded_xesmf = ds['O3'].monet.remap_xesmf(obs['O3'])
   regridded_nn = ds['O3'].monet.remap_nearest(obs['O3'])

   # You can also regrid to a coarser or custom grid:
   coarse = ds['O3'].coarsen(time=1, lat=2, lon=2, boundary='trim').mean()
   regridded = ds['O3'].monet.remap_nearest(coarse)

Notes:
- xESMF requires the `xesmf` and `ESMF/ESMPy` packages. Install with `pip install xesmf` and see xesmf docs for ESMF/ESMPy setup.
- Pyresample is used automatically for `remap_nearest` if installed (`pip install pyresample`).
- Both methods require latitude and longitude coordinates to be named or standardized (see COARDS/CF section above).

For more advanced options (parallelization, custom weights, etc.), see the API docs and the tutorial.

Plotting Data on a Map
~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   import monet
   import xarray as xr

   ds = xr.open_dataset('model_output.nc')
   ds['O3'].monet.quick_map()

   # Other plotting options:
   ds['O3'].monet.quick_imshow()
   ds['O3'].monet.quick_contourf(levels=10)
   ds['O3'].monet.quick_facet_time_map(time_dim='time', ncols=4)

   # For Datasets:
   ds.monet.quick_facet_time_map(var='O3', time_dim='time', ncols=4)

   # See the tutorial for more advanced plotting examples.

Working with Pandas DataFrames
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   import monet
   import pandas as pd

   df = pd.read_csv('obs_points.csv')
   df.monet.plot_points_map()

Full Tutorial

More Examples

Spatial Plot with Custom Map
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   import monet
   import xarray as xr
   from monet.plots import mapgen

   ds = xr.open_dataset('model_output.nc')
   ax = mapgen.draw_map(extent=[-130, -60, 20, 55])
   ds['O3'].isel(time=0).monet.quick_map(ax=ax)

Taylor Diagram for Model Evaluation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   import monet
   import numpy as np
   from monet.plots.taylordiagram import TaylorDiagram

   ref = np.random.normal(0, 1, 100)
   model = ref + np.random.normal(0, 0.5, 100)
   stddev = model.std()
   corr = np.corrcoef(ref, model)[0, 1]
   fig = TaylorDiagram(ref.std())
   fig.add_sample(stddev, corr, marker='o', label='Model')


Comparison and Difference Plots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
MONET's accessors provide a convenient `compare` function for model-vs-obs or difference plots:

.. code-block:: python

   # Compare two DataArrays (e.g., model vs. obs)
   diff = ds['O3'].monet.compare(obs['O3'], stat='diff', plot=True)
   # Plot ratio, RMSE, etc.:
   ratio = ds['O3'].monet.compare(obs['O3'], stat='ratio', plot=True)

   # For Datasets:
   ds.monet.compare(obs, stat='diff', plot=True)


COARDS/CF Format and Renaming Utilities
---------------------------------------
Many datasets use different names for latitude/longitude. MONET provides utilities to standardize these to COARDS/CF-compliant names ('latitude', 'longitude'), which is required for many MONET functions.

.. code-block:: python

   # Standardize coordinate names
   ds_std = ds.monet.structure_for_monet()
   # Or use the base utility directly:
   from monet.accessors.base import BaseAccessor
   ds_std = BaseAccessor._dataset_to_monet(ds)

   # Convert to COARDS/CF-compliant NetCDF
   ds_coards = BaseAccessor._coards_to_netcdf(ds_std)

See the tutorial for more details and examples.

Stratify Data by Level
~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   # Stratify a DataArray by pressure levels
   stratified = ds['O3'].monet.stratify(levels=[850, 700, 500], vertical='level')

Calculate Statistics
~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   # Calculate RMSE between model and obs
   rmse = np.sqrt(((ds['O3'] - obs['O3']) ** 2).mean())

Working with Land/Ocean Masks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   # Mask land points
   land_mask = ds.monet.is_land(return_xarray=True)
   ds_land = ds.where(land_mask)

   # Mask ocean points
   ocean_mask = ds.monet.is_ocean(return_xarray=True)
   ds_ocean = ds.where(ocean_mask)

See :doc:`tutorial` for a step-by-step tutorial covering more advanced workflows, including stratification, time series analysis, and more.

Further Reading
---------------


Statistics Utilities (monet_stats)
-----------------------------------
MONET provides access to comprehensive statistical metrics for model evaluation through the ``monet_stats`` package. These include error metrics, relative/percentage metrics, correlation and agreement metrics, spatial/ensemble metrics, and contingency metrics.

Commonly used metrics include:

- **MB**: Mean Bias
- **RMSE**: Root Mean Square Error
- **MAE**: Mean Absolute Error
- **R2**: Coefficient of Determination
- **IOA**: Index of Agreement
- **NMB**: Normalized Mean Bias
- **NME**: Normalized Mean Error
- **FB**: Fractional Bias
- **FE**: Fractional Error
- **KGE**: Kling-Gupta Efficiency
- **NSE**: Nash-Sutcliffe Efficiency

All metrics accept numpy arrays, pandas Series, or xarray objects for observed and modeled data.

Example Usage
~~~~~~~~~~~~~
.. code-block:: python

   from monet.util import stats
   import numpy as np

   obs = np.array([1, 2, 3, 4, 5])
   mod = np.array([1.1, 1.9, 3.2, 3.8, 5.1])

   mb = stats.MB(obs, mod)
   rmse = stats.RMSE(obs, mod)
   r2 = stats.R2(obs, mod)
   ioa = stats.IOA(obs, mod)
   nmb = stats.NMB(obs, mod)
   nme = stats.NME(obs, mod)

   print(f"Mean Bias: {mb}")
   print(f"RMSE: {rmse}")
   print(f"R2: {r2}")
   print(f"IOA: {ioa}")
   print(f"NMB: {nmb}")
   print(f"NME: {nme}")

You can also use the ``stats.stats`` function to compute a summary dictionary for a DataFrame with columns ``Obs`` and ``CMAQ``:

.. code-block:: python

   import pandas as pd
   from monet.util import stats

   df = pd.DataFrame({
       'Obs': [1, 2, 3, 4, 5],
       'CMAQ': [1.1, 1.9, 3.2, 3.8, 5.1]
   })
   summary = stats.stats(df, minval=0, maxval=10)
   print(summary)

For a full list of available metrics and their documentation, see the API reference for the ``monet_stats`` package.

More Examples
~~~~~~~~~~~~~

Using with xarray DataArrays
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: python

   import xarray as xr
   import monet_stats as stats

   ds = xr.open_dataset('model_output.nc')
   obs = xr.open_dataset('obs_data.nc')
   mb = stats.MB(ds['O3'], obs['O3'])
   rmse = stats.RMSE(ds['O3'], obs['O3'])

Advanced Metrics
^^^^^^^^^^^^^^^^
.. code-block:: python

   # Kling-Gupta Efficiency
   kge = stats.KGE(obs, mod)

   # Nash-Sutcliffe Efficiency
   nse = stats.NSE(obs, mod)

   # Fractional Bias and Error
   fb = stats.FB(obs, mod)
   fe = stats.FE(obs, mod)

Spatial/Ensemble Metrics
^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: python

   # Fractions Skill Score (FSS)
   fss = stats.FSS(obs, mod, window=5, threshold=0.5)

   # Continuous Ranked Probability Score (CRPS)
   # For ensemble forecasts
   import numpy as np
   ensemble = np.random.rand(10, 100)  # 10 ensemble members, 100 samples
   obs = np.random.rand(100)
   crps = stats.CRPS(ensemble, obs)

Contingency Metrics
^^^^^^^^^^^^^^^^^^^
.. code-block:: python

   # Heidke Skill Score (HSS)
   hss = stats.HSS(obs, mod, minval=0.5)

   # Equitable Threat Score (ETS)
   ets = stats.ETS(obs, mod, minval=0.5)

   # Probability of Detection (POD)
   pod = stats.POD(obs, mod, minval=0.5)

   # False Alarm Ratio (FAR)
   far = stats.FAR(obs, mod, minval=0.5)

See the API reference for a full list of metrics and their arguments.
