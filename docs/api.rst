Get in Touch
------------

Ask questions, suggest features or view source code `on GitHub`_.

If an issue arises please post on the
`GitHub issues <https://github.com/noaa-oar-arl/monet/issues>`__.


API
---

.. module:: monet

.. py:currentmodule:: None

Top-level functions
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: api/
   :recursive:

   monet.dataset_to_monet
   monet.rename_to_monet_latlon
   monet.rename_latlon


Modules
~~~~~~~

.. autosummary::
   :toctree: api/
   :recursive:

   monet.met_funcs
   monet.plots
   monet.util

Plotting Functions
~~~~~~~~~~~~~~~~~~

.. currentmodule:: monet.plots

.. autosummary::
   :toctree: api/
   :template: autosummary/accessor_function.rst

   cmap_discretize
   colorbar_index
   kdeplot
   normval
   savefig
   scatter
   sp_scatter_bias
   spatial
   spatial_bias_scatter
   spatial_contourf
   spatial_imshow
   timeseries
   wind_barbs
   wind_quiver
   wind_barbs
   wind_quiver

.. _xarray-accessors:

DataArray Accessor
~~~~~~~~~~~~~~~~~~

.. currentmodule:: xarray

.. autosummary::
   :toctree: api/
   :template: autosummary/accessor_method.rst

   DataArray.monet.wrap_longitudes
   DataArray.monet.tidy
   DataArray.monet.is_land
   DataArray.monet.is_ocean
   DataArray.monet.cftime_to_datetime64
   DataArray.monet.structure_for_monet
   DataArray.monet.stratify
   DataArray.monet.interp_constant_lat
   DataArray.monet.interp_constant_lon
   DataArray.monet.nearest_ij
   DataArray.monet.nearest_latlon
   DataArray.monet.quick_imshow
   DataArray.monet.quick_map
   DataArray.monet.quick_contourf
   DataArray.monet.remap_nearest
   DataArray.monet.remap_xesmf
   DataArray.monet.combine_point
   DataArray.monet.compare
   DataArray.monet.quick_facet_time_map
   DataArray.monet.remap_nearest_parallel
   DataArray.monet.to_area_def
   DataArray.monet.to_swath_def


Dataset Accessor
~~~~~~~~~~~~~~~~

.. currentmodule:: xarray

.. autosummary::
   :toctree: api/
   :template: autosummary/accessor_method.rst

   Dataset.monet.wrap_longitudes
   Dataset.monet.tidy
   Dataset.monet.is_land
   Dataset.monet.is_ocean
   Dataset.monet.cftime_to_datetime64
   Dataset.monet.stratify
   Dataset.monet.window
   Dataset.monet.interp_constant_lat
   Dataset.monet.interp_constant_lon
   Dataset.monet.nearest_ij
   Dataset.monet.nearest_latlon
   Dataset.monet.remap_nearest
   Dataset.monet.remap_xesmf
   Dataset.monet.combine_point
   Dataset.monet.remap_nearest_parallel
   Dataset.monet.quick_facet_time_map
   Dataset.monet.to_area_def
   Dataset.monet.to_swath_def


.. _pandas-accessors:

DataFrame Accessor
~~~~~~~~~~~~~~~~~~

.. currentmodule:: pandas

.. autosummary::
   :toctree: api/
   :template: autosummary/accessor_method.rst

   DataFrame.monet.to_ascii2nc_df
   DataFrame.monet.to_ascii2nc_list
   DataFrame.monet.rename_for_monet
   DataFrame.monet.get_sparse_SwathDefinition
   DataFrame.monet.remap_nearest
   DataFrame.monet.cftime_to_datetime64
   DataFrame.monet.plot_points_map
   DataFrame.monet.plot_lines_map

.. autosummary::
   :toctree: api/
   :template: autosummary/accessor_attribute.rst

   DataFrame.monet.center





.. _util:

Utility Functions (monet.util)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: monet.util

.. autosummary::
   :toctree: api/
   :template: autosummary/function.rst

   nearest
   search_listinlist
   linregress
   findclosest
   kolmogorov_zurbenko_filter
   wsdir2uv
   long_to_wide
   calc_8hr_rolling_max
   calc_24hr_ave
   calc_3hr_ave
   calc_annual_ave
   get_giorgi_region_bounds
   get_giorgi_region_df
   calc_13_category_usda_soil_type

.. _plots:

Plotting Functions (monet.plots)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: monet.plots

.. autosummary::
   :toctree: api/
   :template: autosummary/function.rst

   taylordiagram_function

.. _on GitHub: https://github.com/noaa-oar-arl/monet
.. _xESMF: https://xesmf.readthedocs.io
.. _pyresample: https://pyresample.readthedocs.io/en/latest/
.. _global-land-mask: https://global-land-mask.readthedocs.io/en/latest/
.. _cartopy: https://scitools.org.uk/cartopy/docs/latest/
.. _matplotlib: https://matplotlib.org
.. _xarray: https://docs.xarray.dev/en/stable/
.. _pandas: https://pandas.pydata.org
.. _dask: https://docs.dask.org/en/stable/
.. _monet-reference: https://monet.readthedocs.io/en/latest/
