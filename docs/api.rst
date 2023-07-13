
Get in touch
------------

Ask questions, suggest features or view source code `on GitHub`_.

If an issue arrises please post on the `GitHub` issues.


API
---


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
   DataArray.monet.window
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
   Dataset.monet.remap_nearest_unstructured
   Dataset.monet.remap_xesmf
   Dataset.monet.combine_point


Indices
-------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _on GitHub: https://github.com/noaa-oar-arl/MONET
