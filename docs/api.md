# API Reference

## Top-level Functions

::: monet
    options:
      members:
        - dataset_to_monet
        - rename_to_monet_latlon
        - rename_latlon

## Plotting Functions

The plotting functions are available in the `monet.plots` module.

::: monet.plots
    options:
      members:
        - cmap_discretize
        - colorbar_index
        - kdeplot
        - normval
        - savefig
        - scatter
        - sp_scatter_bias
        - spatial
        - spatial_bias_scatter
        - spatial_contourf
        - spatial_imshow
        - timeseries
        - wind_barbs
        - wind_quiver
        - taylordiagram_function

## Accessors

MONET extends xarray and pandas objects via accessors. These methods are available through the `.monet` attribute on the respective objects.

### DataArray Accessor

::: monet.accessors.dataarray_accessor.MONETAccessor
    options:
      show_root_heading: false
      members:
        - wrap_longitudes
        - tidy
        - is_land
        - is_ocean
        - cftime_to_datetime64
        - structure_for_monet
        - stratify
        - interp_constant_lat
        - interp_constant_lon
        - nearest_ij
        - nearest_latlon
        - quick_imshow
        - quick_map
        - quick_contourf
        - remap_nearest
        - remap_xesmf
        - combine_point
        - compare
        - quick_facet_time_map
        - remap_nearest_parallel
        - to_area_def
        - to_swath_def

### Dataset Accessor

::: monet.accessors.dataset_accessor.MONETAccessorDataset
    options:
      show_root_heading: false
      members:
        - wrap_longitudes
        - tidy
        - is_land
        - is_ocean
        - cftime_to_datetime64
        - stratify
        - window
        - interp_constant_lat
        - interp_constant_lon
        - nearest_ij
        - nearest_latlon
        - remap_nearest
        - remap_xesmf
        - combine_point
        - remap_nearest_parallel
        - quick_facet_time_map
        - to_area_def
        - to_swath_def

### DataFrame Accessor

::: monet.accessors.pandas_accessor.MONETAccessorPandas
    options:
      show_root_heading: false
      members:
        - to_ascii2nc_df
        - to_ascii2nc_list
        - rename_for_monet
        - get_sparse_SwathDefinition
        - remap_nearest
        - cftime_to_datetime64
        - plot_points_map
        - plot_lines_map
        - center

## Utility Functions

### General Utilities

::: monet.util.tools
    options:
      members:
        - nearest
        - search_listinlist
        - linregress
        - findclosest
        - kolmogorov_zurbenko_filter
        - wsdir2uv
        - long_to_wide
        - calc_8hr_rolling_max
        - calc_24hr_ave
        - calc_3hr_ave
        - calc_annual_ave
        - get_giorgi_region_bounds
        - get_giorgi_region_df
        - calc_13_category_usda_soil_type

### Combine Tool

::: monet.util.combinetool
    options:
      members:
        - combine_da_to_df
        - combine_point
        - combine_point_esmf

### Error Metrics

Statistical metrics are provided by the `monet-stats` package, and exposed through `monet.util.stats`.

::: monet.util.stats
    options:
      members:
        - MB
        - RMSE
        - MAE
        - R2
        - IOA
        - NMB
        - NME
        - FB
        - FE
        - KGE
        - NSE
        - FSS
        - CRPS
        - HSS
        - ETS
        - POD
        - FAR
