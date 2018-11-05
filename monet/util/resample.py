# from pyresample.kd_tree import XArrayResamplerNN
# from pyresample.bilinear.xarr import XArrayResamplerBilinear

# from pyresample.geometry import SwathDefinition, AreaDefinition

# def _ensure_swathdef_compatability(defin):
#     """ensures the SwathDefinition is compatible with XArrayResamplerNN.
#
#     Parameters
#     ----------
#     defin : pyresample SwathDefinition
#         a pyresample.geometry.SwathDefinition instance
#
#     Returns
#     -------
#     type
#         Description of returned object.
#
#     """
#     if isinstance(defin.lons, xr.DataArray):
#         return defin  # do nothing
#     else:
#         defin.lons = xr.DataArray(defin.lons, dims=['y', 'x']).chunk()
#         defin.lats = xr.DataArray(defin.lons, dims=['y', 'x']).chunk()
#         return defin
#
#
# def _check_swath_or_area(defin):
#     """Checks for a SwathDefinition or AreaDefinition. If AreaDefinition do
#     nothing else ensure compatability with XArrayResamplerNN
#
#     Parameters
#     ----------
#     defin : pyresample SwathDefinition or AreaDefinition
#         Description of parameter `defin`.
#
#     Returns
#     -------
#     pyresample.geometry
#         SwathDefinition or AreaDefinition
#
#     """
#     try:
#         if isinstance(defin, SwathDefinition):
#             newswath = _ensure_swathdef_compatability(defin)
#         elif isinstance(defin, AreaDefinition):
#             newswath = defin
#         else:
#             raise RuntimeError
#     except RuntimeError:
#         print('grid definition must be a pyresample SwathDefinition or '
#               'AreaDefinition')
#         return
#     return newswath

# def _reformat_resampled_data(orig, new, target_grid):
#     """reformats the resampled data array filling in coords, name and attrs .
#
#     Parameters
#     ----------
#     orig : xarray.DataArray
#         original input DataArray.
#     new : xarray.DataArray
#         resampled xarray.DataArray
#     target_grid : pyresample.geometry
#         target grid is the target SwathDefinition or AreaDefinition
#
#     Returns
#     -------
#     xarray.DataArray
#         reformated xarray.DataArray
#
#     """
#     target_lon, target_lat = target_grid.get_lonlats_dask()
#     new.name = orig.name
#     new['latitude'] = (('y', 'x'), target_lat)
#     new['longitude'] = (('y', 'x'), target_lon)
#     new.attrs['area'] = target_grid
#     return new


def resample_stratify(da, levels, vertical, axis=1):
    import stratify
    import xarray as xr
    result = stratify.interpolate(
        levels, vertical.chunk(), da.chunk(), axis=axis)
    dims = da.dims
    out = xr.DataArray(result, dims=dims)
    for i in dims:
        if i != 'z':
            out[i] = da[i]
    out.attrs = da.attrs.copy()
    if len(da.coords) > 0:
        for i in da.coords:
            if i != 'z':
                out.coords[i] = da.coords[i]
    return out


def resample_xesmf(source_da, target_da, cleanup=False, **kwargs):
    import xesmf as xe
    regridder = xe.Regridder(source_da, target_da, **kwargs)
    if cleanup:
        regridder.clean_weight_file()
    return regridder(source_da)


#
# def resample_dataset(data,
#                      source_grid,
#                      target_grid,
#                      radius_of_influence=100e3,
#                      resample_cache=None,
#                      return_neighbor_info=False,
#                      neighbours=1,
#                      epsilon=0,
#                      interp='nearest'):
#     # first get the source grid definition
#     try:
#         if source_grid is None:
#             raise RuntimeError
#     except RuntimeError:
#         print('Must include pyresample.gemoetry in the data.attrs area_def or '
#               'area')
#         return
#
#     # check for SwathDefinition or AreaDefinition
#     # if swath ensure it is xarray.DataArray and not numpy for chunking
#     source_grid = _check_swath_or_area(source_grid)
#
#     # set kwargs for XArrayResamplerNN
#     kwargs = dict(
#         source_geo_def=source_grid,
#         target_geo_def=target_grid,
#         radius_of_influence=radius_of_influence,
#         neighbours=neighbours,
#         epsilon=epsilon)
#     if interp is 'nearest':
#         resampler = XArrayResamplerNN(**kwargs)
#     # else:
#     # resampler = XArrayResamplerBilinear(**kwargs)
#
#     # check if resample cash is none else assume it is a dict with keys
#     # [valid_input_index, valid_output_index, index_array, distance_array]
#     # else generate the data
#     if resample_cache is None:
#         valid_input_index, valid_output_index, index_array, distance_array = resampler.get_neighbour_info(
#         )
#     else:
#         resampler.valid_input_index = resample_cache['valid_input_index']
#         resampler.valid_output_index = resample_cache['valid_output_index']
#         resampler.index_array = resample_cache['index_array']
#         resampler.distance_array = resample_cache['distance_array']
#
#     # now store the resampled data temporarily in temp
#     temp = resampler.get_sample_from_neighbour_info(data)
#
#     # reformat data from temp
#     out = _reformat_resampled_data(data, temp, target_grid)
#     if return_neighbor_info:
#         resample_cache = dict(
#             valid_input_index=valid_input_index,
#             valid_output_index=valid_output_index,
#             index_array=index_array,
#             distance_array=distance_array)
#         return out, resample_cache
#     else:
#         return out
