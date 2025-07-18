try:
    from pyresample.future.resamplers import BilinearXarrayResampler
    from pyresample.future.resamplers.nearest import KDTreeNearestXarrayResampler
    from pyresample.geometry import AreaDefinition, SwathDefinition
    from pyresample.kd_tree import XArrayResamplerNN  # noqa: F401

    has_pyresample = True
except ImportError:
    print("PyResample not installed.  Some functionality will be lost")
    has_pyresample = False
try:
    import xesmf  # noqa: F401

    has_xesmf = True
except ImportError:
    has_xesmf = False


def _ensure_swathdef_compatability(defn):
    """Ensures the SwathDefinition is compatible with XArrayResamplerNN.

    Converts longitude and latitude arrays in the SwathDefinition to xarray
    DataArrays if they aren't already, which is required for XArrayResamplerNN.

    Parameters
    ----------
    defn : pyresample.geometry.SwathDefinition
        A :class:`pyresample.geometry.SwathDefinition` instance.

    Returns
    -------
    pyresample.geometry.SwathDefinition
        A compatible SwathDefinition with xarray DataArrays for lons and lats.
    """
    import xarray as xr

    if isinstance(defn.lons, xr.DataArray):
        return defn  # do nothing
    else:
        defn.lons = xr.DataArray(defn.lons, dims=["y", "x"]).chunk()
        defn.lats = xr.DataArray(defn.lons, dims=["y", "x"]).chunk()
        return defn


def _check_swath_or_area(defn):
    """Checks for a SwathDefinition or AreaDefinition. If AreaDefinition do
    nothing else ensure compatibility with XArrayResamplerNN.

    Parameters
    ----------
    defn : pyresample.geometry.SwathDefinition or pyresample.geometry.AreaDefinition
        The grid definition to check and potentially convert.

    Returns
    -------
    pyresample.geometry.SwathDefinition or pyresample.geometry.AreaDefinition
        The (potentially modified) grid definition ensuring compatibility
        with XArrayResamplerNN.
    """
    try:
        if isinstance(defn, SwathDefinition):
            newswath = _ensure_swathdef_compatability(defn)
        elif isinstance(defn, AreaDefinition):
            newswath = defn
        else:
            raise RuntimeError
    except RuntimeError:
        print("grid definition must be a pyresample SwathDefinition or AreaDefinition")
        return
    return newswath


def _reformat_resampled_data(orig, new, target_grid):
    """Reformats the resampled data array filling in coords, name and attrs.

    After resampling, this function ensures the new DataArray has proper
    coordinates, name, and attributes from the original.

    Parameters
    ----------
    orig : xarray.DataArray
        Original input DataArray.
    new : xarray.DataArray
        Resampled xarray.DataArray
    target_grid : pyresample.geometry
        Target grid is the target SwathDefinition or AreaDefinition

    Returns
    -------
    xarray.DataArray
        Reformatted xarray.DataArray with proper coordinates and attributes.
    """
    target_lon, target_lat = target_grid.get_lonlats_dask()
    new.name = orig.name
    new["latitude"] = (("y", "x"), target_lat)
    new["longitude"] = (("y", "x"), target_lon)
    new.attrs["area"] = target_grid
    return new


def resample_stratify(da, levels, vertical, axis=1):
    """Vertically interpolate data to specified levels.

    Uses stratify package to interpolate a DataArray to new vertical levels.

    Parameters
    ----------
    da : xarray.DataArray
        The data to interpolate. Must have a vertical dimension.
    levels : array-like
        The target vertical levels to interpolate to.
    vertical : array-like
        The current vertical coordinate values.
    axis : int, default 1
        The axis representing the vertical dimension.

    Returns
    -------
    xarray.DataArray
        Data interpolated to the new vertical levels, preserving attributes
        and other coordinates.
    """
    import stratify
    import xarray as xr

    result = stratify.interpolate(levels, vertical.chunk().data, da.chunk().data, axis=axis)
    dims = da.dims
    out = xr.DataArray(result, dims=dims, name=da.name)
    out.attrs = da.attrs.copy()
    if len(da.coords) > 0:
        for vn in da.coords:
            if vn != "z" and "z" not in da[vn].dims:
                out[vn] = da[vn].copy()
    return out


def resample_xesmf(
    source_da, target_da, method="bilinear", cleanup=False, parallel=False, n_workers=None, **kwargs
):
    """Resample data from one grid to another using xESMF.

    Uses xESMF to perform regridding between different coordinate systems and grids.

    Parameters
    ----------
    source_da : xarray.DataArray or xarray.Dataset
        The source data to regrid.
    target_da : xarray.DataArray or xarray.Dataset
        Target grid definition.
    method : str, default: 'bilinear'
        Regridding method to use. Options:
        - 'bilinear': Bilinear interpolation
        - 'conservative': First-order conservative
        - 'nearest_s2d': Nearest neighbor source to destination
        - 'nearest_d2s': Nearest neighbor destination to source
    cleanup : bool, default: False
        Whether to clean up the weight file after regridding.
    parallel : bool, default: False
        Whether to use dask for parallel processing.
    n_workers : int, optional
        Number of workers for dask. If None, uses available cores.
    **kwargs : dict
        Additional keyword arguments passed to xesmf.Regridder.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        Regridded data on the target grid.
    """
    if not has_xesmf:
        raise ImportError("xESMF is required for this functionality")

    import xarray as xr
    import xesmf as xe

    # Handle parallel processing
    client = None
    try:
        if parallel:
            try:
                # We need to import these conditionally
                # import dask  # unused
                from dask.distributed import Client, LocalCluster

                # Create a local cluster and client
                if n_workers is None:
                    import multiprocessing as mp

                    n_workers = mp.cpu_count()

                cluster = LocalCluster(n_workers=n_workers)
                client = Client(cluster)
                # dask_client argument is no longer supported in xESMF >=0.7.0; do not add it to kwargs

                # Ensure data is chunked
                if isinstance(source_da, xr.Dataset):
                    source_da = source_da.chunk()
                else:
                    source_da = source_da.chunk()
            except (ImportError, AttributeError) as e:
                print(f"Error setting up parallel processing: {e}")
                # Continue without parallel processing

        # Create the regridder with method as a positional argument
        regridder = xe.Regridder(source_da, target_da, method, **kwargs)

        # Process data based on type
        if isinstance(source_da, xr.Dataset):
            das = {}
            for name, var in source_da.data_vars.items():
                da_result = regridder(var)
                # Add suffix if name would conflict with existing variables in target
                if isinstance(target_da, xr.Dataset) and name in target_da.variables:
                    da_result.name = name + "_y"
                else:
                    da_result.name = name
                das[da_result.name] = da_result
            result = xr.Dataset(das)
            result.attrs = source_da.attrs.copy()
        else:
            result = regridder(source_da)
            # Handle variable naming for DataArrays
            if result.name is None:
                result.name = source_da.name

            # Add suffix if name would conflict with existing variables in target
            if isinstance(target_da, xr.Dataset) and result.name in target_da.variables:
                result.name = result.name + "_y"

        # Clean up weight file if requested
        if cleanup:
            regridder.clean_weight_file()

        return result

    finally:
        # Ensure the client is closed to prevent resource leaks
        if client is not None:
            client.close()


def resample_pyresample_parallel(
    source_data, target_grid, radius_of_influence=1e6, n_processes=None, **kwargs
):
    """Resample data using pyresample with parallel processing.

    Parameters
    ----------
    source_data : xarray.DataArray or xarray.Dataset
        Source data to be regridded.
    target_grid : pyresample.geometry definition
        Target grid definition.
    radius_of_influence : float, default: 1e6
        Search radius in meters.
    n_processes : int, optional
        Number of processes to use for parallel processing.
        If None, uses the number of available CPU cores.
    **kwargs : dict
        Additional keyword arguments passed to pyresample.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        Regridded data on the target grid.
    """
    if not has_pyresample:
        raise ImportError("pyresample is required for this functionality")

    import multiprocessing as mp

    import xarray as xr
    from pyresample import kd_tree

    if n_processes is None:
        n_processes = mp.cpu_count()

    # Ensure source and target are properly defined
    source = _check_swath_or_area(source_data)
    target = _check_swath_or_area(target_grid)

    if isinstance(source_data, xr.DataArray):
        # For DataArrays, use built-in parallelization in pyresample
        kwargs["nprocs"] = n_processes
        result = kd_tree.resample_nearest(
            source,
            source_data.values,
            target,
            radius_of_influence=radius_of_influence,
            fill_value=None,
            **kwargs,
        )

        # Reconstruct DataArray with proper coordinates
        target_lon, target_lat = target.get_lonlats()
        da = xr.DataArray(result, dims=("y", "x"), name=source_data.name)
        da["latitude"] = (("y", "x"), target_lat)
        da["longitude"] = (("y", "x"), target_lon)
        da.attrs = source_data.attrs.copy()

        return da

    elif isinstance(source_data, xr.Dataset):
        # For Datasets, process each variable in parallel
        with mp.Pool(processes=n_processes) as pool:
            results = {}

            # Define worker function for parallel processing
            def process_var(var_name):
                var_data = source_data[var_name]
                result = kd_tree.resample_nearest(
                    source,
                    var_data.values,
                    target,
                    radius_of_influence=radius_of_influence,
                    fill_value=None,
                    **kwargs,
                )
                return var_name, result

            # Process all variables in parallel
            tasks = [(var,) for var in source_data.data_vars]
            results_list = pool.starmap(process_var, tasks)

            # Reconstruct Dataset
            for var_name, result in results_list:
                results[var_name] = (("y", "x"), result)

            # Create the Dataset with results
            target_lon, target_lat = target.get_lonlats()
            ds = xr.Dataset(
                results,
                coords={
                    "latitude": (("y", "x"), target_lat),
                    "longitude": (("y", "x"), target_lon),
                },
            )
            ds.attrs = source_data.attrs.copy()

            return ds
    else:
        raise TypeError("source_data must be an xarray.DataArray or xarray.Dataset")


def resample_pyresample_xarray(source_data, target_grid, radius_of_influence=1e6, **kwargs):
    """Resample data using pyresample's KDTreeNearestXarrayResampler.

    This uses the newer xarray-native resampling functionality from pyresample.future.

    Parameters
    ----------
    source_data : xarray.DataArray or xarray.Dataset
        Source data to be regridded.
    target_grid : pyresample.geometry definition
        Target grid definition.
    radius_of_influence : float, default: 1e6
        Search radius in meters.
    **kwargs : dict
        Additional keyword arguments passed to the resampler.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        Regridded data on the target grid.
    """
    if not has_pyresample:
        raise ImportError("pyresample is required for this functionality")

    import xarray as xr

    # Ensure source and target are properly defined
    source_grid = _check_swath_or_area(source_data.attrs.get("area", None))
    target_grid = _check_swath_or_area(target_grid)

    # Create the resampler
    resampler = KDTreeNearestXarrayResampler(
        source_grid, target_grid, radius_of_influence=radius_of_influence
    )

    # Load the data
    if isinstance(source_data, xr.DataArray):
        resampler.load_array_data(source_data)
        result = resampler.get_array_data()
        # Ensure coordinates and metadata are preserved
        result = _reformat_resampled_data(source_data, result, target_grid)
    elif isinstance(source_data, xr.Dataset):
        resampler.load_dataset_data(source_data)
        result = resampler.get_dataset_data()
        # Add coordinates if not present
        target_lon, target_lat = target_grid.get_lonlats_dask()
        if "latitude" not in result.coords:
            result = result.assign_coords(
                {"latitude": (("y", "x"), target_lat), "longitude": (("y", "x"), target_lon)}
            )
        result.attrs.update(source_data.attrs)
    else:
        raise TypeError("source_data must be an xarray.DataArray or xarray.Dataset")

    return result


def resample(source_data, target_grid, method="nearest", radius_of_influence=1e6, **kwargs):
    """Resample data using various methods with dask parallelization.

    This function provides a high-level API for resampling similar to ndpyramid.

    Parameters
    ----------
    source_data : xarray.DataArray or xarray.Dataset
        Source data to be regridded.
    target_grid : pyresample.geometry definition
        Target grid definition (SwathDefinition or AreaDefinition).
    method : str, default: 'nearest'
        Resampling method. Options:
        - 'nearest': Nearest neighbor interpolation
        - 'bilinear': Bilinear interpolation
    radius_of_influence : float, default: 1e6
        Search radius in meters.
    **kwargs : dict
        Additional keyword arguments passed to the resampler.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        Regridded data on the target grid.

    Notes
    -----
    This function automatically leverages dask for parallelization when the
    input data is already chunked.
    """
    if not has_pyresample:
        raise ImportError("pyresample is required for this functionality")

    import xarray as xr

    # Ensure data is chunked for dask parallelization
    if isinstance(source_data, xr.Dataset):
        if not source_data.chunks:
            source_data = source_data.chunk()
    elif isinstance(source_data, xr.DataArray):
        if not source_data.chunks:
            source_data = source_data.chunk()

    # Select resampling method
    if method.lower() == "nearest":
        return _resample_nearest(source_data, target_grid, radius_of_influence, **kwargs)
    elif method.lower() == "bilinear":
        return _resample_bilinear(source_data, target_grid, radius_of_influence, **kwargs)
    else:
        raise ValueError(
            f"Unsupported resampling method: {method}. "
            f"Supported methods are 'nearest' and 'bilinear'."
        )


def _resample_nearest(source_data, target_grid, radius_of_influence=1e6, **kwargs):
    """Internal function for nearest neighbor resampling using KDTreeNearestXarrayResampler.

    Parameters
    ----------
    source_data : xarray.DataArray or xarray.Dataset
        Source data to be regridded.
    target_grid : pyresample.geometry definition
        Target grid definition.
    radius_of_influence : float, default: 1e6
        Search radius in meters.
    **kwargs : dict
        Additional keyword arguments passed to the resampler.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        Regridded data on the target grid.
    """
    import xarray as xr

    # Handle area attribute from source_data or from kwargs
    if "source_grid" in kwargs:
        source_grid = kwargs.pop("source_grid")
    else:
        source_grid = source_data.attrs.get("area", None)

    # Ensure source and target are properly defined
    source_grid = _check_swath_or_area(source_grid)
    target_grid = _check_swath_or_area(target_grid)

    # Create the resampler
    resampler = KDTreeNearestXarrayResampler(
        source_grid, target_grid, radius_of_influence=radius_of_influence, **kwargs
    )

    # Process data based on type
    if isinstance(source_data, xr.DataArray):
        resampler.load_array_data(source_data)
        result = resampler.get_array_data()
        # Ensure coordinates and metadata are preserved
        result = _reformat_resampled_data(source_data, result, target_grid)
    elif isinstance(source_data, xr.Dataset):
        resampler.load_dataset_data(source_data)
        result = resampler.get_dataset_data()
        # Add coordinates if not present
        target_lon, target_lat = target_grid.get_lonlats_dask()
        if "latitude" not in result.coords:
            result = result.assign_coords(
                {"latitude": (("y", "x"), target_lat), "longitude": (("y", "x"), target_lon)}
            )
        result.attrs.update(source_data.attrs)
    else:
        raise TypeError("source_data must be an xarray.DataArray or xarray.Dataset")

    return result


def _resample_bilinear(source_data, target_grid, radius_of_influence=1e6, **kwargs):
    """Internal function for bilinear resampling using BilinearXarrayResampler.

    Parameters
    ----------
    source_data : xarray.DataArray or xarray.Dataset
        Source data to be regridded.
    target_grid : pyresample.geometry definition
        Target grid definition.
    radius_of_influence : float, default: 1e6
        Search radius in meters.
    **kwargs : dict
        Additional keyword arguments passed to the resampler.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        Regridded data on the target grid.
    """
    import xarray as xr

    # Handle area attribute from source_data or from kwargs
    if "source_grid" in kwargs:
        source_grid = kwargs.pop("source_grid")
    else:
        source_grid = source_data.attrs.get("area", None)

    # Ensure source and target are properly defined
    source_grid = _check_swath_or_area(source_grid)
    target_grid = _check_swath_or_area(target_grid)

    # Create the resampler
    resampler = BilinearXarrayResampler(
        source_grid, target_grid, radius_of_influence=radius_of_influence, **kwargs
    )

    # Process data based on type
    if isinstance(source_data, xr.DataArray):
        resampler.load_array_data(source_data)
        result = resampler.get_array_data()
        # Ensure coordinates and metadata are preserved
        result = _reformat_resampled_data(source_data, result, target_grid)
    elif isinstance(source_data, xr.Dataset):
        resampler.load_dataset_data(source_data)
        result = resampler.get_dataset_data()
        # Add coordinates if not present
        target_lon, target_lat = target_grid.get_lonlats_dask()
        if "latitude" not in result.coords:
            result = result.assign_coords(
                {"latitude": (("y", "x"), target_lat), "longitude": (("y", "x"), target_lon)}
            )
        result.attrs.update(source_data.attrs)
    else:
        raise TypeError("source_data must be an xarray.DataArray or xarray.Dataset")

    return result
