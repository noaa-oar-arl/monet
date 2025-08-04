
try:
    from pyresample.future.resamplers.nearest import KDTreeNearestXarrayResampler
    from pyresample.future.geometry.area import AreaDefinition
    from pyresample.future.geometry.swath import SwathDefinition
    from pyresample.kd_tree import XArrayResamplerNN  # noqa: F401
    from pyresample import bilinear  # For XArrayBilinearResampler

    has_pyresample = True
except ImportError:
    print("pyresample not installed.  Some functionality will be lost")
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
    import xarray as xr
    import numpy as np
    target_lon, target_lat = target_grid.get_lonlats_dask()
    # If new is a numpy array, wrap it as a DataArray
    if isinstance(new, np.ndarray):
        # Use dims from the resampled data (target grid), else default to ("y", "x")
        dims = orig.dims if hasattr(orig, "dims") and new.shape == orig.shape else ("y", "x")
        if new.shape == target_lat.shape:
            dims = ("y", "x")
        new = xr.DataArray(new, dims=dims)
    new.name = orig.name
    # Assign latitude/longitude using the dims of the resampled data
    y_dim, x_dim = new.dims[-2], new.dims[-1] if len(new.dims) >= 2 else ("y", "x")
    # Always use .data or .values to avoid DataArray ambiguity
    lat_data = getattr(target_lat, 'data', getattr(target_lat, 'values', target_lat))
    lon_data = getattr(target_lon, 'data', getattr(target_lon, 'values', target_lon))
    # Ensure the shapes match before assignment
    if (target_lat.shape == new.shape[-2:]) and (target_lon.shape == new.shape[-2:]):
        new["latitude"] = ((y_dim, x_dim), lat_data)
        new["longitude"] = ((y_dim, x_dim), lon_data)
    else:
        # Fallback: assign as 1D if possible, else skip
        if target_lat.ndim == 1 and target_lat.shape[0] == new.shape[-2]:
            new["latitude"] = (y_dim, lat_data)
        if target_lon.ndim == 1 and target_lon.shape[0] == new.shape[-1]:
            new["longitude"] = (x_dim, lon_data)
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
    try:
        from stratify import interpolate
    except ImportError:
        import stratify
        if hasattr(stratify, "interpolate"):
            interpolate = stratify.interpolate
        else:
            raise ImportError("stratify.interpolate not available; please install stratify package.")
    import xarray as xr

    result = interpolate(levels, vertical.chunk().data, da.chunk().data, axis=axis)
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
                # Only assign .name if da_result is an xarray.DataArray
                if isinstance(da_result, xr.DataArray):
                    if isinstance(target_da, xr.Dataset) and name in target_da.variables:
                        da_result = da_result.copy()
                        da_result.name = name + "_y"
                        das[name + "_y"] = da_result
                    else:
                        da_result = da_result.copy()
                        da_result.name = name
                        das[name] = da_result
                else:
                    # fallback: just store by name
                    das[name] = da_result
            result = xr.Dataset(das)
            result.attrs = source_da.attrs.copy()
        else:
            result = regridder(source_da)
            # Only assign/access .name if result is an xarray.DataArray
            if isinstance(result, xr.DataArray) and result.name is None:
                result = result.copy()
                result.name = source_da.name

            # Add suffix if name would conflict with existing variables in target
            if isinstance(result, xr.DataArray) and isinstance(target_da, xr.Dataset) and result.name in target_da.variables:
                result = result.copy()
                result.name = str(result.name) + "_y"

        # Clean up weight file if requested
        if cleanup and hasattr(regridder, "clean_weight_file"):
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
    elif method.lower() == "gradient":
        raise NotImplementedError("Gradient search resampling is not yet implemented.")
    else:
        raise ValueError(
            f"Unsupported resampling method: {method}. "
            f"Supported methods are 'nearest' and 'bilinear'."
        )
def _resample_bilinear(source_data, target_grid, radius_of_influence=1e6, **kwargs):
    """Internal function for bilinear resampling using XArrayBilinearResampler from pyresample.bilinear.

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
    from pyresample import bilinear

    # Handle area attribute from source_data or from kwargs
    if "source_grid" in kwargs:
        source_grid = kwargs.pop("source_grid")
    else:
        source_grid = source_data.attrs.get("area", None)

    # Ensure source and target are properly defined
    source_grid = _check_swath_or_area(source_grid)
    target_grid = _check_swath_or_area(target_grid)
    if source_grid is None or target_grid is None:
        raise ValueError("source_grid and target_grid must be valid SwathDefinition or AreaDefinition objects, not None.")

    # Create the resampler
    resampler = bilinear.XArrayBilinearResampler(
        source_grid, target_grid, radius_of_influence=radius_of_influence, **kwargs
    )

    # Process data based on type
    if isinstance(source_data, xr.DataArray):
        resampler.load_array_data(source_data)
        result = resampler.resample(source_data)
        # Ensure coordinates and metadata are preserved
        result = _reformat_resampled_data(source_data, result, target_grid)
    elif isinstance(source_data, xr.Dataset):
        resampler.load_dataset_data(source_data)
        result = resampler.resample(source_data)
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


    # Auto-create AreaDefinition for regular/curvilinear grids, SwathDefinition for irregular
    from pyresample.future.geometry.area import AreaDefinition
    from pyresample.future.geometry.swath import SwathDefinition
    import numpy as np
    def to_griddef(obj, fallback=None):
        """
        Try to auto-create AreaDefinition (preferred) or SwathDefinition from xarray object.
        AreaDefinition is used for both regular and curvilinear grids (2D lat/lon),
        SwathDefinition is only used for truly irregular grids (1D lat/lon).
        """
        candidates = [obj]
        if fallback is not None:
            candidates.append(fallback)
        for candidate in candidates:
            if candidate is None:
                continue
            if isinstance(candidate, AreaDefinition):
                return candidate
            if isinstance(candidate, SwathDefinition):
                return _ensure_swathdef_compatability(candidate)
            lats = lons = None
            # Check .coords
            if hasattr(candidate, 'coords'):
                coords = candidate.coords
                if 'latitude' in coords and 'longitude' in coords:
                    lats = coords['latitude'].values
                    lons = coords['longitude'].values
            # Check .data_vars (for Dataset)
            if (lats is None or lons is None) and hasattr(candidate, 'data_vars'):
                data_vars = candidate.data_vars
                if 'latitude' in data_vars and 'longitude' in data_vars:
                    lats = data_vars['latitude'].values
                    lons = data_vars['longitude'].values
            # Check .variables (for older xarray)
            if (lats is None or lons is None) and hasattr(candidate, 'variables'):
                variables = candidate.variables
                if 'latitude' in variables and 'longitude' in variables:
                    lats = variables['latitude'].values
                    lons = variables['longitude'].values
            # Check as attributes (rare)
            if (lats is None or lons is None) and hasattr(candidate, 'latitude') and hasattr(candidate, 'longitude'):
                lats = getattr(candidate, 'latitude')
                lons = getattr(candidate, 'longitude')
                if hasattr(lats, 'values'):
                    lats = lats.values
                if hasattr(lons, 'values'):
                    lons = lons.values
            # If not found, check for lons/lats attributes directly (for pyresample objects)
            if (lats is None or lons is None) and hasattr(candidate, 'lons') and hasattr(candidate, 'lats'):
                lons = getattr(candidate, 'lons')
                lats = getattr(candidate, 'lats')
            # If we found lat/lon, try to create AreaDefinition (preferred)
            if lats is not None and lons is not None:
                # If both are 2D and shapes match, use AreaDefinition
                if lats.ndim == 2 and lons.ndim == 2 and lats.shape == lons.shape:
                    print(f"[DEBUG] Creating AreaDefinition: area_id={getattr(candidate, 'name', 'curvilinear')}, shape={lons.shape}, area_extent={float(np.nanmin(lons)), float(np.nanmin(lats)), float(np.nanmax(lons)), float(np.nanmax(lats))}")
                    print(f"[DEBUG] lats min/max: {np.nanmin(lats)}, {np.nanmax(lats)}; lons min/max: {np.nanmin(lons)}, {np.nanmax(lons)}")
                    print(f"[DEBUG] lats shape: {lats.shape}, lons shape: {lons.shape}")
                    area_id = getattr(candidate, 'name', 'curvilinear')
                    proj_dict = {'proj': 'latlong'}
                    shape = lons.shape
                    area_extent = [float(np.nanmin(lons)), float(np.nanmin(lats)), float(np.nanmax(lons)), float(np.nanmax(lats))]
                    areadef = AreaDefinition.from_extent(area_id, proj_dict, shape, area_extent)
                    return areadef
                # If both are 1D, use SwathDefinition
                elif lats.ndim == 1 and lons.ndim == 1:
                    swath = SwathDefinition(lons=lons, lats=lats)
                    return _ensure_swathdef_compatability(swath)
                # If one is 2D and the other is 1D, try to meshgrid and use AreaDefinition
                elif (lats.ndim == 2 and lons.ndim == 1) or (lats.ndim == 1 and lons.ndim == 2):
                    # Try to meshgrid
                    if lats.ndim == 1 and lons.ndim == 2:
                        lats, lons = np.meshgrid(lats, lons)
                    elif lats.ndim == 2 and lons.ndim == 1:
                        lons, lats = np.meshgrid(lons, lats)
                    print(f"[DEBUG] Creating AreaDefinition (meshgrid): area_id={getattr(candidate, 'name', 'curvilinear')}, shape={lons.shape}, area_extent={float(np.nanmin(lons)), float(np.nanmin(lats)), float(np.nanmax(lons)), float(np.nanmax(lats))}")
                    print(f"[DEBUG] lats min/max: {np.nanmin(lats)}, {np.nanmax(lats)}; lons min/max: {np.nanmin(lons)}, {np.nanmax(lons)}")
                    print(f"[DEBUG] lats shape: {lats.shape}, lons shape: {lons.shape}")
                    area_id = getattr(candidate, 'name', 'curvilinear')
                    proj_dict = {'proj': 'latlong'}
                    shape = lons.shape
                    area_extent = [float(np.nanmin(lons)), float(np.nanmin(lats)), float(np.nanmax(lons)), float(np.nanmax(lats))]
                    areadef = AreaDefinition.from_extent(area_id, proj_dict, shape, area_extent)
                    return areadef
        # If still not found, print debug info and raise
        print("[DEBUG] to_griddef: obj type:", type(obj))
        if hasattr(obj, 'coords'):
            print("[DEBUG] obj.coords:", list(obj.coords.keys()))
        if hasattr(obj, 'data_vars'):
            print("[DEBUG] obj.data_vars:", list(obj.data_vars.keys()))
        if hasattr(obj, 'variables'):
            print("[DEBUG] obj.variables:", list(obj.variables.keys()))
        available = []
        if hasattr(obj, 'coords'):
            available.extend(list(obj.coords.keys()))
        if hasattr(obj, 'data_vars'):
            available.extend(list(obj.data_vars.keys()))
        if hasattr(obj, 'variables'):
            available.extend(list(obj.variables.keys()))
        raise ValueError(f"Could not auto-create AreaDefinition or SwathDefinition: latitude/longitude not found. Available keys: {available}")

    import xarray as xr
    # If input is Dask-backed, use xESMF for robust Dask support
    is_dask = False
    try:
        import dask.array as da
        if isinstance(source_data, xr.DataArray) and hasattr(source_data.data, 'chunks'):
            is_dask = isinstance(source_data.data, da.Array)
        elif isinstance(source_data, xr.Dataset):
            # Check if any variable is Dask-backed
            is_dask = any(isinstance(v.data, da.Array) for v in source_data.data_vars.values())
    except ImportError:
        is_dask = False

    if is_dask and has_xesmf:
        # Use xESMF for Dask-backed arrays, including curvilinear grids
        print("[DEBUG] Using xESMF for Dask-backed remapping.")
        import xarray as xr
        def ensure_latlon_names(obj):
            # Only allow xarray objects
            import xarray as xr
            if not isinstance(obj, (xr.DataArray, xr.Dataset)):
                raise TypeError("For Dask/xESMF regridding, both source and target must be xarray objects with 'lat' and 'lon' coordinates. Got: {}".format(type(obj)))
            coords = list(obj.coords.keys())
            dims = list(obj.dims)
            rename_coords = {}
            rename_dims = {}
            if 'latitude' in coords:
                rename_coords['latitude'] = 'lat'
            if 'longitude' in coords:
                rename_coords['longitude'] = 'lon'
            if 'latitude' in dims:
                rename_dims['latitude'] = 'lat'
            if 'longitude' in dims:
                rename_dims['longitude'] = 'lon'
            out = obj
            if rename_coords:
                out = out.rename(rename_coords)
            if rename_dims:
                out = out.rename_dims(rename_dims)
            return out
        src = ensure_latlon_names(source_data)
        tgt = ensure_latlon_names(target_grid)
        # Check for required lat/lon coords (1D or 2D)
        for arr in (src, tgt):
            if not (('lat' in arr.coords and 'lon' in arr.coords)):
                raise ValueError("Both source and target must have 'lat' and 'lon' coordinates for xESMF regridding.")
            lat = arr.coords['lat']
            lon = arr.coords['lon']
            if not (lat.ndim in (1,2) and lon.ndim in (1,2)):
                raise ValueError("'lat' and 'lon' coordinates must be 1D or 2D for xESMF.")
        return resample_xesmf(src, tgt, method="nearest_s2d", **kwargs)

    # Otherwise, use pyresample logic
    source_grid = to_griddef(source_data)
    target_grid = to_griddef(target_grid)
    print(f"[DEBUG] source_grid type: {type(source_grid)}")
    if hasattr(source_grid, 'area_extent'):
        print(f"[DEBUG] source_grid area_extent: {getattr(source_grid, 'area_extent', None)}")
    print(f"[DEBUG] target_grid type: {type(target_grid)}")
    if hasattr(target_grid, 'area_extent'):
        print(f"[DEBUG] target_grid area_extent: {getattr(target_grid, 'area_extent', None)}")
    if source_grid is None or target_grid is None:
        raise ValueError("source_grid and target_grid must be valid AreaDefinition or SwathDefinition objects, not None, or convertible from xarray object with latitude/longitude.")

    resampler = KDTreeNearestXarrayResampler(
        source_grid, target_grid, **kwargs
    )

    if isinstance(source_data, xr.DataArray):
        result = resampler.resample(source_data, radius_of_influence=radius_of_influence)
        result = _reformat_resampled_data(source_data, result, target_grid)
    elif isinstance(source_data, xr.Dataset):
        result = resampler.resample(source_data, radius_of_influence=radius_of_influence)
        target_lon, target_lat = target_grid.get_lonlats_dask()
        if hasattr(result, 'coords') and "latitude" not in result.coords:
            result = result.assign_coords(
                {"latitude": (("y", "x"), target_lat), "longitude": (("y", "x"), target_lon)}
            )
        if hasattr(result, 'attrs'):
            result.attrs.update(source_data.attrs)
    else:
        raise TypeError("source_data must be an xarray.DataArray or xarray.Dataset")

    if not isinstance(result, (xr.DataArray, xr.Dataset)):
        try:
            result = xr.DataArray(result)
        except Exception:
            raise TypeError("Remap result is not an xarray object and could not be wrapped as one.")
    return result


