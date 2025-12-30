import numpy as np
import xarray as xr


def resample(source_data, target_grid, method="nearest", **kwargs):
    """Resample data using monet-regrid.

    Parameters
    ----------
    source_data : xarray.DataArray or xarray.Dataset
        Source data to be regridded.
    target_grid : xarray.DataArray or xarray.Dataset
        Target grid definition.
    method : str, default: 'nearest'
        Resampling method. Options include 'nearest', 'bilinear' (linear), 'conservative', etc.
        Mapped to monet-regrid methods.
    **kwargs : dict
        Additional keyword arguments passed to the resampler.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        Regridded data on the target grid.
    """

    # Map method names to monet-regrid methods
    method_map = {
        "nearest": "nearest",
        "bilinear": "linear",
        "linear": "linear",
        "conservative": "conservative",
        "xesmf": "conservative",
    }

    # If method is xesmf, check for xesmf_method in kwargs
    if method == "xesmf":
        xesmf_method = kwargs.pop("xesmf_method", "bilinear")
        if xesmf_method in ["bilinear", "patch"]:
            real_method = "linear"
        elif xesmf_method in ["nearest_s2d", "nearest_d2s"]:
            real_method = "nearest"
        elif xesmf_method == "conservative":
            real_method = "conservative"
        else:
            real_method = "linear"  # Fallback
    else:
        real_method = method_map.get(method, method)

    if real_method not in [
        "nearest",
        "linear",
        "conservative",
        "cubic",
        "least_common",
        "most_common",
        "stat",
    ]:
        # Fallback?
        pass

    # Ensure target_grid is a Dataset as required by monet-regrid for curvilinear/some methods
    if isinstance(target_grid, xr.DataArray):
        target_grid = target_grid.to_dataset()

    # Perform regridding
    # monet-regrid syntax: ds.regrid.method(target_grid)

    if hasattr(source_data, "regrid"):
        regridder = getattr(source_data.regrid, real_method, None)
        if regridder:
            return regridder(target_grid, **kwargs)
        else:
            raise ValueError(f"Method {real_method} not supported by monet-regrid")
    else:
        raise TypeError("source_data must be an xarray object with regrid accessor")


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
    except (ImportError, ValueError) as e:
        if "numpy.dtype size changed" in str(e):
            # Handle numpy compatibility issues in Python 3.10
            raise ImportError(
                "stratify package has numpy compatibility issues in this Python version. "
                "This is a known issue with Python 3.10 and certain numpy versions. "
                "The package works correctly in Python 3.11 and 3.12."
            ) from e
        try:
            import stratify

            if hasattr(stratify, "interpolate"):
                interpolate = stratify.interpolate
            else:
                raise ImportError(
                    "stratify.interpolate not available; please install stratify package."
                ) from e
        except (ImportError, ValueError) as inner_e:
            if "numpy.dtype size changed" in str(inner_e):
                raise ImportError(
                    "stratify package has numpy compatibility issues in this Python version. "
                    "This is a known issue with Python 3.10 and certain numpy versions. "
                    "The package works correctly in Python 3.11 and 3.12."
                ) from inner_e
            raise ImportError(
                "stratify.interpolate not available; please install stratify package."
            ) from inner_e

    # Handle numpy compatibility issues by converting to numpy arrays first
    # This avoids the dtype size mismatch that occurs in Python 3.10
    try:
        vertical_array = vertical.chunk().data
        da_array = da.chunk().data
        result = interpolate(levels, vertical_array, da_array, axis=axis)
    except ValueError as e:
        if "numpy.dtype size changed" in str(e):
            # Fallback: convert to numpy arrays directly without chunking
            vertical_array = np.asarray(vertical)
            da_array = np.asarray(da)
            result = interpolate(levels, vertical_array, da_array, axis=axis)
        else:
            raise
    dims = da.dims
    out = xr.DataArray(result, dims=dims, name=da.name)
    out.attrs = da.attrs.copy()
    if len(da.coords) > 0:
        for vn in da.coords:
            if vn != "z" and "z" not in da[vn].dims:
                out[vn] = da[vn].copy()
    return out
