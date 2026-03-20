import typing as t

import numpy as np
import xarray as xr

# Check for xregrid and esmpy at module level for better mockability and performance
try:
    import esmpy  # noqa: F401
    from xregrid import Regridder

    has_xregrid = True
except ImportError:
    try:
        import ESMF as esmpy  # noqa: F401
        from xregrid import Regridder

        has_xregrid = True
    except ImportError:
        has_xregrid = False

try:
    import monet_regrid  # noqa: F401

    has_monet_regrid = True
except ImportError:
    has_monet_regrid = False


def resample(
    source_data: xr.DataArray | xr.Dataset,
    target_grid: xr.DataArray | xr.Dataset,
    method: str = "nearest",
    **kwargs: t.Any,
) -> xr.DataArray | xr.Dataset:
    """Resample data using xregrid (default) or monet-regrid (fallback).

    Parameters
    ----------
    source_data : xarray.DataArray or xarray.Dataset
        Source data to be regridded (Backend-agnostic: supports NumPy or Dask).
    target_grid : xarray.DataArray or xarray.Dataset
        Target grid definition.
    method : str, default: 'nearest'
        Resampling method. Options include 'bilinear', 'nearest', 'conservative', etc.
    **kwargs : dict
        Additional keyword arguments passed to the regridder.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        Regridded data on the target grid.

    Examples
    --------
    >>> out = resample(source, target, method='bilinear')
    """

    # Handle backward compatibility for xesmf_method
    if method == "xesmf":
        method = kwargs.pop("xesmf_method", "bilinear")

    # Map method names
    method_map = {
        "linear": "bilinear",
        "bilinear": "bilinear",
        "nearest": "nearest_s2d",
        "conservative": "conservative",
        "nearest_s2d": "nearest_s2d",
        "nearest_d2s": "nearest_d2s",
    }
    real_method = method_map.get(method, method)

    # Ensure target_grid is a Dataset
    if isinstance(target_grid, xr.DataArray):
        target_grid = target_grid.to_dataset()

    if has_xregrid:
        # xregrid.Regridder detection logic works better with Datasets.
        # If source_data is a DataArray, we pass a temporary Dataset for detection and application.
        was_da = isinstance(source_data, xr.DataArray)
        src_for_regrid = source_data
        if was_da:
            da_name = source_data.name or "data"
            src_for_regrid = source_data.to_dataset(name=da_name)

        # Create regridder and apply
        regridder = Regridder(src_for_regrid, target_grid, method=real_method, **kwargs)
        out = regridder(src_for_regrid)

        # Convert back to DataArray if necessary
        if was_da:
            out = out[da_name]

        # Update history for provenance
        from .conventions import update_history

        update_history(out, f"Resampled via monet.util.resample (method={real_method})")

        return out
    else:
        # Fallback to monet-regrid
        try:
            import monet_regrid

            regridder = monet_regrid.Regridder(source_data)
            if real_method in ["bilinear", "linear"]:
                return regridder.linear(target_grid, **kwargs)
            else:
                # nearest_s2d, nearest_d2s, nearest all map to nearest in monet-regrid
                return regridder.nearest(target_grid, **kwargs)
        except ImportError:
            raise ImportError("Neither xregrid (with esmpy) nor monet-regrid is available.")


def resample_stratify(
    da: xr.DataArray,
    levels: t.Sequence[float],
    vertical: xr.DataArray | str | t.Sequence[float],
    axis: int = 1,
    tension: float = 0.0,
) -> xr.DataArray:
    """Vertically interpolate data to specified levels.

    Uses pytspack package to interpolate a DataArray to new vertical levels.
    Supports both Eager (NumPy) and Lazy (Dask) backends.

    Parameters
    ----------
    da : xarray.DataArray
        The data to interpolate. Must have a vertical dimension.
    levels : array-like
        The target vertical levels to interpolate to.
    vertical : array-like or str
        The current vertical coordinate values or coordinate name.
    axis : int, default 1
        The axis representing the vertical dimension.
    tension : float, default 0.0
        Tension factor for the spline interpolation.

    Returns
    -------
    xarray.DataArray
        Data interpolated to the new vertical levels, preserving attributes
        and other coordinates.

    Examples
    --------
    >>> out = resample_stratify(da, [100, 200, 500], 'altitude')
    """
    from pytspack import interpolate_vertical

    orig_dim = da.dims[axis]

    # Handle different types of 'vertical' input
    if isinstance(vertical, xr.DataArray):
        vertical_name = vertical.name or "vertical_coord"
        if vertical_name not in da.coords:
            da = da.assign_coords({vertical_name: vertical})
    elif isinstance(vertical, str):
        vertical_name = vertical
    else:
        # array-like
        vertical_name = "vertical_coord"
        da = da.assign_coords({vertical_name: (orig_dim, np.asarray(vertical))})

    # interpolate_vertical expects the vertical coordinate name and the dimension name
    # to be the same (level_dim). We temporarily rename the dimension to match.
    da_renamed = da.rename({orig_dim: vertical_name})

    # Ensure vertical dimension is not chunked for apply_ufunc in pytspack
    if da_renamed.chunks is not None:
        da_renamed = da_renamed.chunk({vertical_name: -1})

    # Convert levels to raw array to avoid xarray broadcast issues in pytspack when levels is a DataArray
    if isinstance(levels, xr.DataArray):
        levels_arr = levels.data
    else:
        levels_arr = np.asarray(levels)

    out = interpolate_vertical(da_renamed, levels_arr, level_dim=vertical_name, tension=tension)

    # Rename the dimension back to the original name
    out = out.rename({vertical_name: orig_dim})

    # Preserve the original name
    out.name = da.name

    # Update history
    from .conventions import update_history

    update_history(out, "Vertically interpolated via monet.util.resample.resample_stratify")

    return out
