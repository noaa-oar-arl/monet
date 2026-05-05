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


def resample(
    source_data: xr.DataArray | xr.Dataset,
    target_grid: xr.DataArray | xr.Dataset,
    method: str = "nearest",
    **kwargs: t.Any,
) -> xr.DataArray | xr.Dataset:
    """Resample data using xregrid.

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

    if not has_xregrid:
        raise ImportError(
            "xregrid (with esmpy/ESMF) is required for regridding. "
            "Install it with: pip install xregrid\n"
            "  or: conda install -c conda-forge esmpy xregrid"
        )

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


def resample_stratify(
    da: xr.DataArray,
    levels,
    vertical: xr.DataArray | str,
    axis: int = 1,
    tension: float = 0.0,
) -> xr.DataArray:
    """Deprecated: use ``pytspack.interpolate_vertical`` directly.

    Parameters
    ----------
    da : xarray.DataArray
        The data to interpolate.
    levels : array-like
        Target vertical level values.
    vertical : xarray.DataArray or str
        Vertical coordinate DataArray or coordinate name.
    axis : int, default 1
        Axis index used only to infer ``level_dim`` when ``vertical`` is a DataArray
        without a name.
    tension : float, default 0.0
        Tension factor for the spline interpolation.

    Returns
    -------
    xarray.DataArray
        Data interpolated to the new vertical levels.
    """
    import warnings

    from pytspack import interpolate_vertical

    warnings.warn(
        "resample_stratify() is deprecated. Use pytspack.interpolate_vertical() directly.",
        DeprecationWarning,
        stacklevel=2,
    )

    if isinstance(vertical, str):
        level_dim = vertical
    elif isinstance(vertical, xr.DataArray) and vertical.name:
        level_dim = vertical.name
    else:
        level_dim = da.dims[axis]
        if not isinstance(vertical, str) and isinstance(vertical, xr.DataArray):
            da = da.assign_coords({level_dim: vertical})

    if isinstance(levels, xr.DataArray):
        levels = levels.values

    # pytspack requires a single chunk along the core (vertical) dimension
    if hasattr(da.data, "chunks") and level_dim in da.dims:
        da = da.chunk({level_dim: -1})

    return interpolate_vertical(da, np.asarray(levels), level_dim=level_dim, tension=tension)
