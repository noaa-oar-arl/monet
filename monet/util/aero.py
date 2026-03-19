"""
Aero Protocol utilities for MONET.
"""

from collections.abc import Callable
from typing import Any

import xarray as xr

from .conventions import update_history


def _apply_aero(
    func: Callable,
    *args: Any,
    name: str = "",
    output_dtypes: list[Any] | None = None,
    output_core_dims: list[list[str]] | None = None,
    input_core_dims: list[list[str]] | None = None,
    source: str = "monet",
    **kwargs: Any,
) -> Any:
    """Helper to apply a function following Aero Protocol.

    This function facilitates backend-agnostic computations (NumPy/Dask)
    using xarray.apply_ufunc and ensures data provenance via history tracking.

    Parameters
    ----------
    func : Callable
        The core logic function to apply.
    *args : Any
        Arguments passed to the function.
    name : str, optional
        A descriptive name of the computation for history tracking.
    output_dtypes : list, optional
        Data types of the outputs. Defaults to [float].
    output_core_dims : list of lists, optional
        Core dimensions of the outputs.
    input_core_dims : list of lists, optional
        Core dimensions of the inputs.
    source : str, optional
        The module or package name for provenance tracking. Defaults to "monet".
    **kwargs : Any
        Keyword arguments passed to the function.

    Returns
    -------
    Any
        The result of the computation, either as NumPy/scalar or xarray object.
    """
    is_xr = any(isinstance(arg, xr.DataArray | xr.Dataset) for arg in args)

    if is_xr:
        if output_dtypes is None:
            output_dtypes = [float]

        apply_kwargs = {
            "kwargs": kwargs,
            "dask": "parallelized",
            "output_dtypes": output_dtypes,
        }
        if output_core_dims is not None:
            apply_kwargs["output_core_dims"] = output_core_dims
        if input_core_dims is not None:
            apply_kwargs["input_core_dims"] = input_core_dims

        result = xr.apply_ufunc(func, *args, **apply_kwargs)

        # Update history
        results = result if isinstance(result, tuple) else (result,)
        for res in results:
            if hasattr(res, "attrs"):
                update_history(res, f"Computed {name} via {source}")

        return result

    return func(*args, **kwargs)
