"""
MONET Accessor module for pandas and xarray objects.

This module provides accessors for pandas DataFrames, xarray DataArrays,
and xarray Datasets, adding MONET-specific functionality.
"""

from .dataarray_accessor import MONETAccessor
from .dataset_accessor import MONETAccessorDataset
from .pandas_accessor import MONETAccessorPandas

__all__ = ["MONETAccessorPandas", "MONETAccessor", "MONETAccessorDataset"]
