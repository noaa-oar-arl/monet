"""
MONET Accessor module for pandas and xarray objects.

This module provides accessors for pandas DataFrames, xarray DataArrays,
and xarray Datasets, adding MONET-specific functionality.
"""

from .pandas_accessor import MONETAccessorPandas
from .dataarray_accessor import MONETAccessor
from .dataset_accessor import MONETAccessorDataset

__all__ = ["MONETAccessorPandas", "MONETAccessor", "MONETAccessorDataset"]
