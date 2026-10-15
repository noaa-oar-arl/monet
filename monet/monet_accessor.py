"""MONET Accessor (Backward Compatibility Layer)"""

from .accessors.base import (  # noqa: F401
    BaseAccessor,
    has_axis,
    wrap_longitudes,
)
from .accessors.dataarray_accessor import MONETAccessor  # noqa: F401
from .accessors.dataset_accessor import MONETAccessorDataset  # noqa: F401
from .accessors.pandas_accessor import MONETAccessorPandas  # noqa: F401

# Import old functions for backward compatibility
_rename_latlon = BaseAccessor._rename_latlon
_monet_to_latlon = BaseAccessor._monet_to_latlon
_dataset_to_monet = BaseAccessor._dataset_to_monet
_rename_to_monet_latlon = BaseAccessor._rename_to_monet_latlon
_coards_to_netcdf = BaseAccessor._coards_to_netcdf
_dataarray_coards_to_netcdf = BaseAccessor._dataarray_coards_to_netcdf


def __getattr__(name):
    if name == "has_xregrid":
        import warnings

        warnings.warn("has_xregrid is deprecated; use has_axis instead.", DeprecationWarning, stacklevel=2)
        return has_axis
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
