"""MONET Accessor (Backward Compatibility Layer)"""

from .accessors.base import BaseAccessor, has_xregrid, wrap_longitudes  # noqa: F401
from .accessors.dataarray_accessor import MONETAccessor  # noqa: F401
from .accessors.dataset_accessor import MONETAccessorDataset  # noqa: F401
from .accessors.pandas_accessor import MONETAccessorPandas  # noqa: F401

has_monet_regrid = has_xregrid

has_pyresample = False
has_xesmf = False

# Import old functions for backward compatibility
_rename_latlon = BaseAccessor._rename_latlon
_monet_to_latlon = BaseAccessor._monet_to_latlon
_dataset_to_monet = BaseAccessor._dataset_to_monet
_rename_to_monet_latlon = BaseAccessor._rename_to_monet_latlon
_coards_to_netcdf = BaseAccessor._coards_to_netcdf
_dataarray_coards_to_netcdf = BaseAccessor._dataarray_coards_to_netcdf
