"""MONET Accessor (Backward Compatibility Layer)"""

from .accessors.base import BaseAccessor
from .accessors.pandas_accessor import MONETAccessorPandas
from .accessors.dataarray_accessor import MONETAccessor
from .accessors.dataset_accessor import MONETAccessorDataset
from .accessors.base import has_pyresample, has_xesmf, wrap_longitudes

# Import old functions for backward compatibility
_rename_latlon = BaseAccessor._rename_latlon
_monet_to_latlon = BaseAccessor._monet_to_latlon
_dataset_to_monet = BaseAccessor._dataset_to_monet
_rename_to_monet_latlon = BaseAccessor._rename_to_monet_latlon
_coards_to_netcdf = BaseAccessor._coards_to_netcdf
_dataarray_coards_to_netcdf = BaseAccessor._dataarray_coards_to_netcdf
