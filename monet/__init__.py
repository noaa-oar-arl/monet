from . import met_funcs, plots, util
from .accessors import base
from .plots import savefig
from .util.coards_tools import (
    convert_coards_to_monet_format,
    is_coards_compliant,
    add_cf_attributes,
    monet_to_coards,
    add_cf_standard_names
)

__version__ = "2.2.12"

__all__ = [
    "__version__",
    "plots",
    "sat",
    "util",
    "accessors",
    "met_funcs",
    "savefig",
    "dataset_to_monet",
    "rename_to_monet_latlon",
    "rename_latlon",
    "convert_coards_to_monet_format",
    "is_coards_compliant",
    "add_cf_attributes",
    "monet_to_coards",
    "add_cf_standard_names",
]

# Use the base accessor methods for the old function names for backward compatibility
dataset_to_monet = base.BaseAccessor._dataset_to_monet
rename_to_monet_latlon = base.BaseAccessor._rename_to_monet_latlon
rename_latlon = base.BaseAccessor._rename_latlon
