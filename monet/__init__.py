"""
MONET: Model and Observation Evaluation Toolkit

A Python toolkit for comparing air quality models with observations.
Provides utilities for pairing model output with observational data,
statistical analysis, and visualization.

Main Components
-------------
plots : module
    Visualization tools for model-observation comparisons
met_funcs : module
    Meteorological calculation utilities
monet_accessor : module
    xarray and pandas accessor methods for MONET functionality
util : module
    General utility functions and statistical tools
"""

from . import met_funcs, plots, util

try:
    # Import accessors to ensure they get registered
    from .accessors import base, dataarray_accessor, dataset_accessor, pandas_accessor  # noqa: F401

    _accessors_available = True
except ImportError:
    # Skip accessor registration if dependencies are missing during docs build
    _accessors_available = False
    base = None
from .plots import savefig
from .util.coards_tools import (
    add_cf_attributes,
    add_cf_standard_names,
    convert_coards_to_monet_format,
    is_coards_compliant,
    monet_to_coards,
)

__version__ = "2.3.1"

# Core functionality
__all__ = [
    "__version__",
    "plots",
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
try:
    dataset_to_monet = base.BaseAccessor._dataset_to_monet
    rename_to_monet_latlon = base.BaseAccessor._rename_to_monet_latlon
    rename_latlon = base.BaseAccessor._rename_latlon
except AttributeError:
    # Fallback if accessors aren't properly initialized (e.g., during docs build)
    def dataset_to_monet(*args, **kwargs):
        """Placeholder function for docs build."""
        pass

    def rename_to_monet_latlon(*args, **kwargs):
        """Placeholder function for docs build."""
        pass

    def rename_latlon(*args, **kwargs):
        """Placeholder function for docs build."""
        pass
