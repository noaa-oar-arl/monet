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
from .accessors import *  # Ensure accessors are registered on import
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
dataset_to_monet = base.BaseAccessor._dataset_to_monet
rename_to_monet_latlon = base.BaseAccessor._rename_to_monet_latlon
rename_latlon = base.BaseAccessor._rename_latlon
    "plots",  # Plotting utilities and visualization tools
    "sat",  # Satellite data tools
    "util",  # General utility functions
    "monet_accessor",  # xarray/pandas accessors
    "met_funcs",  # Meteorological functions
    "savefig",  # Save figure utility
    "dataset_to_monet",  # Convert datasets to MONET format
    "rename_to_monet_latlon",  # Standardize lat/lon naming
    "rename_latlon",  # Basic lat/lon renaming
]

dataset_to_monet = monet_accessor._dataset_to_monet
rename_to_monet_latlon = monet_accessor._rename_to_monet_latlon
rename_latlon = monet_accessor._rename_latlon
