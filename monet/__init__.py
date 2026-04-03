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

from . import met_funcs, monet_accessor, plots, util
from .plots import savefig

__version__ = "2.3.1"

# Core functionality
__all__ = [
    "__version__",
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
