"""
MONET utilities.

This module provides various utilities for working with
geospatial data, interpolation, and statistics.
"""

from . import coards_tools, combinetool, interp_util, resample, tools, vertical
from .tools import (
    _force_forder,
    add_mask,
    calc_3hr_ave,
    calc_8hr_rolling_max,
    calc_13_category_usda_soil_type,
    calc_24hr_ave,
    calc_annual_ave,
    findclosest,
    get_epa_region_bounds,
    get_epa_region_df,
    get_giorgi_region_bounds,
    get_giorgi_region_df,
    get_relhum,
    kolmogorov_zurbenko_filter,
    linregress,
    long_to_wide,
    nearest,
    search_listinlist,
    wsdir2uv,
)

# Import monet_stats as stats for compatibility
try:
    import monet_stats as stats
except ImportError:
    stats = None

__all__ = [
    "combinetool",
    "coards_tools",
    "interp_util",
    "resample",
    "stats",
    "tools",
    "vertical",
    "nearest",
    "search_listinlist",
    "linregress",
    "findclosest",
    "_force_forder",
    "kolmogorov_zurbenko_filter",
    "wsdir2uv",
    "get_relhum",
    "long_to_wide",
    "calc_8hr_rolling_max",
    "calc_24hr_ave",
    "calc_3hr_ave",
    "calc_annual_ave",
    "get_giorgi_region_bounds",
    "get_giorgi_region_df",
    "get_epa_region_bounds",
    "get_epa_region_df",
    "add_mask",
    "calc_13_category_usda_soil_type",
]
