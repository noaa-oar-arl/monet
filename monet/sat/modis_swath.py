# MODIS Swath data
""" this will read the modis data"""
import xarray as xr

from ..grids import get_modis_latlon_from_swath_hv, get_sinu_area_def


def _get_swath_from_fname(fname):
    vert_grid_num = fname.split(".")[-4].split("v")[-1]
    hori_grid_num = fname.split(".")[-4].split("v")[0].split("h")[-1]
    return hori_grid_num, vert_grid_num


def _get_time_from_fname(fname):
    import pandas as pd

    u = pd.Series([fname.split(".")[-2]])
    date = pd.to_datetime(u, format="%Y%j%H%M%S")[0]
    return date


def open_single_file(fname):
    # first get the h,v from the file name
    h, v = _get_swath_from_fname(fname)
    # get the grid boundary
    timestamp = _get_time_from_fname(fname)
    # open the dataset
    dset = xr.open_dataset(fname)
    # rename  x and y dimensions
    dset = dset.rename({"XDim:MOD_Grid_BRDF": "x", "YDim:MOD_Grid_BRDF": "y"})
    # get lat lon from dset and h, v
    dset = get_modis_latlon_from_swath_hv(h, v, dset)
    # get the area_def
    dset.attrs["area"] = get_sinu_area_def(dset)
    # set the time
    dset["time"] = timestamp

    return dset
