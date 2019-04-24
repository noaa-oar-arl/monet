""" this will read the modis data"""
import xarray as xr
from ..grids import _geos_16_grid


def _get_swath_from_fname(fname):
    vert_grid_num = fname.split(".")[-4].split("v")[-1]
    hori_grid_num = fname.split(".")[-4].split("v")[0].split("h")[-1]
    return hori_grid_num, vert_grid_num


def _get_time_from_fname(fname):
    import pandas as pd

    u = pd.Series([fname.split(".")[-2]])
    date = pd.to_datetime(u, format="%Y%j%H%M%S")[0]
    return date


def _open_single_file(fname):
    # open the file
    dset = xr.open_dataset(fname)
    dset = dset.rename({"t": "time"})
    # get the area def
    area = _geos_16_grid(dset)
    dset.attrs["area"] = area
    # get proj4 string
    dset.attrs["proj4_srs"] = area.proj_str
    # get longitude and latitudes
    lon, lat = area.get_lonlats_dask()
    dset.coords["longitude"] = (("y", "x"), lon)
    dset.coords["latitude"] = (("y", "x"), lat)

    for i in dset.variables:
        dset[i].attrs["proj4_srs"] = area.proj_str
        dset[i].attrs["area"] = area

    # expand dimensions for time
    dset = dset.expand_dims("time")
    return dset


def open_files(fname):

    if isinstance(fname, str):
        # single file
        dset = _open_single_file(fname)
    else:
        # loop over dsets and combine
        dsets = []
        for i in fname:
            dsets.append(_open_single_file(i))

        dset = xr.auto_combine(dsets, concat_dim="time")

    return dset
