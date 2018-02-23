from __future__ import absolute_import, division, print_function

# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1
from builtins import object, zip
from gc import collect

import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar
from numpy import array, zeros
from past.utils import old_div

ProgressBar().register()

# TO DO - Need to decide what belongs in the BaseModel. This is just a first guess.


class BaseModel(object):
    def __init__(self):
        self.dset = None  # CMAQ xarray dataset object
        self.dates = None
        self.keys = None
        self.indexdates = None
        self.latitude = None
        self.longitude = None
        self.map = None

    def open_files(self, flist=None):
        """Adds information from files to the dset xarray"""
        for fname in flist:
            self.add_files(fname)

    def check_z(self, varname):
        if pd.Series(self.dset[varname].dims).isin(['z']).max():
            return True
        else:
            return False

    def add_multiple_fields(self, findkeys, lay=None, weights=None):
        from numpy import ones
        keys = self.keys
        newkeys = pd.Series(findkeys).loc[pd.Series(findkeys).isin(keys)].values
        if weights is None:
            w = ones(len(newkeys))
        if self.check_z(newkeys[0]):
            if lay is not None:
                var = self.dset[newkeys[0]][:, 0, :, :].squeeze() * w[0]
                for i, j in zip(newkeys[1:], w[1:]):
                    var += self.dset[i][:, 0, :, :].squeeze() * j
            else:
                var = self.dset[newkeys[0]][:, :, :, :].squeeze() * w[0]
                for i, j in zip(newkeys[1:], w[1:]):
                    var += self.dset[i][:, :, :, :].squeeze() * j
        else:
            var = self.dset[newkeys[0]][:, :, :].copy() * w[0]
            for i, j in zip(newkeys[1:], w[1:]):
                var += self.dset[i][:, :, :].squeeze() * j
        return var

    def get_var(self, varname):
        return self.dset[varname]
