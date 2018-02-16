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

##TO DO - Need to decide what belongs in the BaseModel. This is just a first guess.

class BaseModel(object):
    def __init__(self):
        self.dset = None  # CMAQ xarray dataset object
        self.grid = None  # CMAQ xarray dataset gridcro2d obj
        self.dates = None
        self.keys = None
        self.metcrokeys = []
        self.indexdates = None
        self.metdates = None
        self.metindex = None
        self.latitude = None
        self.longitude = None
        self.map = None

    def get_dates(self):
        idims = len(self.dset.TFLAG.dims)
        if idims == 2:
            tflag1 = array(self.dset['TFLAG'][:, 0], dtype='|S7')
            tflag2 = array(old_div(self.dset['TFLAG'][:, 1], 10000), dtype='|S6')
        else:
            tflag1 = array(self.dset['TFLAG'][:, 0, 0], dtype='|S7')
            tflag2 = array(old_div(self.dset['TFLAG'][:, 0, 1], 10000), dtype='|S6')
        date = pd.to_datetime([i + j.zfill(2) for i, j in zip(tflag1, tflag2)], format='%Y%j%H')
        indexdates = pd.Series(date).drop_duplicates(keep='last').index.values
        self.dset = self.dset.isel(time=indexdates)
        self.dset['time'] = date[indexdates]

    def open_files(self, flist=None):
        """Adds information from files to the dset xarray"""
        for fname in flist:
            self.add_files(fname)

    def add_files(self, mfile):
        """adds a file with concentration information to the dset array"""
        from glob import glob
        from numpy import sort
        nameset = {'COL': 'x', 'ROW': 'y', 'TSTEP': 'time', 'LAY': 'z'}
        if type(mfile) == str:
            fname = sort(array(glob(mfile)))
        else:
            fname = sort(array(mfile))
        if fname.shape[0] >= 1:
            if self.dset is None:
                self.dset = xr.open_mfdataset(fname.tolist(), concat_dim='TSTEP').rename(nameset).squeeze()
            else:
                dset = xr.open_mfdataset(fname.tolist(), concat_dim='TSTEP').rename(nameset).squeeze()
                self.dset = xr.merge([self.dset, dset])
        else:
            print('Files not found')
        if self.grid is not None:
            self.dset = self.dset.assign(latitude=self.grid.LAT.squeeze())
            self.dset = self.dset.assign(longitude=self.grid.LON.squeeze())
            self.dset = self.dset.set_coords(['latitude', 'longitude'])
        self.get_dates()
        self.keys = list(self.dset.keys())

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


    def set_gridcro2d(self, filename):
        self.grid = xr.open_dataset(filename).rename({'COL': 'x', 'ROW': 'y'}).drop('TFLAG').squeeze()
        lat = 'LAT'
        lon = 'LON'
        self.latitude = self.grid[lat][:][:, :].squeeze().compute()
        self.longitude = self.grid[lon][:][:, :].squeeze().compute()
        self.load_conus_basemap(res='l')

    def load_conus_basemap(self, res='l'):
        from mpl_toolkits.basemap import Basemap
        if isinstance(self.map, type(None)):
            lat1 = self.grid.P_ALP
            lat2 = self.grid.P_BET
            lon1 = self.grid.P_GAM
            lon0 = self.grid.XCENT
            lat0 = self.grid.YCENT
            m = Basemap(projection='lcc', resolution=res, lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0,
                        lon_1=lon1,
                        llcrnrlat=self.latitude[0, 0], urcrnrlat=self.latitude[-1, -1],
                        llcrnrlon=self.longitude[0, 0],
                        urcrnrlon=self.longitude[-1, -1], rsphere=6371200.,
                        area_thresh=50.)
            self.map = m
        else:
            m = self.map
        return self.map
