from __future__ import division, print_function

from builtins import object, zip

import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar
from numpy import array
from past.utils import old_div

from monet.models.basemodel import BaseModel

# This file is to deal with CAMx code - try to make it general for CAMx 4.7.1 --> 5.1


ProgressBar().register()


class CAMx(BaseModel):
    def __init__(self):
        BaseModel.__init__(self)
        self.objtype = 'CAMX'
        self.coarse = array(
            ['NA', 'PSO4', 'PNO3', 'PNH4', 'PH2O', 'PCL', 'PEC', 'FPRM', 'FCRS', 'CPRM', 'CCRS', 'SOA1', 'SOA2', 'SOA3',
             'SOA4'])
        self.fine = array(
            ['NA', 'PSO4', 'PNO3', 'PNH4', 'PH2O', 'PCL', 'PEC', 'FPRM', 'FCRS', 'SOA1', 'SOA2', 'SOA3',
             'SOA4'])
        self.noy_gas = array(
            ['NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA', 'NTR', 'CRON', 'CRN2', 'CRNO',
             'CRPX', 'OPAN'])
        self.poc = array(['SOA1', 'SOA2', 'SOA3', 'SOA4'])
        self.map = None

    def get_dates(self):
        print('Reading CAMx dates...')
        print(self.dset)
        tflag1 = array(self.dset['TFLAG'][:, 0], dtype='|S7')
        tflag2 = array(old_div(self.dset['TFLAG'][:, 1], 10000), dtype='|S6')
        date = pd.to_datetime([i + j.zfill(2) for i, j in zip(tflag1, tflag2)], format='%Y%j%H')
        indexdates = pd.Series(date).drop_duplicates(keep='last').index.values
        self.dset = self.dset.isel(time=indexdates)
        self.dset['time'] = date[indexdates]

    def add_files(self, file):
        from glob import glob
        from numpy import sort
        dropset = ['layer', 'longitude_bounds', 'latitude_bounds',
                   'x', 'y', 'level', 'lambert_conformal_conic']
        nameset = {'COL': 'x', 'ROW': 'y', 'TSTEP': 'time', 'LAY': 'z'}
        if type(file) == str:
            fname = sort(array(glob(file)))
        else:
            fname = sort(array(file))
        if fname.shape[0] >= 1:
            if self.dset is None:
                self.dset = xr.open_mfdataset(
                    fname.tolist(), concat_dim='TSTEP', engine='pnc').drop(dropset).rename(nameset).squeeze()
                self.load_conus_basemap(res='l')
                self.get_dates()
            else:
                dset = xr.open_mfdataset(fname.tolist(), concat_dim='TSTEP',
                                         engine='pnc').drop(dropset).rename(nameset).squeeze()
                self.dset = xr.merge([self.dset, dset])
        else:
            print('Files not found')
        self.keys = list(self.dset.keys())

    def check_z(self, varname):
        if pd.Series(self.dset[varname].dims).isin('z').max():
            return True
        else:
            return False

    def add_multiple_fields(self, findkeys, lay=None, weights=None):
        from numpy import ones
        keys = self.keys
        newkeys = pd.Series(findkeys).loc[pd.Series(findkeys).isin(keys)].values
        if weights is None:
            w = ones(len(newkeys))
        var = self.dset[newkeys[0]] * w[0]
        for i, j in zip(newkeys[1:], w[1:]):
            var = var + self.dset[i] * j
        return select_layer(var, lay=lay)

    @staticmethod
    def select_layer(variable, lay=None):
        if lay is not None:
            try:
                var = variable.sel(z=lay)
            except ValueError:
                print('Dimension \'z\' not in Dataset.  Returning Dataset anyway')
                var = variable
        else:
            var = variable
        return var

    def get_nox(self, lay=None):
        var = add_multiple_fields(['NO', 'NO2'], lay=lay)
        return var

    def get_pm25(self, lay=None):
        keys = list(self.dset.keys())
        allvars = self.fine
        index = pd.Series(allvars).isin(keys)
        newkeys = allvars[index]
        var = add_multiple_fields(newkeys, lay=lay)
        return var

    def get_pm10(self, lay=None):
        keys = list(self.dset.keys())
        allvars = self.coarse
        index = pd.Series(allvars).isin(keys)
        newkeys = allvars[index]
        var = add_multiple_fields(newkeys, lay=lay)
        return var

    def get_var(self, param='O3', lay=None):
        p = param.upper()
        print(param)
        if p == 'PM25':
            var = self.get_pm25(lay=lay)
        elif p == 'PM10':
            var = self.get_pm10(lay=lay)
        elif p == 'NOX':
            var = self.get_nox(lay=lay)
        elif p == 'OC':
            var = self.get_oc(lay=lay)
        elif p == 'VOC':
            if lay is not None:
                var = selfself.dset['VOC']
            else:
                var = self.dset['VOC']
        else:
            var = self.select_layer(self.dset[param], lay=lay)
        return var

    def load_conus_basemap(self, res='l'):
        from mpl_toolkits.basemap import Basemap
        if self.map is None:
            lat1 = self.dset.P_ALP
            lat2 = self.dset.P_BET
            lon1 = self.dset.P_GAM
            lon0 = self.dset.XCENT
            lat0 = self.dset.YCENT
            m = Basemap(projection='lcc', resolution=res, lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0,
                        lon_1=lon1,
                        llcrnrlat=self.dset.latitude[0, 0], urcrnrlat=self.dset.latitude[-1, -1],
                        llcrnrlon=self.dset.longitude[0, 0],
                        urcrnrlon=self.dset.longitude[-1, -1], rsphere=6371200.,
                        area_thresh=50.)
            self.map = m
        else:
            m = self.map
        return self.map
