# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1

import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar
from numpy import array

ProgressBar().register()


class camx:
    def __init__(self):
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
        self.conc = None  # concentration object
        self.metcro2d = None  # metcro2d obj
        self.aerodiam = None  # aerodiam obj
        self.grid = None  # gridcro2d obj
        self.fname = None
        self.metcrofnames = None
        self.aerofnames = None
        self.dates = None
        self.keys = None
        self.indexdates = None
        self.metindex = None
        self.latitude = None
        self.longitude = None
        self.map = None

    def get_conc_dates(self):
        print 'Reading CAMx dates...'
        tflag1 = array(self.conc['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.conc['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i, j in zip(tflag1, tflag2)]
        self.conc['TSTEP'] = pd.to_datetime(date, format='%Y%j%H')
        self.indexdates = pd.Series(date).drop_duplicates(keep='last').index.values
        self.dates = self.conc.TSTEP[self.indexdates].to_index()
        self.conc = self.conc.isel(TSTEP=self.indexdates)

    def get_met2d_dates(self):
        print 'Reading CMAQ METCRO2D dates...'
        tflag1 = array(self.metcro2d['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.metcro2d['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i, j in zip(tflag1, tflag2)]
        self.metcro2d['TSTEP'] = pd.to_datetime(date, format='%Y%j%H')
        self.met2dindex = pd.Series(date).drop_duplicates(keep='last').index.values
        self.met2ddates = self.conc.TSTEP[self.metindex].to_index()

    def get_metsfc_dates(self):
        print 'Reading CMAQ METCRO2D dates...'
        tflag1 = array(self.aerodiam['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.aerodiam['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i, j in zip(tflag1, tflag2)]
        if 'AGRID' in self.conc.FILEDESC:
            self.aerodiam['TSTEP'] = pd.to_datetime(date, format='%Y%j%H') + pd.TimeDelta
        self.metsfc['TSTEP'] = pd.to_datetime(date, format='%Y%j%H')

    def open_conc(self, file):
        from glob import glob
        from numpy import sort
        if type(file) == str:
            self.fname = sort(array(glob(file)))
        else:
            self.fname = sort(array(file))
        if self.fname.shape[0] >= 1:
            #            print self.fname
            self.conc = xr.open_mfdataset(self.fname.tolist(), concat_dim='TSTEP')
        else:
            print 'Files not found'
        self.get_conc_dates()
        self.keys = self.conc.keys()

    def open_metcro2d(self, f):
        from glob import glob
        from numpy import sort
        try:
            if type(f) == str:
                self.metcrofnames = sort(array(glob(f)))
                print self.metcrofnames
            else:
                self.metcrofnames = sort(array(f))
                print self.metcrofnames
            if self.metcrofnames.shape[0] >= 1:
                self.met2d = xr.open_mfdataset(self.metcrofnames.tolist(), concat_dim='TSTEP')
            self.metkeys = self.met2d.keys()
            self.get_met2d_dates()
        except:
            print 'METCRO2D Files Not Found'
            pass

    def get_nox(self, lay=None):
        if lay is not None:
            print '   Getting CMAQ NOx:  NO'
            var = self.conc['NO'][:, 0, :, :].squeeze()
            print '   Getting CMAQ NOx:  NO2'
            var += self.conc['NO2'][:, 0, :, :].squeeze()
        else:
            print '   Getting CMAQ NOx:  NO'
            var = self.conc['NO'][:, :, :, :].squeeze()
            print '   Getting CMAQ NOx:  NO2'
            var += self.conc['NO2'][:, :, :, :].squeeze()
        return var

    def get_pm25(self,lay=None):
        keys = self.conc.keys()
        allvars = self.fine
        index = pd.Series(allvars).isin(keys)
        newkeys = allvars[index]
        if lay is not None:
            var = self.conc[newkeys[0]][:, 0, :, :].squeeze()
        else:
            var = self.conc[newkeys[0]][:, :, :, :].squeeze()
        for i in newkeys[1:]:
            if lay is not None:
                var += self.conc[i][:, 0, :, :].squeeze()
            else:
                var += self.conc[i][:, :, :, :].squeeze()
        return var

    def get_pm10(self,lay=None):
        keys = self.conc.keys()
        allvars = self.coarse
        index = pd.Series(allvars).isin(keys)
        newkeys = allvars[index]
        if lay is not None:
            var = self.conc[newkeys[0]][:, 0, :, :].squeeze()
        else:
            var = self.conc[newkeys[0]][:, :, :, :].squeeze()
        for i in newkeys[1:]:
            if lay is not None:
                var += self.conc[i][:, 0, :, :].squeeze()
            else:
                var += self.conc[i][:, :, :, :].squeeze()
        return var


def get_var(self, param='O3',lay=None):
    p = param.upper()
    print param
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
            var = self.conc['VOC'][:, 0, :, :].squeeze()
        else:
            var = self.conc['VOC'][:, :, :, :].squeeze()
    else:
        print '   Getting CMAQ Variable: ' + param
        var = self.conc[param][:, 0, :, :].squeeze()
    return var



def get_met2d_var(self, param='TEMPG', lay=None):
    param = param.upper()
    print  ' Getting CMAQ Variable: ' + param
    if lay is not None:
        var = self.metcro2d[param][self.metindex, 0, :, :].squeeze()
    else:
        var = self.metcro2d[param][self.metindex, :, :, :].squeeze()
    return var


def set_gridcro2d(self, filename):
    self.grid = xr.open_dataset(filename)
    lat = 'LAT'
    lon = 'LON'
    self.latitude = self.grid[lat][:][0, 0, :, :].squeeze().compute()
    self.longitude = self.grid[lon][:][0, 0, :, :].squeeze().compute()
    self.load_conus_basemap(res='l')


def load_conus_basemap(self, res='l'):
    from mpl_toolkits.basemap import Basemap
    if self.map is None:
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
