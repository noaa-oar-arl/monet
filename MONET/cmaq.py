# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1
from gc import collect

import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar
from numpy import array, zeros

ProgressBar().register()
from tools import search_listinlist


class cmaq:
    def __init__(self):
        self.objtype = 'CMAQ'
        self.dust_pm25 = array(
            ['ASO4J', 'ANO3J', 'ACLJ', 'ANH4J', 'ANAJ', 'ACAJ', 'AMGJ', 'AKJ', 'APOCJ', 'APNCOMJ', 'AECJ', 'AFEJ',
             'AALJ', 'ASIJ', 'ATIJ', 'AMNJ', 'AOTHRJ'])
        self.dust_total = array(
            ['ASO4J', 'ASO4K', 'ANO3J', 'ANO3K', 'ACLJ', 'ACLK', 'ANH4J', 'ANAJ', 'ACAJ', 'AMGJ', 'AKJ', 'APOCJ',
             'APNCOMJ', 'AECJ', 'AFEJ', 'AALJ', 'ASIJ', 'ATIJ', 'AMNJ', 'AOTHRJ', 'ASOIL'])
        self.aitken = array(['ACLI', 'AECI', 'ANAI', 'ANH4I',
                             'ANO3I', 'AOTHRI', 'APNCOMI', 'APOCI',
                             'ASO4I', 'AORGAI', 'AORGPAI',
                             'AORGBI'])
        self.accumulation = array(
            ['AALJ', 'AALK1J', 'AALK2J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J', 'ACAJ', 'ACLJ', 'AECJ', 'AFEJ',
             'AISO1J', 'AISO2J', 'AISO3J', 'AKJ', 'AMGJ', 'AMNJ', 'ANAJ', 'ANH4J', 'ANO3J', 'AOLGAJ', 'AOLGBJ',
             'AORGCJ', 'AOTHRJ', 'APAH1J', 'APAH2J', 'APAH3J', 'APNCOMJ', 'APOCJ', 'ASIJ', 'ASO4J', 'ASQTJ', 'ATIJ',
             'ATOL1J', 'ATOL2J', 'ATOL3J', 'ATRP1J', 'ATRP2J', 'AXYL1J', 'AXYL2J', 'AXYL3J', 'AORGAJ',
             'AORGPAJ', 'AORGBJ'])
        self.coarse = array(['ACLK', 'ACORS', 'ANH4K', 'ANO3K', 'ASEACAT', 'ASO4K', 'ASOIL'])
        self.noy_gas = array(
            ['NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA', 'NTR', 'CRON', 'CRN2', 'CRNO',
             'CRPX', 'OPAN'])
        self.pec = array(['AECI', 'AECJ'])
        self.pso4 = array(['ASO4I', 'ASO4J'])
        self.pno3 = array(['ANO3I', 'ANO3J'])
        self.pnh4 = array(['ANH4I', 'ANH4J'])
        self.pcl = array(['ACLI', 'ACLJ'])
        self.poc = array(['AOTHRI', 'APNCOMI', 'APOCI', 'AORGAI', 'AORGPAI', 'AORGBI', 'ATOL1J', 'ATOL2J', 'ATOL3J',
                          'ATRP1J', 'ATRP2J', 'AXYL1J', 'AXYL2J', 'AXYL3J', 'AORGAJ', 'AORGPAJ', 'AORGBJ', 'AOLGAJ',
                          'AOLGBJ', 'AORGCJ', 'AOTHRJ', 'APAH1J', 'APAH2J', 'APAH3J', 'APNCOMJ', 'APOCJ', 'ASQTJ',
                          'AISO1J', 'AISO2J', 'AISO3J', 'AALK1J', 'AALK2J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J', 'AORGAI',
                          'AORGAJ', 'AORGPAI', 'AORGPAJ', 'AORGBI', 'AORGBJ'])
        self.minerals = array(['AALJ', 'ACAJ', 'AFEJ', 'AKJ', 'AMGJ', 'AMNJ', 'ANAJ', 'ATIJ', 'ASIJ'])
        self.conc = None  # concentration object
        self.metcro2d = None  # metcro2d obj
        self.aerodiam = None  # aerodiam obj
        self.grid = None  # gridcro2d obj
        self.fname = None
        self.metcrofnames = None
        self.aerofnames = None
        self.dates = None
        self.keys = None
        self.metcrokeys = []
        self.indexdates = None
        self.metdates = None
        self.metindex = None
        self.latitude = None
        self.longitude = None
        self.map = None

    def get_conc_dates(self):
        print 'Reading CMAQ dates...'
        tflag1 = array(self.conc['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.conc['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i, j in zip(tflag1, tflag2)]
        self.conc['TSTEP'] = pd.to_datetime(date, format='%Y%j%H')
        self.indexdates = pd.Series(date).drop_duplicates(keep='last').index.values
        self.dates = self.conc.TSTEP[self.indexdates].to_index()
        self.conc = self.conc.isel(TSTEP=self.indexdates)

    def get_metcro2d_dates(self):
        print 'Reading CMAQ METCRO2D dates...'
        tflag1 = array(self.metcro2d['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.metcro2d['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i, j in zip(tflag1, tflag2)]
        self.metcro2d['TSTEP'] = pd.to_datetime(date, format='%Y%j%H')
        self.metindex = pd.Series(date).drop_duplicates(keep='last').index.values
        self.metdates = self.metcro2d.TSTEP[self.metindex].to_index()
        self.metcro2d = self.metcro2d.isel(TSTEP=self.metindex)

    def get_aerodiam_dates(self):
        print 'Reading CMAQ METCRO2D dates...'
        tflag1 = array(self.aerodiam['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.aerodiam['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i, j in zip(tflag1, tflag2)]
        if 'AGRID' in self.conc.FILEDESC:
            self.aerodiam['TSTEP'] = pd.to_datetime(date, format='%Y%j%H') + pd.TimeDelta
        self.aerodiam['TSTEP'] = pd.to_datetime(date, format='%Y%j%H')
        self.aerodiamindex = pd.Series(date).drop_duplicates(keep='last').index.values

    def open_cmaq(self, file):
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
        if self.grid is not None:
            self.conc = self.conc.assign(latitude=self.grid.LAT.squeeze())
            self.conc = self.conc.assign(longitude=self.grid.LON.squeeze())
            self.conc = self.conc.set_coords(['latitude','longitude'])
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
                self.metcro2d = xr.open_mfdataset(self.metcrofnames.tolist(), concat_dim='TSTEP')
            self.metcrokeys = self.metcro2d.keys()
            self.get_metcro2d_dates()
            if self.grid is not None:
                self.metcro2d =	self.metcro2d.assign(latitude=self.grid.LAT.squeeze())
                self.metcro2d =	self.metcor2d.assign(longitude=self.grid.LON.squeeze())
                self.metcro2d =	self.metcro2d.set_coords(['latitude','longitude'])
        except:
            print 'METCRO2D Files Not Found'
            pass

    def open_aerodiam(self, f):
        from glob import glob
        from numpy import sort
        try:
            if type(f) == str:
                self.aerodiamfnames = sort(array(glob(f)))
            else:
                self.aerodiamfnames = sort(array(f))
            if self.aerodiamfnames.shape[0] >= 1:
                self.aerodiam2d = xr.open_mfdataset(self.aerodiamfnames.tolist(), concat_dim='TSTEP')
            self.aerodiamkeys = self.aerodiam2d.keys()
            self.get_aerodiam_dates()
            if self.grid is not None:
                self.aerodiam2d = self.aerodiam2d.assign(latitude=self.grid.LAT.squeeze())
                self.aerodiam2d = self.metcor2d.assign(longitude=self.grid.LON.squeeze())
                self.aerodiam2d = self.aerodiam2d.set_coords(['latitude','longitude'])
        except:
            print 'AERODIAM2D Files Not Found'
            self.aerodiam = None
            pass

    def get_dust_total(self, lay=None):
        keys = self.keys
        cmaqvars = keys[pd.Series(self.dust_totl).isin(keys)]
        if lay is not None:
            var = self.conc[cmaqvars[0]][:][:, lay, :, :].squeeze()
        else:
            var = self.conc[cmaqvars[0]][:][:, :, :, :].squeeze()
        for i in cmaqvars[1:]:
            print '   Getting CMAQ PM DUST: ' + keys[i]
            if lay is not None:
                var += self.conc[i][:, lay, :, :].squeeze()
            else:
                var += self.conc[i][:, :, :, :].squeeze()
        return var

    def get_noy(self, lay=None):
        keys = self.keys
        if 'NOY' in keys:
            if lay is not None:
                var = self.conc['NOY'][:, lay, :, :].squeeze()
            else:
                var = self.conc['NOY'][:, :, :, :]
        else:
            cmaqvars = keys[pd.Series(self.noy_gas).isin(keys)]
            if lay is not None:
                var = self.conc[cmaqvars[0]][:, lay, :, :].squeeze()
            else:
                var = self.conc[cmaqvars[0]][:, :, :, :].squeeze()
            for i in cmaqvars[1:]:
                print '   Getting CMAQ NOy: ' + keys[i]
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze()
                else:
                    var += self.conc[i][:, :, :, :].squeeze()
        return var

    def get_nox(self, lay=None):
        print '   Getting CMAQ NOx:  NO'
        if lay is not None:
            var = self.conc['NO'][:, lay, :, :].squeeze()
        else:
            var = self.conc['NO'][:, :, :, :].squeeze()
        print '   Getting CMAQ NOx:  NO2'
        if lay is not None:
            var += self.conc['NO2'][:, lay, :, :].squeeze()
        else:
            var += self.conc['NO2'][:, :, :, :].squeeze()
        return var

    def get_dust_pm25(self, lay=None):
        keys = self.keys
        cmaqvars = keys[pd.Series(self.dust_pm25).isin(keys)]
        if lay is not None:
            var = self.conc[cmaqvars[0]][:][:, lay, :, :].squeeze()
        else:
            var = self.conc[cmaqvars[0]][:][:, :, :, :].squeeze()
        for i in cmaqvars[1:]:
            print '   Getting CMAQ PM25 DUST: ' + keys[i]
            if lay is not None:
                var += self.conc[i][:, lay, :, :].squeeze()
            else:
                var += self.conc[i][:, :, :, :].squeeze()
        return var

    def get_pm25(self, lay=None):
        from numpy import concatenate
        keys = self.keys
        allvars = concatenate([self.aitken, self.accumulation, self.coarse])
        weights = array(
            [1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
             1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
             1., 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2])
        if 'PM25_TOT' in keys:
            print 'Getting CMAQ PM25: PM25_TOT'
            if lay is not None:
                var = self.conc['PM25_TOT'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PM25_TOT'][:, :, :, :].squeeze()
        else:
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:, lay, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze() * j
                else:
                    var += self.conc[i][:, :, :, :].squeeze() * j
        var.name = 'PM2.5'
        var['long_name'] = 'PM2.5'
        var['var_desc'] = 'Variable PM2.5'

        return var

    def get_pm10(self, lay=None):
        from numpy import concatenate
        keys = self.keys
        allvars = concatenate([self.aitken, self.accumulation, self.coarse])
        var = None
        if 'PMC_TOT' in keys:
            if lay is not None:
                var = self.conc['PMC_TOT'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PMC_TOT'][:, :, :, :].squeeze()
            print 'DONE'
        elif 'PM10' in keys:
            if lay is not None:
                var = self.conc['PMC_TOT'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PMC_TOT'][:, :, :, :].squeeze()
        else:
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            if lay is not None:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze()
            else:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze()
            for i in newkeys[1:]:
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze()
                else:
                    var += self.conc[i][:, :, :, :].squeeze()
        var.name = 'PM10'
        var['long_name'] = 'PM10'
        var['var_desc'] = 'Variable PM10'
        return var

    def get_clf(self, lay=None):
        keys = self.keys
        allvars = array(['ACLI', 'ACLJ', 'ACLK'])
        weights = array([1, 1, .2])
        var = None
        if 'PM25_CL' in keys:
            var = self.conc['PM25_CL'][:, lay, :, :].squeeze()
        else:
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:, 0, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                var += self.conc[i][:, 0, :, :].squeeze() * j
        var.name = 'PM25_CL'
        var['long_name'] = 'PM25_CL'
        var['var_desc'] = 'Variable PM25_CL'
        return var

    def get_naf(self, lay=None):
        keys = self.keys
        allvars = array(['ANAI', 'ANAJ', 'ASEACAT', 'ASOIL', 'ACORS'])
        weights = array([1, 1, .2 * 837.3 / 1000., .2 * 62.6 / 1000., .2 * 2.3 / 1000.])
        if 'PM25_NA' in keys:
            if lay is not None:
                var = self.conc['PM25_NA'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PM25_NA'][:, :, :, :].squeeze()
        else:
            print '    Computing PM25_NA...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            if lay is not None:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze() * neww[0]
            else:
                var = self.conc[newkeys[0]][:, :, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze() * j
                else:
                    var += self.conc[i][:, :, :, :].squeeze() * j
        var.name = 'PM25_NA'
        var['long_name'] = 'PM25_NA'
        var['var_desc'] = 'Variable PM25_NA'

        return var

    def get_kf(self, lay=None):
        keys = self.keys
        allvars = array(['AKI', 'AKJ', 'ASEACAT', 'ASOIL', 'ACORS'])
        weights = array([1, 1, .2 * 31. / 1000., .2 * 24. / 1000., .2 * 17.6 / 1000.])
        if 'PM25_K' in keys:
            if lay is not None:
                var = self.conc['PM25_K'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PM25_K'][:, :, :, :].squeeze()
        else:
            print '    Computing PM25_K...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            if lay is not None:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze() * neww[0]
            else:
                var = self.conc[newkeys[0]][:, :, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze() * j
                else:
                    var += self.conc[i][:, :, :, :].squeeze() * j
        var.name = 'PM25_K'
        var['long_name'] = 'PM25_K'
        var['var_desc'] = 'Variable PM25_K'

        return var

    def get_caf(self, lay=None):
        keys = self.keys
        allvars = array(['ACAI', 'ACAJ', 'ASEACAT', 'ASOIL', 'ACORS'])
        weights = array([1, 1, .2 * 32. / 1000., .2 * 83.8 / 1000., .2 * 56.2 / 1000.])
        if 'PM25_CA' in keys:
            if lay is not None:
                var = self.conc['PM25_CA'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PM25_CA'][:, :, :, :].squeeze()
        else:
            print '    Computing PM25_NO3...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index.values]
            if lay is not None:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze() * neww[0]
            else:
                var = self.conc[newkeys[0]][:, :, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze() * j
                else:
                    var += self.conc[i][:, :, :, :].squeeze() * j
        var.name = 'PM25_CA'
        var['long_name'] = 'PM25_CA'
        var['var_desc'] = 'Variable PM25_CA'
        return var

    def get_so4f(self, lay=None):
        keys = self.keys
        allvars = array(['ASO4I', 'ASO4J', 'ASO4K'])
        weights = array([1., 1., .2])
        if 'PM25_SO4' in keys:
            if lay is not None:
                var = self.conc['PM25_SO4'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PM25_SO4'][:, :, :, :].squeeze()
        else:
            print '    Computing PM25_SO4...'
            index = pd.Series(allvars).isin(keys)
            print index
            newkeys = allvars[index]
            neww = weights[index.values]
            if lay is not None:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze() * neww[0]
            else:
                var = self.conc[newkeys[0]][:, :, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze() * j
                else:
                    var += self.conc[i][:, :, :, :].squeeze() * j
        var.name = 'PM25_SO4'
        var['long_name'] = 'PM25_SO4'
        var['var_desc'] = 'Variable PM25_SO4'
        return var

    def get_nh4f(self, lay=None):
        keys = self.keys
        allvars = array(['ANH4I', 'ANH4J', 'ANH4K'])
        weights = array([1., 1., .2])
        var = None
        if 'PM25_NH4' in keys:
            if lay is not None:
                var = self.conc['PM25_NH4'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PM25_NH4'][:, :, :, :].squeeze()
        else:
            print '    Computing PM25_NH4...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            if lay is not None:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze() * neww[0]
            else:
                var = self.conc[newkeys[0]][:, :, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze() * j
                else:
                    var += self.conc[i][:, :, :, :].squeeze() * j
        var.name = 'PM25_NH4'
        var['long_name'] = 'PM25_NH4'
        var['var_desc'] = 'Variable PM25_NH4'

        return var

    def get_no3f(self, lay=None):
        keys = self.keys
        allvars = array(['ANO3I', 'ANO3J', 'ANO3K'])
        weights = array([1., 1., .2])
        var = None
        if 'PM25_NO3' in keys:
            if lay is not None:
                var = self.conc['PM25_NO3'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PM25_NO3'][:, :, :, :].squeeze()
        else:
            print '    Computing PM25_NO3...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            if lay is not None:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze() * neww[0]
            else:
                var = self.conc[newkeys[0]][:, :, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze() * j
                else:
                    var += self.conc[i][:, :, :, :].squeeze() * j
        var.name = 'PM25_NO3'
        var['long_name'] = 'PM25_NO3'
        var['var_desc'] = 'Variable PM25_NO3'
        return var

    def get_ec(self, lay=None):
        keys = self.keys
        allvars = array(['AECI', 'AECJ'])
        weights = array([1., 1.])
        var = None
        if 'PM25_EC' in keys:
            if lay is not None:
                var = self.conc['PM25_EC'][:, lay, :, :].squeeze()
            else:
                var = self.conc['PM25_EC'][:, :, :, :].squeeze()
        else:
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            if lay is not None:
                var = self.conc[newkeys[0]][:, lay, :, :].squeeze() * neww[0]
            else:
                var = self.conc[newkeys[0]][:, :, :, :].squeeze() * neww[0]
            for i, j in zip(newkeys[1:], neww[1:]):
                if lay is not None:
                    var += self.conc[i][:, lay, :, :].squeeze() * j
                else:
                    var += self.conc[i][:, :, :, :].squeeze() * j
        var.name = 'PM25_EC'
        var['long_name'] = 'PM25_EC'
        var['var_desc'] = 'Variable PM25_EC'

        return var

    def get_var(self, param='O3', lay=None):
        p = param.upper()
        print param
        if p == 'PM25':
            var = self.get_pm25(lay=lay)
        elif p == 'PM10':
            var = self.get_pm10(lay=lay)
        elif p == 'PM25_DUST':
            var = self.get_dust_pm25(lay=lay)
        elif p == 'PM10_DUST':
            var = self.get_dust_total(lay=lay)
        elif p == 'NOX':
            var = self.get_nox(lay=lay)
        elif p == 'NOY':
            var = self.get_noy(lay=lay)
        elif p == 'CLF':
            var = self.get_clf(lay=lay)
        elif p == 'CAF':
            var = self.get_caf(lay=lay)
        elif p == 'NAF':
            var = self.get_naf(lay=lay)
        elif p == 'KF':
            var = self.get_kf(lay=lay)
        elif (p == 'SO4F') | (p == 'PM2.5_SO4'):
            var = self.get_so4f(lay=lay)
        elif p == 'NH4F':
            var = self.get_nh4f(lay=lay)
        elif (p == 'NO3F') | (p == 'PM2.5_NO3'):
            var = self.get_no3f(lay=lay)
        elif (p == 'PM2.5_EC') | (p == 'ECF'):
            var = self.get_ec(lay=lay)
        elif p == 'OC':
            var = self.get_oc(lay=lay)
        elif p == 'VOC':
            var = self.conc['VOC'][:, lay, :, :].squeeze()
        else:
            print '   Getting CMAQ Variable: ' + param
            var = self.conc[param][:, lay, :, :].squeeze()
        return var

    def get_metcro2d_rh(self, lay=None):
        import atmos
        data = {'T': self.metcro2d['TEMP2'][:].compute().values, 'rv': self.metcro2d['Q2'][:].compute().values,
                'p': self.metcro2d['PRSFC'][:].compute().values}
        if lay is None:
            a = atmos.calculate('RH', **data)[:, :, :, :].squeeze()
        else:
            atmos.calculate('RH', **data)[:, lay, :, :].squeeze()
        return a

    def get_metcro2d_var(self, param='TEMPG', lay=None):
        param = param.upper()
        if param == 'RH':
            print '   Calculating CMAQ Variable: ' + param
            var = self.get_metcro2d_rh(lay=lay)
        else:
            print '   Getting CMAQ Variable: ' + param
            if lay is None:
                var = self.metcro2d[param][:, :, :, :].squeeze()
            else:
                var = self.metcro2d[param][:, lay, :, :].squeeze()
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
