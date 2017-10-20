# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1
import cPickle as pickle
from gc import collect
import pandas as pd
import xarray as xr
from netCDF4 import Dataset, MFDataset
from numpy import array, zeros
from dask.diagnostics import ProgressBar
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
        self.conc = None #concentration object
        self.metcro2d = None # metcro2d obj
        self.aerodiam = None # aerodiam obj
        self.grid = None #gridcro2d obj
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
        from datetime import datetime
        from numpy import concatenate, arange
        print 'Reading CMAQ dates...'
        tflag1 = array(self.conc['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.conc['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i,j in zip(tflag1,tflag2)]
        self.conc['TSTEP'] = pd.to_datetime(date,format='%Y%j%H')
        self.indexdates = pd.Series(date).drop_duplicates(keep='last').index.values
        self.dates = self.conc.TSTEP[self.indexdates].to_index()
        
    def get_metcro2d_dates(self):
        from datetime import datetime
        from pandas import DataFrame
        from numpy import concatenate, arange
        print 'Reading CMAQ METCRO2D dates...'
        tflag1 = array(self.metcro2d['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.metcro2d['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i,j in zip(tflag1,tflag2)]
        self.metcro2d['TSTEP'] = pd.to_datetime(date,format='%Y%j%H')
        self.metindex = pd.Series(date).drop_duplicates(keep='last').index.values
        self.metdates = self.conc.TSTEP[self.metindex].to_index()

    def get_aerodiam_dates(self):
        from datetime import datetime
        from pandas import DataFrame
        from numpy import concatenate, arange
        print 'Reading CMAQ METCRO2D dates...'
        tflag1 = array(self.aerodiam['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.aerodiam['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = [i + j.zfill(2) for i,j in zip(tflag1,tflag2)]
        if 'AGRID' in self.conc.FILEDESC:
            self.aerodiam['TSTEP'] = pd.to_datetime(date,format='%Y%j%H') + pd.TimeDelta
        self.aerodiam['TSTEP'] = pd.to_datetime(date,format='%Y%j%H')
        self.aerodiamindex = pd.Series(date).drop_duplicates(keep='last').index.values

    def open_cmaq(self, file):
        # file can be a single file or many files
        # example:
        #       file='this_is_my_file.ncf'
        #       file='this_is_my_files*.ncf'
        from glob import glob
        from numpy import sort
        if type(file) == str:
            self.fname = sort(array(glob(file)))
        else:
            self.fname = sort(array(file))
        if self.fname.shape[0] >= 1:
#            print self.fname
            self.conc = xr.open_mfdataset(self.fname.tolist(),concat_dim='TSTEP')
        else:
            print 'Files not found'
        self.get_conc_dates()
        self.keys = self.conc.keys()

    def open_metcro2d(self,f):
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
                self.metcro2d = xr.open_mfdataset(self.metcrofnames.tolist(),concat_dim='TSTEP')
            self.metcrokeys =  self.metcro2d.keys()
            self.get_metcro2d_dates()
        except:
            print 'METCRO2D Files Not Found'
            pass

    def open_aerodiam(self,f):
        from glob import glob
        from numpy import sort
        try:
            if type(f) == str:
                self.aerodiamfnames = sort(array(glob(f)))
            else:
                self.aerodiamfnames = sort(array(f))
            if self.aerodiamfnames.shape[0] >= 1:
                self.aerodiam2d = xr.open_mfdataset(self.aerodiamfnames.tolist(),concat_dim='TSTEP')
            self.aerodiamkeys =  self.aerodiam2d.keys()
            self.get_aerodiam_dates()
        except:
            print 'AERODIAM2D Files Not Found'
            self.aerodiam=None
            pass

    def get_surface_dust_total(self):
        keys = self.keys
        cmaqvars, temp = search_listinlist(keys, self.dust_total)
        var = zeros(self.conc[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
        for i in cmaqvars[:]:
            print '   Getting CMAQ PM DUST: ' + keys[i]
            var += self.conc[keys[i]][self.indexdates, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_noy(self):
        keys = self.keys
        if 'NOY' in keys:
            var = self.conc['NOY'][:][self.indexdates, 0, :, :]
        else:
            cmaqvars, temp = search_listinlist(keys, self.noy_gas)
            var = zeros(self.conc[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ NOy: ' + keys[i]
                var += self.conc[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_nox(self):
        print '   Getting CMAQ NOx:  NO'
        var = self.conc['NO'][self.indexdates, 0, :, :].squeeze()
        print '   Getting CMAQ NOx:  NO2'
        var += self.conc['NO2'][self.indexdates, 0, :, :].squeeze()
        collect()
        return var

    def get_surface_dust_pm25(self):
        keys = self.keys
#        allvars =
        cmaqvars, temp = search_listinlist(keys, self.dust_pm25)
        var = zeros(self.conc[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
        for i in cmaqvars[:]:
            print '   Getting CMAQ PM25 DUST: ' + keys[i]
            var += self.conc[keys[i]][self.indexdates, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_pm25(self):
        from numpy import concatenate
        keys = self.keys
        allvars = concatenate([self.aitken, self.accumulation,self.coarse])
        weights = array([ 1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1., 1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1., 1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1., 1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1.,  1., 1.,  1.,  1.,  1.,  0.2,  0.2,  0.2,  0.2,  0.2,  0.2,  0.2])
        if 'PM25_TOT' in keys:
            print 'Getting CMAQ PM25: PM25_TOT'
            var = self.conc['PM25_TOT'][self.indexdates, 0, :, :].squeeze()
        else:
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM2.5'
        var['long_name'] = 'PM2.5'
        var['var_desc'] = 'Variable PM2.5'

        return var

    def get_surface_pm10(self):
        from numpy import concatenate
        keys = self.keys
        allvars = concatenate([self.aitken, self.accumulation, self.coarse])
        var = None
        if 'PMC_TOT' in keys:
            var = self.conc['PMC_TOT'][self.indexdates, 0, :, :].squeeze()
            print 'DONE'
        elif 'PM10' in keys:
            var = self.conc['PMC_TOT'][self.indexdates, 0, :, :].squeeze()
        else:
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze()
            for i in newkeys[1:]:
                var += self.conc[i][:,0,:,:].squeeze()
        var.name = 'PM10'
        var['long_name'] = 'PM10'
        var['var_desc'] = 'Variable PM10'
        return var

    def get_surface_clf(self):
        keys = self.keys
        allvars = array(['ACLI', 'ACLJ','ACLK'])
        weights = array([1,1,.2])
        var = None
        if 'PM25_CL' in keys:
            var = self.conc['PM25_CL'][self.indexdates, 0, :, :].squeeze()
        else:
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM25_CL'
        var['long_name'] = 'PM25_CL'
        var['var_desc'] = 'Variable PM25_CL'
        return var

    def get_surface_naf(self):
        keys = self.keys
        allvars = array(['ANAI', 'ANAJ','ASEACAT','ASOIL','ACORS'])
        weights = array([1,1,.2*837.3/1000.,.2*62.6/1000.,.2*2.3/1000.])
        var = None
        if 'PM25_NA' in keys:
            var = self.conc['PM25_NA'][self.indexdates, 0, :, :].squeeze()
        else:
            print '    Computing PM25_NA...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM25_NA'
        var['long_name'] = 'PM25_NA'
        var['var_desc'] = 'Variable PM25_NA'

        return var

    def get_surface_kf(self):
        keys = self.keys
        allvars = array(['AKI', 'AKJ','ASEACAT','ASOIL','ACORS'])
        weights = array([1,1,.2*31./1000.,.2*24./1000.,.2*17.6/1000.])
        var = None
        if 'PM25_K' in keys:
            var = self.conc['PM25_K'][self.indexdates, 0, :, :].squeeze()
        else:
            print '    Computing PM25_K...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM25_K'
        var['long_name'] = 'PM25_K'
        var['var_desc'] = 'Variable PM25_K'

        return var

    def get_surface_caf(self):
        keys = self.keys
        allvars = array(['ACAI', 'ACAJ','ASEACAT','ASOIL','ACORS'])
        weights = array([1,1,.2*32./1000.,.2*83.8/1000.,.2*56.2/1000.])
        var = None
        if 'PM25_CA' in keys:
            var = self.conc['PM25_CA'][self.indexdates, 0, :, :].squeeze()
        else:
            print '    Computing PM25_NO3...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index.values]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM25_CA'
        var['long_name'] = 'PM25_CA'
        var['var_desc'] = 'Variable PM25_CA'

        return var

    def get_surface_so4f(self):
        keys = self.keys
        allvars = array(['ASO4I', 'ASO4J','ASO4K'])
        weights = array([1.,1.,.2])
        var = None
        if 'PM25_SO4' in keys:
            var = self.conc['PM25_SO4'][self.indexdates, 0, :, :].squeeze()
        else:
            print '    Computing PM25_SO4...'
            index = pd.Series(allvars).isin(keys)
            print index
            newkeys = allvars[index]
            neww = weights[index.values]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM25_SO4'
        var['long_name'] = 'PM25_SO4'
        var['var_desc'] = 'Variable PM25_SO4'

        return var

    def get_surface_nh4f(self):
        keys = self.keys
        allvars = array(['ANH4I', 'ANH4J','ANH4K'])
        weights = array([1.,1.,.2])
        var = None
        if 'PM25_NH4' in keys:
            var = self.conc['PM25_NH4'][self.indexdates, 0, :, :].squeeze()
        else:
            print '    Computing PM25_NH4...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM25_NH4'
        var['long_name'] = 'PM25_NH4'
        var['var_desc'] = 'Variable PM25_NH4'

        return var

    def get_surface_no3f(self):
        keys = self.keys
        allvars = array(['ANO3I', 'ANO3J','ANO3K'])
        weights = array([1.,1.,.2])
        var = None
        if 'PM25_NO3' in keys:
            var = self.conc['PM25_NO3'][self.indexdates, 0, :, :].squeeze()
        else:
            print '    Computing PM25_NO3...'
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM25_NO3'
        var['long_name'] = 'PM25_NO3'
        var['var_desc'] = 'Variable PM25_NO3'
        return var

    def get_surface_ec(self):
        keys = self.keys
        allvars = array(['AECI', 'AECJ'])
        weights = array([1.,1.])
        var = None
        if 'PM25_EC' in keys:
            var = self.conc['PM25_EC'][self.indexdates, 0, :, :].squeeze()
        else:
            index = pd.Series(allvars).isin(keys)
            newkeys = allvars[index]
            neww = weights[index]
            var = self.conc[newkeys[0]][:,0,:,:].squeeze() * neww[0]
            for i,j in zip(newkeys[1:],neww[1:]):
                var += self.conc[i][:,0,:,:].squeeze() * j
        var.name = 'PM25_EC'
        var['long_name'] = 'PM25_EC'
        var['var_desc'] = 'Variable PM25_EC'

        return var

    # def get_surface_oc(self):
    #     keys = self.keys
    #     allvars = array(['AXYL1J', 'AXYL2J', 'AXYL3J', 'ATOL1J', 'ATOL2J', 'ATOL3J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J',
    #                      'AISO1J', 'AISO2J', 'AISO3J', 'ATRP1J', 'ATRP2J', 'ASQTJ', 'AALK1J', 'AALK2J', 'AORGCJ',
    #                      'AOLGBJ', 'AOLGAJ', 'APOCI', 'APOCJ', 'APAH1J', 'APAH2J', 'APAH3J'])
    #     weights = array([.5,.5,.5,.5,.5,.5,.5,.5,.5,.625,.625,.37
    #     var = None
    #     if 'PM25_OC' in keys:
    #         var = self.conc['PM25_OC'][self.indexdates, 0, :, :].squeeze()
    #     else:
    #         cmaqvars, temp = search_listinlist(keys, allvars)
    #     OC = var
    #     if temp.shape[0] != allvars.shape[0]:
    #         cmaqvars, temp = search_listinlist(keys, self.poc)
    #         var = zeros(self.conc[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
    #         for i in cmaqvars[:]:
    #             print '   Getting CMAQ Variable: ' + keys[i]
    #             var += self.conc[keys[i]][self.indexdates, 0, :, :].squeeze()
    #             collect()
    #         OC = var
    #     else:
    #         vars = []
    #         for i in allvars:
    #             if i not in keys:
    #                 print '    Variable ' + i + ' not found'
    #                 var = zeros((self.indexdates.shape[0], self.latitude.shape[0], self.longitude.shape[0]))
    #             else:
    #                 print '   Getting CMAQ Variable: ' + i
    #                 var = self.conc[i][self.indexdates, 0, :, :].squeeze()
    #             vars.append(var)
    #         OC = (vars[0] + vars[1] + vars[2]) / 2.0 + (vars[3] + vars[4] + vars[5]) / 2.0 + (vars[6] + vars[7] + vars[
    #             8]) / 2.0 + (vars[9] + vars[10]) / 1.6 + vars[11] / 2.7 + (vars[12] + vars[13]) / 1.4 + vars[
    #                                                                                                         14] / 2.1 + 0.64 * (
    #             vars[15] + vars[16]) + vars[17] / 2.0 + (vars[18] + vars[19]) / 2.1 + vars[20] + vars[21] + vars[
    #                                                                                                             22] / 2.03 + \
    #              vars[23] / 2.03 + vars[24] / 2.03
    #     collect()
    #     return OC

    def get_surface_cmaqvar(self, param='O3'):
        lvl = 0.
        revert = param
        p = param.upper()
        print param
        if p == 'PM25':
            var = self.get_surface_pm25()
        elif p == 'PM10':
            var = self.get_surface_pm10()
        elif p == 'PM25_DUST':
            var = self.get_surface_dust_pm25()
        elif p == 'PM10_DUST':
            var = self.get_surface_dust_total()
        elif p == 'NOX':
            var = self.get_surface_nox()
        elif p == 'NOY':
            var = self.get_surface_noy()
        elif p == 'CLF':
            var = self.get_surface_clf()
        elif p == 'CAF':
            var = self.get_surface_caf()
        elif p == 'NAF':
            var = self.get_surface_naf()
        elif p == 'KF':
            var = self.get_surface_kf()
        elif (p == 'SO4F') | (p == 'PM2.5_SO4'):
            var = self.get_surface_so4f()
        elif p == 'NH4F':
            var = self.get_surface_nh4f()
        elif (p == 'NO3F') | (p == 'PM2.5_NO3'):
            var = self.get_surface_no3f()
        elif (p == 'PM2.5_EC') | (p == 'ECF'):
            var = self.get_surface_ec()
        elif (p == 'OC'):
            var = self.get_surface_oc()
        elif (p == 'VOC'):
            var = self.conc['VOC'][self.indexdates, 0, :, :].squeeze()
        else:
            print '   Getting CMAQ Variable: ' + param
            var = self.conc[param][self.indexdates, 0, :, :].squeeze()
        return var

    def get_metcro2d_rh(self):
        import atmos
        data = {'T': self.metcro2d['TEMP2'][:].compute().values, 'rv': self.metcro2d['Q2'][:].compute().values, 'p': self.metcro2d['PRSFC'][:].compute().values}
        return atmos.calculate('RH', **data)[self.metindex,0,:,:].squeeze()

    def get_metcro2d_cmaqvar(self, param='TEMPG', lvl=0.):
        param = param.upper()
        if param == 'RH':
            print '   Calculating CMAQ Variable: ' + param
            var = self.get_metcro2d_rh()
        else:
            print '   Getting CMAQ Variable: ' + param
            var = self.metcro2d[param][self.metindex, 0, :, :].squeeze()
        return var

    def set_gridcro2d(self, filename):
        self.grid = xr.open_dataset(filename)
        lat = 'LAT'
        lon = 'LON'
        self.latitude = self.grid[lat][:][0, 0, :, :].squeeze().compute()
        self.longitude = self.grid[lon][:][0, 0, :, :].squeeze().compute()
        self.load_conus_basemap(res='l')

    def load_conus_basemap(self,res='l'):
        import cPickle as pickle
        from mpl_toolkits.basemap import Basemap
        import os
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
