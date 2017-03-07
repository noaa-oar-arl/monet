# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1
import cPickle as pickle
from gc import collect

from netCDF4 import Dataset, MFDataset
from numpy import array, zeros

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
        self.concobj = None
        self.metcro2d = None
        self.gridobj = None
        self.fname = None
        self.metcrofnames = None
        self.dates = None
        self.keys = None
        self.metcrokeys = []
        self.indexdates = None
        self.latitude = None
        self.longitude = None
        self.map = None

    def get_dates(self):
        from datetime import datetime
        from pandas import DataFrame
        from numpy import concatenate, arange
        tflag1 = array(self.concobj.variables['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.concobj.variables['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = []
        for i, j in zip(tflag1, tflag2):
            date.append(datetime.strptime(i + j, '%Y%j%H'))
        self.dates = array(date)
        r = DataFrame(self.dates, columns=['dates'])
        if r.dates.count() > len(r.dates.unique()):
            self.indexdates = concatenate([arange(24), r.dates[r.dates.duplicated()].index.values])
        else:
            self.indexdates = arange(len(date))

    def load_single_cmaq_run(self):
        self.concobj = Dataset(self.fname[0])

    def load_multi_metcro2d_cmaq_runs(self):
        self.metcro2d = MFDataset(self.metcrofnames)

    def load_multi_cmaq_runs(self):
        self.concobj = MFDataset(self.fname)

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
            self.load_multi_cmaq_runs()
        else:
            print 'Files not found'
        self.keys = self.get_keys(self.concobj)

    def open_metcro2d(self, file):
        from glob import glob
        from numpy import sort
        if self.metcrofnames == None:
            self.metcrofnames = sort(array(glob(file)))
        else:
            self.fname = sort(array(file))
        if self.fname.shape[0] >= 1:
            self.load_multi_metcro2d_cmaq_runs()
        self.metcrokeys = self.get_keys(self.metcro2d)

    def get_keys(self, cdfobj):
        return array(cdfobj.variables.keys()).astype('|S10')

    def get_surface_dust_total(self):
        keys = self.keys
        cmaqvars, temp = search_listinlist(keys, self.dust_total)
        var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
        for i in cmaqvars[:]:
            print '   Getting CMAQ PM DUST: ' + keys[i]
            var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_noy(self):
        keys = self.keys
        if 'NOY' in keys:
            var = self.concobj.variables['NOY'][:][self.indexdates, 0, :, :]
        else:
            cmaqvars, temp = search_listinlist(keys, self.noy_gas)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ NOy: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_nox(self):
        print '   Getting CMAQ NOx:  NO'
        var = self.concobj.variables['NO'][self.indexdates, 0, :, :].squeeze()
        print '   Getting CMAQ NOx:  NO2'
        var += self.concobj.variables['NO2'][self.indexdates, 0, :, :].squeeze()
        collect()
        return var

    def get_surface_dust_pm25(self):
        keys = self.keys
        cmaqvars, temp = search_listinlist(keys, self.dust_pm25)
        var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
        for i in cmaqvars[:]:
            print '   Getting CMAQ PM25 DUST: ' + keys[i]
            var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_pm25(self):
        from numpy import concatenate
        keys = self.keys
        allvars = concatenate([self.aitken, self.accumulation])
        if 'PM25_TOT' in keys:
            print 'Getting CMAQ PM25: PM25_TOT'
            var = self.concobj.variables['PM25_TOT'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze()
            for i in cmaqvars[:]:
                print '   Getting CMAQ PM25: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_pm10(self):
        from numpy import concatenate
        keys = self.keys
        allvars = concatenate([self.aitken, self.accumulation, self.coarse])
        var = None
        if 'PMC_TOT' in keys:
            var = self.concobj.variables['PMC_TOT'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ PM10: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_clf(self):
        keys = self.keys
        allvars = array(['ACLI', 'ACLJ'])
        var = None
        if 'PM25_CL' in keys:
            var = self.concobj.variables['PM25_CL'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_naf(self):
        keys = self.keys
        allvars = array(['ANAI', 'ANAJ'])
        var = None
        if 'PM25_NA' in keys:
            var = self.concobj.variables['PM25_NA'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_kf(self):
        keys = self.keys
        allvars = array(['AKI', 'AKJ'])
        var = None
        if 'PM25_K' in keys:
            var = self.concobj.variables['PM25_K'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_caf(self):
        keys = self.keys
        allvars = array(['ACAI', 'ACAJ'])
        var = None
        if 'PM25_CA' in keys:
            var = self.concobj.variables['PM25_CA'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_so4f(self):
        keys = self.keys
        allvars = array(['ASO4I', 'ASO4J'])
        var = None
        if 'PM25_SO4' in keys:
            var = self.concobj.variables['PM25_SO4'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_nh4f(self):
        keys = self.keys
        allvars = array(['ANH4I', 'ANH4J'])
        var = None
        if 'PM25_NH4' in keys:
            var = self.concobj.variables['PM25_NH4'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_no3f(self):
        keys = self.keys
        allvars = array(['ANO3I', 'ANO3J'])
        var = None
        if 'PM25_NO3' in keys:
            var = self.concobj.variables['PM25_NO3'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_ec(self):
        keys = self.keys
        allvars = array(['AECI', 'AECJ'])
        var = None
        if 'PM25_EC' in keys:
            var = self.concobj.variables['PM25_EC'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_oc(self):
        keys = self.keys
        allvars = array(['AXYL1J', 'AXYL2J', 'AXYL3J', 'ATOL1J', 'ATOL2J', 'ATOL3J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J',
                         'AISO1J', 'AISO2J', 'AISO3J', 'ATRP1J', 'ATRP2J', 'ASQTJ', 'AALK1J', 'AALK2J', 'AORGCJ',
                         'AOLGBJ', 'AOLGAJ', 'APOCI', 'APOCJ', 'APAH1J', 'APAH2J', 'APAH3J'])
        var = None
        if 'PM25_OC' in keys:
            var = self.concobj.variables['PM25_OC'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
        OC = var
        if temp.shape[0] != allvars.shape[0]:
            cmaqvars, temp = search_listinlist(keys, self.poc)
            var = zeros(self.concobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ Variable: ' + keys[i]
                var += self.concobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
            OC = var
        else:
            vars = []
            for i in allvars:
                if i not in keys:
                    print '    Variable ' + i + ' not found'
                    var = zeros((self.indexdates.shape[0], self.latitude.shape[0], self.longitude.shape[0]))
                else:
                    print '   Getting CMAQ Variable: ' + i
                    var = self.concobj.variables[i][self.indexdates, 0, :, :].squeeze()
                vars.append(var)
            OC = (vars[0] + vars[1] + vars[2]) / 2.0 + (vars[3] + vars[4] + vars[5]) / 2.0 + (vars[6] + vars[7] + vars[
                8]) / 2.0 + (vars[9] + vars[10]) / 1.6 + vars[11] / 2.7 + (vars[12] + vars[13]) / 1.4 + vars[
                                                                                                            14] / 2.1 + 0.64 * (
                vars[15] + vars[16]) + vars[17] / 2.0 + (vars[18] + vars[19]) / 2.1 + vars[20] + vars[21] + vars[
                                                                                                                22] / 2.03 + \
                 vars[23] / 2.03 + vars[24] / 2.03
        collect()
        return OC

    def get_surface_cmaqvar(self, param='O3'):
        lvl = 0.
        param = param.upper()
        print param
        if param == 'PM25':
            var = self.get_surface_pm25()
        elif param == 'PM10':
            var = self.get_surface_pm10()
        elif param == 'PM25_DUST':
            var = self.get_surface_dust_pm25()
        elif param == 'PM10_DUST':
            var = self.get_surface_dust_total()
        elif param == 'NOX':
            var = self.get_surface_nox()
        elif param == 'NOY':
            var = self.get_surface_noy()
        elif param == 'CLF':
            var = self.get_surface_clf()
        elif param == 'CAF':
            var = self.get_surface_caf()
        elif param == 'NAF':
            var = self.get_surface_naf()
        elif param == 'KF':
            var = self.get_surface_kf()
        elif (param == 'SO4F') | (param == 'PM2.5_SO4'):
            var = self.get_surface_so4f()
        elif param == 'NH4F':
            var = self.get_surface_nh4f()
        elif (param == 'NO3F') | (param == 'PM2.5_NO3'):
            var = self.get_surface_no3f()
        elif (param == 'PM2.5_EC') | (param == 'ECF'):
            var = self.get_surface_ec()
        elif (param == 'OC'):
            var = self.get_surface_oc()
        elif (param == 'VOC'):
            var = self.concobj.variables['VOC'][self.indexdates, 0, :, :].squeeze()
        else:
            print '   Getting CMAQ Variable: ' + param
            var = self.concobj.variables[param][self.indexdates, 0, :, :].squeeze()
        return var

    def get_metcro2d_rh(self):
        import atmos
        data = {'T': self.metcro2d.variables['TEMP2'][:], 'rv': self.metcro2d.variables['Q2'][:],
                'P': self.metcro2d.variables['PRSFC'][:]}
        return atmos.calculate('RH', **data)

    def get_metcro2d_cmaqvar(self, param='TEMPG', lvl=0.):
        param = param.upper()
        if param == 'RH':
            print '   Calculating CMAQ Variable: ' + param
            var = self.get_metcro2d_rh()
        else:
            print '   Getting CMAQ Variable: ' + param
            var = self.metcro2d.variables[param][self.indexdates, 0, :, :].squeeze()
        return var

    def set_gridcro2d(self, filename=''):
        self.gridobj = Dataset(filename)
        lat = 'LAT'
        lon = 'LON'
        self.latitude = self.gridobj.variables[lat][:][0, 0, :, :].squeeze()
        self.longitude = self.gridobj.variables[lon][:][0, 0, :, :].squeeze()

    def load_conus_basemap(self, path):
        import cPickle as pickle
        from mpl_toolkits.basemap import Basemap
        import os
        if isinstance(self.map, type(None)):
            if os.path.isfile(path):
                print 'Loading from file'
                m = pickle.load(open(path, 'rb'))
            else:
                lat1 = self.gridobj.P_ALP
                lat2 = self.gridobj.P_BET
                lon1 = self.gridobj.P_GAM
                lon0 = self.gridobj.XCENT
                lat0 = self.gridobj.YCENT
                m = Basemap(projection='lcc', resolution='i', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0,
                            lon_1=lon1,
                            llcrnrlat=self.latitude[0, 0], urcrnrlat=self.latitude[-1, -1],
                            llcrnrlon=self.longitude[0, 0],
                            urcrnrlon=self.longitude[-1, -1], rsphere=6371200.,
                            area_thresh=50.)
            self.map = m
        else:
            m = self.map
        return self.map

    def load_pacific_coast_basemap(self, path):
        from mpl_toolkits.basemap import Basemap
        import os
        fname = path + '/basemap-cmaq_pacific.p'
        if os.path.isfile(fname):
            pickle.load(open(path, 'rb'))
        else:
            lat1 = self.gridobj.P_ALP
            lat2 = self.gridobj.P_BET
            lon1 = self.gridobj.P_GAM
            lon0 = self.gridobj.XCENT
            lat0 = self.gridobj.YCENT
            m = Basemap(projection='lcc', resolution='l', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=29., urcrnrlat=53., llcrnrlon=-125., urcrnrlon=-116., rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_rockies_basemap(self, path):
        import cPickle as pickle
        from mpl_toolkits.basemap import Basemap
        import os
        fname = path + '/basemap-cmaq_rockies.p'
        if os.path.isfile(fname):
            pickle.load(open(path, 'rb'))
        else:
            lat1 = self.gridobj.P_ALP
            lat2 = self.gridobj.P_BET
            lon1 = self.gridobj.P_GAM
            lon0 = self.gridobj.XCENT
            lat0 = self.gridobj.YCENT
            m = Basemap(projection='lcc', resolution='i', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=29., urcrnrlat=52., llcrnrlon=-116., urcrnrlon=-91., rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_south_central_basemap(self, path):
        import cPickle as pickle
        from mpl_toolkits.basemap import Basemap
        import os
        fname = path + '/basemap-cmaq_southcentral.p'
        if os.path.isfile(fname):
            pickle.load(open(path, 'rb'))
        else:
            lat1 = self.gridobj.P_ALP
            lat2 = self.gridobj.P_BET
            lon1 = self.gridobj.P_GAM
            lon0 = self.gridobj.XCENT
            lat0 = self.gridobj.YCENT
            lat = self.latitude
            lon = self.longitude
            m = Basemap(projection='lcc', resolution='i', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=25., urcrnrlat=37.5, llcrnrlon=-108, urcrnrlon=-86., rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_northeast_basemap(self, path):
        import cPickle as pickle
        from mpl_toolkits.basemap import Basemap
        import os
        fname = path + '/basemap-cmaq_northeast.p'
        if os.path.isfile(fname):
            pickle.load(open(path, 'rb'))
        else:
            lat1 = self.gridobj.P_ALP
            lat2 = self.gridobj.P_BET
            lon1 = self.gridobj.P_GAM
            lon0 = self.gridobj.XCENT
            lat0 = self.gridobj.YCENT
            m = Basemap(projection='lcc', resolution='l', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=37., urcrnrlat=46.5, llcrnrlon=-83.5, urcrnrlon=-61.5, rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_north_central_basemap(self, path):
        import cPickle as pickle
        from mpl_toolkits.basemap import Basemap
        import os
        fname = path + '/basemap-cmaq_northeast.p'
        if os.path.isfile(fname):
            pickle.load(open(path, 'rb'))
        else:
            lat1 = self.gridobj.P_ALP
            lat2 = self.gridobj.P_BET
            lon1 = self.gridobj.P_GAM
            lon0 = self.gridobj.XCENT
            lat0 = self.gridobj.YCENT
            m = Basemap(projection='lcc', resolution='l', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=37., urcrnrlat=46.5, llcrnrlon=-83.5, urcrnrlon=-61.5, rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_southeast_basemap(self, path):
        import cPickle as pickle
        from mpl_toolkits.basemap import Basemap
        import os
        fname = path + '/basemap-cmaq_southeast.p'
        if os.path.isfile(fname):
            pickle.load(open(path, 'rb'))
        else:
            lat1 = self.gridobj.P_ALP
            lat2 = self.gridobj.P_BET
            lon1 = self.gridobj.P_GAM
            lon0 = self.gridobj.XCENT
            lat0 = self.gridobj.YCENT
            m = Basemap(projection='lcc', resolution='l', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=25., urcrnrlat=39, llcrnrlon=-93., urcrnrlon=-70., rsphere=6371200.,
                        area_thresh=10.)
        return m

    def choose_map(self, path, region):
        region = region.upper()
        if region == 'NORTHEAST':
            m = self.load_northeast_basemap(path)
        elif region == 'SOUTHEAST':
            m = self.load_southeast_basemap(path)
        elif region == 'SOUTH CENTRAL':
            m = self.load_south_central_basemap(path)
        elif region == 'ROCKIES':
            m = self.load_rockies_basemap(path)
        elif region == 'PACIFIC':
            m = self.load_pacific_coast_basemap(path)
        elif region == 'NORTH CENTRAL':
            m = self.load_north_central_basemap(path)
        else:
            m = self.load_conus_basemap(path)
        return m
