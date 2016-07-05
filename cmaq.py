# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1
from gc import collect

from netCDF4 import Dataset, MFDataset
from numpy import array, zeros

from tools import search_listinlist

import cPickle as pickle


class cmaq:
    def __init__(self):
        self.objtype = 'CMAQ'
        self.dust_pm25 = array(
                ['ASO4J', 'ANO3J', 'ACLJ', 'ANH4J', 'ANAJ', 'ACAJ', 'AMGJ', 'AKJ', 'APOCJ', 'APNCOMJ', 'AECJ', 'AFEJ',
                 'AALJ', 'ASIJ', 'ATIJ', 'AMNJ', 'AH2OJ', 'AOTHRJ'])
        self.dust_total = array(
                ['ASO4J', 'ASO4K', 'ANO3J', 'ANO3K', 'ACLJ', 'ACLK', 'ANH4J', 'ANAJ', 'ACAJ', 'AMGJ', 'AKJ', 'APOCJ',
                 'APNCOMJ', 'AECJ', 'AFEJ', 'AALJ', 'ASIJ', 'ATIJ', 'AMNJ', 'AH2OJ', 'AOTHRJ', 'ASOIL'])
        self.aitken = array(['ACLI', 'AECI', 'AH2OI', 'ANAI', 'ANH4I',
                             'ANO3I', 'AOTHRI', 'APNCOMI', 'APOCI',
                             'ASO4I', 'A25I', 'AORGAI', 'AORGPAI',
                             'AORGBI'])
        self.accumulation = array(
                ['AALJ', 'AALK1J', 'AALK2J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J', 'ACAJ', 'ACLJ', 'AECJ', 'AFEJ', 'AH2OJ',
                 'AISO1J', 'AISO2J', 'AISO3J', 'AKJ', 'AMGJ', 'AMNJ', 'ANAJ', 'ANH4J', 'ANO3J', 'AOLGAJ', 'AOLGBJ',
                 'AORGCJ', 'AOTHRJ', 'APAH1J', 'APAH2J', 'APAH3J', 'APNCOMJ', 'APOCJ', 'ASIJ', 'ASO4J', 'ASQTJ', 'ATIJ',
                 'ATOL1J', 'ATOL2J', 'ATOL3J', 'ATRP1J', 'ATRP2J', 'AXYL1J', 'AXYL2J', 'AXYL3J', 'A25J', 'AORGAJ',
                 'AORGPAJ', 'AORGBJ'])
        self.coarse = array(['ACLK', 'ACORS', 'AH2OK', 'ANH4K', 'ANO3K', 'ASEACAT', 'ASO4K', 'ASOIL'])
        self.noy_gas = array(
                ['NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA', 'NTR', 'CRON', 'CRN2', 'CRNO',
                 'CRPX', 'OPAN'])

        self.cdfobj = None
        self.gridobj = None
        self.fname = None
        self.dates = None
        self.keys = None
        self.indexdates = None

    def get_single_var(self, param):
        return self.cdfobj.variables[param][:]

    def get_dates(self):
        from datetime import datetime
        from pandas import DataFrame
        from numpy import concatenate, arange, shape
        tflag1 = array(self.cdfobj.variables['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.cdfobj.variables['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = []
        for i, j in zip(tflag1, tflag2):
            date.append(datetime.strptime(i + j, '%Y%j%H'))
        self.dates = array(date)
        r = DataFrame(date, columns=['dates'])
        if r.dates.count() > len(r.dates.unique()):
            print 'here'
            self.indexdates = concatenate([arange(24), r.dates[r.dates.duplicated()].index.values])
        else:
            self.indexdates = arange(len(date))

    def load_single_cmaq_run(self):
        self.cdfobj = Dataset(self.fname[0])

    def load_multi_cmaq_runs(self):
        self.cdfobj = MFDataset(self.fname)

    def open_cmaq(self, file=''):
        # file can be a single file or many files
        # example:
        #       file='this_is_my_file.ncf'
        #       file='this_is_my_files*.ncf'
        from glob import glob
        self.fname = array(glob(file))
        if self.fname.shape[0] > 1:
            self.load_multi_cmaq_runs()
        else:
            self.load_single_cmaq_run()
        self.get_keys()

    def get_keys(self):
        self.keys = array(self.cdfobj.variables.keys()).astype('|S10')

    def get_surface_dust_total(self):
        keys = self.keys
        cmaqvars, temp = search_listinlist(keys, self.dust_total)
        var = zeros(self.cdfobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
        for i in cmaqvars[:]:
            print '   Getting CMAQ PM DUST: ' + keys[i]
            var += self.cdfobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_noy(self):
        keys = self.keys
        if 'NOY' in keys:
            var = self.cdfobj.variables['NOY'][:][:, 0, :, :]
        else:
            cmaqvars, temp = search_listinlist(keys, self.noy_gas)
            var = zeros(self.cdfobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
            for i in cmaqvars[:]:
                print '   Getting CMAQ NOy: ' + keys[i]
                var += self.cdfobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_nox(self):
        print '   Getting CMAQ NOx:  NO'
        var = self.cdfobj.variables['NO'][:, 0, :, :].squeeze()
        print '   Getting CMAQ NOx:  NO2'
        var += self.cdfobj.variables['NO2'][self.indexdates, 0, :, :].squeeze()
        collect()
        return var

    def get_surface_dust_pm25(self):
        keys = self.keys
        cmaqvars, temp = search_listinlist(keys, self.dust_pm25)
        var = zeros(self.cdfobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
        for i in cmaqvars[:]:
            print '   Getting CMAQ PM25 DUST: ' + keys[i]
            var += self.cdfobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_pm25(self):
        from numpy import concatenate
        keys = self.keys
        allvars = concatenate([self.aitken, self.accumulation])
        if 'PM25_TOT' in keys:
            print 'Getting CMAQ PM25: PM25_TOT'
            var = self.cdfobj.variables['PM25_TOT'][self.indexdates, 0, :, :].squeeze()
        else:
            cmaqvars, temp = search_listinlist(keys, allvars)
            var = self.cdfobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze()
            for i in cmaqvars[:]:
                print '   Getting CMAQ PM25: ' + keys[i]
                var += self.cdfobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
                collect()
        return var

    def get_surface_pm10(self):
        from numpy import concatenate
        keys = self.keys
        allvars = concatenate([self.aitken, self.accumulation, self.coarse])
        cmaqvars, temp = search_listinlist(keys, allvars)
        var = zeros(self.cdfobj.variables[keys[cmaqvars[0]]][:][self.indexdates, 0, :, :].squeeze().shape)
        for i in cmaqvars[:]:
            print '   Getting CMAQ PM10: ' + keys[i]
            var += self.cdfobj.variables[keys[i]][self.indexdates, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_cmaqvar(self, param='O3'):
        lvl = 0.
        param = param.upper()
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
        else:
            print '   Getting CMAQ Variable: ' + param
            var = self.cdfobj.variables[param][self.indexdates, 0, :, :].squeeze()
        return var

    def set_gridcro2d(self, filename=''):
        self.gridobj = Dataset(filename)

    def load_conus_basemap(self, path):
        from six.moves import cPickle as pickle
        from mpl_toolkits.basemap import Basemap
        import os
        if os.path.isfile(path):
            print 'Loading from file'
            m = pickle.load(open(path, 'rb'))
        else:
            lat1 = self.gridobj.P_ALP
            lat2 = self.gridobj.P_BET
            lon1 = self.gridobj.P_GAM
            lon0 = self.gridobj.XCENT
            lat0 = self.gridobj.YCENT
            lat = self.gridobj.variables['LAT'][:][0, 0, :, :].squeeze()
            lon = self.gridobj.variables['LON'][:][0, 0, :, :].squeeze()
            m = Basemap(projection='laea', resolution='h', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=lat[0, 0],
                        urcrnrlat=lat[-1, -1], llcrnrlon=lon[0, 0], urcrnrlon=lon[-1, -1], rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_pacific_coast_basemap(self, path):
        def load_conus_basemap(self, path):

            pass

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
            lat = self.gridobj.variables['LAT'][:][0, 0, :, :].squeeze()
            lon = self.gridobj.variables['LON'][:][0, 0, :, :].squeeze()
            m = Basemap(projection='laea', resolution='l', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=29., urcrnrlat=53., llcrnrlon=-125., urcrnrlon=-116., rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_rockies_basemap(self, path):
        from six.moves import cPickle as pickle
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
            lat = self.gridobj.variables['LAT'][:][0, 0, :, :].squeeze()
            lon = self.gridobj.variables['LON'][:][0, 0, :, :].squeeze()
            m = Basemap(projection='laea', resolution='i', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=29., urcrnrlat=52., llcrnrlon=-116., urcrnrlon=-91., rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_south_central_basemap(self, path):
        from six.moves import cPickle as pickle
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
            lat = self.gridobj.variables['LAT'][:][0, 0, :, :].squeeze()
            lon = self.gridobj.variables['LON'][:][0, 0, :, :].squeeze()
            m = Basemap(projection='laea', resolution='i', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=25., urcrnrlat=37.5, llcrnrlon=-108, urcrnrlon=-86., rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_northeast_basemap(self, path):
        from six.moves import cPickle as pickle
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
            lat = self.gridobj.variables['LAT'][:][0, 0, :, :].squeeze()
            lon = self.gridobj.variables['LON'][:][0, 0, :, :].squeeze()
            m = Basemap(projection='laea', resolution='l', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=37., urcrnrlat=46.5, llcrnrlon=-83.5, urcrnrlon=-61.5, rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_north_central_basemap(self, path):
        from six.moves import cPickle as pickle
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
            lat = self.gridobj.variables['LAT'][:][0, 0, :, :].squeeze()
            lon = self.gridobj.variables['LON'][:][0, 0, :, :].squeeze()
            m = Basemap(projection='laea', resolution='l', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
                        llcrnrlat=37., urcrnrlat=46.5, llcrnrlon=-83.5, urcrnrlon=-61.5, rsphere=6371200.,
                        area_thresh=50.)
        return m

    def load_southeast_basemap(self, path):
        from six.moves import cPickle as pickle
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
            lat = self.gridobj.variables['LAT'][:][0, 0, :, :].squeeze()
            lon = self.gridobj.variables['LON'][:][0, 0, :, :].squeeze()
            m = Basemap(projection='laea', resolution='l', lat_1=lat1, lat_2=lat2, lat_0=lat0, lon_0=lon0, lon_1=lon1,
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
