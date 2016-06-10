# This file is to deal with CMAQ code - try to make it general for cmaq 4.7.1 --> 5.1
from netCDF4 import Dataset, MFDataset
from numpy import array, zeros
from tools import search_listinlist
from gc import collect

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
        self.cdfobj = None
        self.gridobj = None
        self.fname = None
        self.dates = None

    def get_single_var(self, param):
        return self.cdfobj.variables[param][:]

    def get_cmaq_dates(self):
        from datetime import datetime
        tflag1 = array(self.cdfobj.variables['TFLAG'][:, 0, 0], dtype='|S7')
        tflag2 = array(self.cdfobj.variables['TFLAG'][:, 1, 1] / 10000, dtype='|S6')
        date = []
        for i, j in zip(tflag1, tflag2):
            date.append(datetime.strptime(i + j, '%Y%j%H'))
        self.dates = array(date)
        return array(date)

    def load_single_cmaq_run(self):
        self.cdfobj = Dataset(self.fname)

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

    def get_surface_dust_total(self):
        keys = array(self.cdfobj.variables.keys())
        cmaqvars, temp = search_listinlist(keys, self.dust_total)
        var = zeros(self.cdfobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
        for i in cmaqvars[1:]:
            print 'Retrieving PM DUST: '+keys[i]
            var += self.cdfobj.variables[keys[i]][:, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_dust_pm25(self):
        keys = array(self.cdfobj.variables.keys())
        cmaqvars, temp = search_listinlist(keys, self.dust_pm25)
        var = zeros(self.cdfobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
        for i in cmaqvars[1:]:
            print 'Retrieving PM25 DUST: '+keys[i]
            var += self.cdfobj.variables[keys[i]][:, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_pm25(self):
        from numpy import concatenate
        keys = array(self.cdfobj.variables.keys())
        vars = concatenate([self.aitken,self.accumulation])
        cmaqvars, temp = search_listinlist(keys, vars)
        var = self.cdfobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze()
        for i in cmaqvars[1:]:
            print 'Retrieving PM25: '+keys[i] 
            var += self.cdfobj.variables[keys[i]][:, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_pm10(self):
        from numpy import concatenate
        keys = array(self.cdfobj.variables.keys())
        vars = concatenate([self.aitken,self.accumulation,self.coarse])
        cmaqvars, temp = search_listinlist(keys, vars)
        var = zeros(self.cdfobj.variables[keys[cmaqvars[0]]][:][:, 0, :, :].squeeze().shape)
        for i in cmaqvars[1:]:
            print 'Retrieving PM10: '+keys[i] 
            var += self.cdfobj.variables[keys[i]][:, 0, :, :].squeeze()
            collect()
        return var

    def get_surface_cmaqvar(self,param='O3'):
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
        else:
            print 'Retrieving: '+ param
            var = self.cdfobj.variables[param][:, 0, :, :].squeeze()
        return var

    def set_gridcro2d(self,filename=''):
        self.gridobj = Dataset(filename)
