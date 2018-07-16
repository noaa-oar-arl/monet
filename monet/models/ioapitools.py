from numpy import array, concatenate
from pandas import Series, to_datetime


def can_do(index):
    if index.max():
        return True
    else:
        return False


def get_times(d):
    idims = len(d.TFLAG.dims)
    if idims == 2:
        tflag1 = Series(d['TFLAG'][:, 0]).astype(str).str.zfill(7)
        tflag2 = Series(d['TFLAG'][:, 1]).astype(str).str.zfill(6)
    else:
        tflag1 = Series(d['TFLAG'][:, :, 0]).astype(str).str.zfill(7)
        tflag2 = Series(d['TFLAG'][:, 0, 1]).astype(str).str.zfill(6)
    date = to_datetime(
        [i + j for i, j in zip(tflag1, tflag2)], format='%Y%j%H%M%S')
    indexdates = Series(date).drop_duplicates(keep='last').index.values
    d = d.isel(time=indexdates)
    d['time'] = date[indexdates]


def add_lazy_pm25(d):
    keys = Series([i for i in d.variables])
    allvars = Series(concatenate([aitken, accumulation, coarse]))
    weights = Series([
        1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
        1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
        1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
        1., 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2
    ])
    if 'PM25_TOT' in keys:
        d['PM25'] = d['PM25_TOT'].chunks()
    else:
        index = allvars.isin(keys)
        newkeys = allvars.loc[index]
        newweights = weights.loc[index]
        d['PM25'] = add_multiple_lazy(d, newkeys, weights=newweights)
        d['PM25'].assign_attrs({'name': 'PM2.5', 'long_name': 'PM2.5'})
    return d


def add_lazy_pm10(d):
    keys = Series([i for i in d.variables])
    allvars = Series(concatenate([aitken, accumulation, coarse]))
    if 'PM_TOT' in keys:
        d['PM10'] = d['PM_TOT'].chunks()
    else:
        index = allvars.isin(keys)
        if can_do(index):
            newkeys = allvars.loc[index]
            d['PM10'] = add_multiple_lazy(d, newkeys)
            d['PM10'] = add_multiple_lazy(d, newkeys)
            d['PM10'].assign_attrs({
                'name':
                'PM10',
                'long_name':
                'Particulate Matter < 10 microns'
            })
    return d


def add_lazy_pm_course(d):
    keys = Series([i for i in d.variables])
    allvars = Series(coarse)
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        d['PM_COURSE'] = add_multiple_lazy(d, newkeys)
        d['PM_COURSE'].assign_attrs({
            'name':
            'PM_COURSE',
            'long_name':
            'Course Mode Particulate Matter'
        })
    return d


def add_lazy_clf(d):
    keys = Series([i for i in d.variables])
    allvars = Series(['ACLI', 'ACLJ', 'ACLK'])
    weights = Series([1, 1, .2])
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        neww = weights.loc[index]
        d['CLf'] = add_multiple_lazy(d, newkeys, weights=neww)
        d['CLf'].attrs = d[newkeys[0]].attrs
    return d


def add_lazy_naf(d):
    keys = Series([i for i in d.variables])
    allvars = Series(['ANAI', 'ANAJ', 'ASEACAT', 'ASOIL', 'ACORS'])
    weights = Series(
        [1, 1, .2 * 837.3 / 1000., .2 * 62.6 / 1000., .2 * 2.3 / 1000.])
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        neww = weights.loc[index]
        d['NAf'] = add_multiple_lazy(d, newkeys, weights=neww)
        d['NAf'].assign_attrs({'name': 'NAf', 'long_name': 'NAf'})
    return d


def add_multiple_lazy(dset, variables, weights=None):
    from numpy import ones
    if weights is None:
        weights = ones(len(variables))
    new = dset[variables[0]].copy() * weights[0]
    for i, j in zip(variables[1:], weights[1:]):
        new = new + dset[i].chunk() * j
    return new


accumulation = array([
    'AALJ', 'AALK1J', 'AALK2J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J', 'ACAJ', 'ACLJ',
    'AECJ', 'AFEJ', 'AISO1J', 'AISO2J', 'AISO3J', 'AKJ', 'AMGJ', 'AMNJ',
    'ANAJ', 'ANH4J', 'ANO3J', 'AOLGAJ', 'AOLGBJ', 'AORGCJ', 'AOTHRJ', 'APAH1J',
    'APAH2J', 'APAH3J', 'APNCOMJ', 'APOCJ', 'ASIJ', 'ASO4J', 'ASQTJ', 'ATIJ',
    'ATOL1J', 'ATOL2J', 'ATOL3J', 'ATRP1J', 'ATRP2J', 'AXYL1J', 'AXYL2J',
    'AXYL3J', 'AORGAJ', 'AORGPAJ', 'AORGBJ'
])
aitken = array([
    'ACLI', 'AECI', 'ANAI', 'ANH4I', 'ANO3I', 'AOTHRI', 'APNCOMI', 'APOCI',
    'ASO4I', 'AORGAI', 'AORGPAI', 'AORGBI'
])
coarse = array(
    ['ACLK', 'ACORS', 'ANH4K', 'ANO3K', 'ASEACAT', 'ASO4K', 'ASOIL'])
noy_gas = array([
    'NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA', 'NTR',
    'CRON', 'CRN2', 'CRNO', 'CRPX', 'OPAN'
])
pec = array(['AECI', 'AECJ'])
pso4 = array(['ASO4I', 'ASO4J'])
pno3 = array(['ANO3I', 'ANO3J'])
pnh4 = array(['ANH4I', 'ANH4J'])
pcl = array(['ACLI', 'ACLJ'])
poc = array([
    'AOTHRI', 'APNCOMI', 'APOCI', 'AORGAI', 'AORGPAI', 'AORGBI', 'ATOL1J',
    'ATOL2J', 'ATOL3J', 'ATRP1J', 'ATRP2J', 'AXYL1J', 'AXYL2J', 'AXYL3J',
    'AORGAJ', 'AORGPAJ', 'AORGBJ', 'AOLGAJ', 'AOLGBJ', 'AORGCJ', 'AOTHRJ',
    'APAH1J', 'APAH2J', 'APAH3J', 'APNCOMJ', 'APOCJ', 'ASQTJ', 'AISO1J',
    'AISO2J', 'AISO3J', 'AALK1J', 'AALK2J', 'ABNZ1J', 'ABNZ2J', 'ABNZ3J',
    'AORGAI', 'AORGAJ', 'AORGPAI', 'AORGPAJ', 'AORGBI', 'AORGBJ'
])
minerals = array(
    ['AALJ', 'ACAJ', 'AFEJ', 'AKJ', 'AMGJ', 'AMNJ', 'ANAJ', 'ATIJ', 'ASIJ'])
