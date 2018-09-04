""" CAMx File Reader """
from numpy import array, concatenate
from pandas import Series, to_datetime
import xarray as xr
from ..grids import grid_from_dataset, get_ioapi_pyresample_area_def


def can_do(index):
    if index.max():
        return True
    else:
        return False


def open_files(fname, earth_radius=6370000):
    """Short summary.

    Parameters
    ----------
    fname : type
        Description of parameter `fname`.
    earth_radius : type
        Description of parameter `earth_radius`.

    Returns
    -------
    type
        Description of returned object.

    """
    # open the dataset using xarray
    dset = xr.open_mfdataset(
        fname, engine='pseudonetcdf', backend_kwargs={'format': 'uamiv'})

    # get the grid information
    grid = grid_from_dataset(dset, earth_radius=earth_radius)
    area_def = get_ioapi_pyresample_area_def(dset, grid)
    # assign attributes for dataset and all DataArrays
    dset = dset.assign_attrs({'proj4_srs': grid})
    for i in dset.variables:
        dset[i] = dset[i].assign_attrs({'proj4_srs': grid})
        for j in dset[i].attrs:
            dset[i].attrs[j] = dset[i].attrs[j].strip()
        dset[i] = dset[i].assign_attrs({'area': area_def})
    dset = dset.assign_attrs(area=area_def)

    # add lazy diagnostic variables
    dset = add_lazy_pm25(dset)
    dset = add_lazy_pm10(dset)
    dset = add_lazy_pm_course(dset)
    dset = add_lazy_noy(dset)
    dset = add_lazy_nox(dset)

    # get the times
    dset = _get_times(dset)

    # get the lat lon
    dset = _get_latlon(dset)

    # get Predefined mapping tables for observations
    dset = _predefined_mapping_tables(dset)

    # rename dimensions
    dset = dset.rename({'COL': 'x', 'ROW': 'y', 'LAY': 'z'})

    return dset


def _get_times(d):
    idims = len(d.TFLAG.dims)
    if idims == 2:
        tflag1 = Series(d['TFLAG'][:, 0]).astype(str).str.zfill(7)
        tflag2 = Series(d['TFLAG'][:, 1]).astype(str).str.zfill(6)
    else:
        tflag1 = Series(d['TFLAG'][:, 0, 0]).astype(str).str.zfill(7)
        tflag2 = Series(d['TFLAG'][:, 0, 1]).astype(str).str.zfill(6)
    date = to_datetime(
        [i + j for i, j in zip(tflag1, tflag2)], format='%Y%j%H%M%S')
    indexdates = Series(date).drop_duplicates(keep='last').index.values
    d = d.isel(TSTEP=indexdates)
    d['TSTEP'] = date[indexdates]
    return d.rename({'TSTEP': 'time'})


def _get_latlon(dset):
    """gets the lat and lons from the pyreample.geometry.AreaDefinition

    Parameters
    ----------
    dset : xarray.Dataset
        Description of parameter `dset`.

    Returns
    -------
    xarray.Dataset
        Description of returned object.

    """
    lon, lat = dset.area.get_lonlats()
    dset['longitude'] = xr.DataArray(lon[::-1, :], dims=['ROW', 'COL'])
    dset['latitude'] = xr.DataArray(lat[::-1, :], dims=['ROW', 'COL'])
    dset = dset.assign_coords(longitude=dset.longitude, latitude=dset.latitude)
    return dset


def add_lazy_pm25(d):
    """Short summary.

    Parameters
    ----------
    d : type
        Description of parameter `d`.

    Returns
    -------
    type
        Description of returned object.

    """
    keys = Series([i for i in d.variables])
    allvars = Series(fine)
    if 'PM25_TOT' in keys:
        d['PM25'] = d['PM25_TOT'].chunk()
    else:
        index = allvars.isin(keys)
        newkeys = allvars.loc[index]
        d['PM25'] = add_multiple_lazy(d, newkeys)
        d['PM25'].assign_attrs({'name': 'PM2.5', 'long_name': 'PM2.5'})
    return d


def add_lazy_pm10(d):
    keys = Series([i for i in d.variables])
    allvars = Series(concatenate([fine, coarse]))
    if 'PM_TOT' in keys:
        d['PM10'] = d['PM_TOT'].chunk()
    else:
        index = allvars.isin(keys)
        if can_do(index):
            newkeys = allvars.loc[index]
            d['PM10'] = add_multiple_lazy(d, newkeys)
            d['PM10'] = d['PM10'].assign_attrs({
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
        d['PM_COURSE'] = d['PM_COURSE'].assign_attrs({
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
        d['CLf'] = d['CLf'].assign_attrs({
            'name':
            'CLf',
            'long_name':
            'Fine Mode particulate Cl'
        })
    return d


def add_lazy_noy(d):
    keys = Series([i for i in d.variables])
    allvars = Series(noy_gas)
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        d['NOy'] = add_multiple_lazy(d, newkeys)
        d['NOy'] = d['NOy'].assign_attrs({'name': 'NOy', 'long_name': 'NOy'})
    return d


def add_lazy_nox(d):
    keys = Series([i for i in d.variables])
    allvars = Series(['NO', 'NOX'])
    index = allvars.isin(keys)
    if can_do(index):
        newkeys = allvars.loc[index]
        d['NOx'] = add_multiple_lazy(d, newkeys)
        d['NOx'] = d['NOx'].assign_attrs({'name': 'NOx', 'long_name': 'NOx'})
    return d


def add_multiple_lazy(dset, variables, weights=None):
    from numpy import ones
    if weights is None:
        weights = ones(len(variables))
    new = dset[variables[0]].copy() * weights[0]
    for i, j in zip(variables[1:], weights[1:]):
        new = new + dset[i].chunk() * j
    return new


def _predefined_mapping_tables(dset):
    """Predefined mapping tables for different observational parings used when
        combining data.

    Returns
    -------
    dictionary
        A dictionary of to map to.

    """
    to_improve = {}
    to_nadp = {}
    to_aqs = {
        'OZONE': ['O3'],
        'PM2.5': ['PM25'],
        'CO': ['CO'],
        'NOY': [
            'NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA',
            'NTR', 'CRON', 'CRN2', 'CRNO', 'CRPX', 'OPAN'
        ],
        'NOX': ['NO', 'NO2'],
        'SO2': ['SO2'],
        'NO': ['NO'],
        'NO2': ['NO2'],
        'SO4f': ['PSO4'],
        'PM10': ['PM10'],
        'NO3f': ['PNO3'],
        'ECf': ['PEC'],
        'OCf': ['OC'],
        'ETHANE': ['ETHA'],
        'BENZENE': ['BENZENE'],
        'TOLUENE': ['TOL'],
        'ISOPRENE': ['ISOP'],
        'O-XYLENE': ['XYL'],
        'WS': ['WSPD10'],
        'TEMP': ['TEMP2'],
        'WD': ['WDIR10'],
        'NAf': ['NA'],
        'NH4f': ['PNH4']
    }
    to_airnow = {
        'OZONE': ['O3'],
        'PM2.5': ['PM25'],
        'CO': ['CO'],
        'NOY': [
            'NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA',
            'NTR', 'CRON', 'CRN2', 'CRNO', 'CRPX', 'OPAN'
        ],
        'NOX': ['NO', 'NO2'],
        'SO2': ['SO2'],
        'NO': ['NO'],
        'NO2': ['NO2'],
        'SO4f': ['PSO4'],
        'PM10': ['PM10'],
        'NO3f': ['PNO3'],
        'ECf': ['PEC'],
        'OCf': ['OC'],
        'ETHANE': ['ETHA'],
        'BENZENE': ['BENZENE'],
        'TOLUENE': ['TOL'],
        'ISOPRENE': ['ISOP'],
        'O-XYLENE': ['XYL'],
        'WS': ['WSPD10'],
        'TEMP': ['TEMP2'],
        'WD': ['WDIR10'],
        'NAf': ['NA'],
        'NH4f': ['PNH4']
    }
    to_crn = {}
    to_aeronet = {}
    to_cems = {}
    mapping_tables = {
        'improve': to_improve,
        'aqs': to_aqs,
        'airnow': to_airnow,
        'crn': to_crn,
        'cems': to_cems,
        'nadp': to_nadp,
        'aeronet': to_aeronet
    }
    dset = dset.assign_attrs({'mapping_tables': mapping_tables})
    return dset


# Arrays for different gasses and pm groupings
coarse = array(['CPRM', 'CCRS'])
fine = array([
    'NA', 'PSO4', 'PNO3', 'PNH4', 'PH2O', 'PCL', 'PEC', 'FPRM', 'FCRS', 'SOA1',
    'SOA2', 'SOA3', 'SOA4'
])
noy_gas = array([
    'NO', 'NO2', 'NO3', 'N2O5', 'HONO', 'HNO3', 'PAN', 'PANX', 'PNA', 'NTR',
    'CRON', 'CRN2', 'CRNO', 'CRPX', 'OPAN'
])
poc = array(['SOA1', 'SOA2', 'SOA3', 'SOA4'])
