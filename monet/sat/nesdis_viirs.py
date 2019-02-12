import inspect
import os
import xarray as xr


server = 'ftp.star.nesdis.noaa.gov'
base_dir = '/pub/smcd/jhuang/npp.viirs.aerosol.data/edraot550/'


def open_dataset(date, resolution='high', datapath='.'):
    current = change_dir(datapath)
    # check resolution; by default 0.1 degree data is assumed
    if resolution is 'high' or resolution is 'h':
        # this is the 0.1 degree data
        nlat = 1800
        nlon = 3600
        lon, lat = _get_latlons(nlat, nlon)
        fname, date = download_data(date, resolution='high')
    else:
        nlat = 720
        nlon = 1440
        lon, lat = _get_latlons(nlat, nlon)
        fname, date = download_data(date, resolution=0.25)
    # unzip fname
    fname = _unzip_file(fname)
    # read the data
    data = read_data(fname, lat, lon, date)
    return data

def open_mfdataset(dates, resolution='high',datapath='.'):
    from xarray import concat
    das = []
    for i in dates:
        das.append(open_dataset(i,resolution=resolution,datapath=datapath))
    ds = concat(das,dim='time')
    return ds


def read_data(fname, lat, lon, date):
    from numpy import fromfile, float32, nan
    from pandas import to_datetime
    f = fromfile(fname, dtype=float32)
    nlat, nlon = lon.shape
    aot = f.reshape(2, nlat, nlon)[0, :, :].reshape(1, nlat, nlon)
    aot[aot < -999] = nan
    datearr = to_datetime([date])
    da = xr.DataArray(aot, coords=[datearr, range(
        nlat), range(nlon)], dims=['time', 'y', 'x'])
    da['latitude'] = (('y', 'x'), lat)
    da['longitude'] = (('y', 'x'), lon)
    da.attrs['units'] = ''
    da.name = 'VIIRS EDR AOD'
    da.attrs['long_name'] = 'Aerosol Optical Depth'
    da.attrs['source'] = 'ftp://ftp.star.nesdis.noaa.gov/pub/smcd/jhuang/npp.viirs.aerosol.data/edraot550'
    return da


def _unzip_file(fname):
    import subprocess
    subprocess.run(['gunzip', '-f', fname])
    return fname[:-3]


def change_dir(to_path):
    current = os.getcwd()
    os.chdir(to_path)
    return current


def download_data(date, resolution='high'):
    import ftplib
    from datetime import datetime
    if isinstance(date, datetime):
        year = date.strftime('%Y')
        yyyymmdd = date.strftime('%Y%m%d')
    else:
        from pandas import Timestamp
        date = Timestamp(date)
        year = date.strftime('%Y')
        yyyymmdd = date.strftime('%Y%m%d')
    if resolution is 'high':
        file = 'npp_aot550_edr_gridded_0.10_{}.high.bin.gz'.format(yyyymmdd)
    else:
        file = 'npp_aot550_edr_gridded_0.25_{}.high.bin.gz'.format(yyyymmdd)
    ftp = ftplib.FTP(server)
    ftp.login()
    ftp.cwd(base_dir + year)
    ftp.retrbinary("RETR " + file, open(file, 'wb').write)
    return file, date


def _get_latlons(nlat, nlon):
    from numpy import linspace, meshgrid
    lon_min = -179.875
    lon_max = -1 * lon_min
    lat_min = -89.875
    lat_max = -1. * lat_min
    lons = linspace(lon_min, lon_max, nlon)
    lats = linspace(lat_min, lat_max, nlat)
    lon, lat = meshgrid(lons, lats)
    return lon, lat
