import inspect
import os
import xarray as xr


server = 'ftp.star.nesdis.noaa.gov'
base_dir = '/pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550/'


def open_dataset(date, datapath='.'):
    """Short summary.

    Parameters
    ----------
    date : type
        Description of parameter `date`.
    datapath : type
        Description of parameter `datapath`.

    Returns
    -------
    type
        Description of returned object.

    """
    current = change_dir(datapath)
    nlat = 720
    nlon = 1440
    lon, lat = _get_latlons(nlat, nlon)
    fname = download_data(date)
    data = read_data(fname, lat, lon, date)
    change_dir(current)
    return data


def open_mfdataset(dates, datapath='.'):
    """Short summary.

    Parameters
    ----------
    dates : type
        Description of parameter `dates`.
    datapath : type
        Description of parameter `datapath`.

    Returns
    -------
    type
        Description of returned object.

    """
    from xarray import concat
    das = []
    for i in dates:
        das.append(open_dataset(i, resolution=resolution, datapath=datapath))
    ds = concat(das, dim='time')
    return ds


def read_data(fname, lat, lon, date):
    """Short summary.

    Parameters
    ----------
    fname : type
        Description of parameter `fname`.
    lat : type
        Description of parameter `lat`.
    lon : type
        Description of parameter `lon`.
    date : type
        Description of parameter `date`.

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import nan
    from pandas import to_datetime
    da = xr.open_dataset(fname)
    datearr = to_datetime([date])
    da = f['aot_ip_out']
    da = da.rename({'nlat': 'y', 'nlon': 'x'})
    da['latitude'] = (('y', 'x'), lat)
    da['longitude'] = (('y', 'x'), lon)
    da['time'] = datearr
    da.attrs['units'] = ''
    da.name = 'VIIRS EPS AOT'
    da.attrs['long_name'] = 'Aerosol Optical Thickness'
    da.attrs['source'] = 'ftp://ftp.star.nesdis.noaa.gov/pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550'
    return da


def change_dir(to_path):
    """Short summary.

    Parameters
    ----------
    to_path : type
        Description of parameter `to_path`.

    Returns
    -------
    type
        Description of returned object.

    """
    current = os.getcwd()
    os.chdir(to_path)
    return current


def download_data(date, resolution='high'):
    """Short summary.

    Parameters
    ----------
    date : type
        Description of parameter `date`.
    resolution : type
        Description of parameter `resolution`.

    Returns
    -------
    type
        Description of returned object.

    """
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
    file = 'npp_eaot_ip_gridded_0.25_{}.high.nc'.format(yyyymmdd)
    ftp = ftplib.FTP(server)
    ftp.login()
    ftp.cwd(base_dir + year)
    ftp.retrbinary("RETR " + file, open(file, 'wb').write)
    return file, date


def _get_latlons(nlat, nlon):
    """Short summary.

    Parameters
    ----------
    nlat : type
        Description of parameter `nlat`.
    nlon : type
        Description of parameter `nlon`.

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import linspace, meshgrid
    lon_min = -179.875
    lon_max = -1 * lon_min
    lat_min = -89.875
    lat_max = -1. * lat_min
    lons = linspace(lon_min, lon_max, nlon)
    lats = linspace(lat_max, lat_min, nlat)
    lon, lat = meshgrid(lons, lats)
    return lon, lat
