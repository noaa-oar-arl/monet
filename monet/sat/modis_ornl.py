#!/usr/bin/env python
"""
Get MODIS data using the ORNL DAAC MODIS web service.
https://modis.ornl.gov/data/modis_webservice.html

originally created by Tristan Quaife

Adapted to MONET by Barry Baker
"""

__author__ = "Barry Baker"
__version__ = "0.4 (29.07.2010)"
__email__ = "barry.baker@noaa.gov"
# Modified P. Campbell 2019-04-03

# import optparse
# import os
import pickle
import sys
from copy import copy

import numpy as np
from dask.diagnostics import ProgressBar

try:
    from suds.client import *
    has_suds = True
except ImportError:
    has_suds = False
DEBUG_PRINTING = False

defaultURL = 'https://modis.ornl.gov/cgi-bin/MODIS/soapservice/MODIS_soapservice.wsdl'

pbar = ProgressBar()
pbar.register()


class modisData(object):
    def __init__(self):

        self.server = None
        self.product = None
        self.latitude = None
        self.longitude = None

        self.band = None
        self.nrows = None
        self.ncols = None
        self.cellsize = None
        self.scale = None
        self.units = None
        self.yllcorner = None
        self.xllcorner = None

        self.kmAboveBelow = 0
        self.kmLeftRight = 0

        self.dateStr = []
        self.dateInt = []
        self.data = []
        self.QA = []

        # self.header=None
        # self.subset=None

        self.isScaled = False

    def getFilename(self):

        d = '.'

        fn = self.product
        fn = fn + d + self.band
        fn = fn + d + 'LAT__' + str(self.latitude)
        fn = fn + d + 'LON__' + str(self.longitude)
        fn = fn + d + self.dateStr[0]
        fn = fn + d + self.dateStr[-1]
        fn = fn + d + str(int(self.nrows))
        fn = fn + d + str(int(self.ncols))

        return fn

    def pickle(self):

        fn = self.getFilename() + '.' + 'pkl'

        f = open(fn, 'w')
        pickle.dump(self, f)
        f.close()

    def applyScale(self):

        if self.isScaled is False:
            self.data = self.data * self.scale
            self.isScaled = True

    def filterQA(self, QAOK, fill=np.nan):

        if np.size(self.data) != np.size(self.QA):
            # should do this using an exception
            print >> sys.stderr, 'data and QA are different sizes'
            sys.exit()

        r = np.shape(self.data)[0]
        c = np.shape(self.data)[1]

        for i in range(c):
            for j in range(r):
                if np.sum(QAOK == self.QA[j][i]) == 0:
                    self.data[j][i] = fill


def __getDummyDateList():
    """
        Generate a dummy date list for testing without
        hitting the server
        """

    D = []
    for y in range(2001, 2010):
        for d in range(1, 365, 1):
            D.append('A%04d%03d' % (y, d))

    return D


def __error(msg):
    raise Exception(msg)


def latLonErr():
    __error('Latitude and longitude must both be specified')


def serverDataErr():
    __error('Server not returning data (possibly busy)')


def mkIntDate(s):
    """
        Convert the webserver formatted dates
        to an integer format by stripping the
        leading char and casting
        """
    n = s.__len__()
    d = int(s[-(n - 1):n])

    return d


def setClient(wsdlurl=defaultURL):

    return Client(wsdlurl)


def printList(l):

    for i in range(l.__len__()):
        print(l[i])


def printModisData(m):

    print('server:', m.server)
    print('product:', m.product)
    print('latitude:', m.latitude)
    print('longitude:', m.longitude)

    print('band:', m.band)
    print('nrows:', m.nrows)
    print('ncols:', m.ncols)
    print('cellsize:', m.cellsize)
    print('scale:', m.scale)
    print('units:', m.units)
    print('xllcorner:', m.yllcorner)
    print('yllcorner:', m.xllcorner)

    print('kmAboveBelow:', m.kmAboveBelow)
    print('kmLeftRight:', m.kmLeftRight)

    print('dates:', m.dateStr)

    print('QA:', m.QA)
    print(m.data)


def __debugPrint(o):

    if DEBUG_PRINTING:
        print >> sys.stderr, 'DB> ', o
        sys.stderr.flush


def modisGetQA(m,
               QAname,
               client=None,
               chunkSize=8,
               kmAboveBelow=0,
               kmLeftRight=0):

    startDate = m.dateInt[0]
    endDate = m.dateInt[-1]

    q = modisClient(
        client,
        product=m.product,
        band=QAname,
        lat=m.latitude,
        lon=m.longitude,
        startDate=startDate,
        endDate=endDate,
        chunkSize=chunkSize,
        kmAboveBelow=kmAboveBelow,
        kmLeftRight=kmLeftRight)

    m.QA = copy(q.data)


def modisClient(client=None,
                product=None,
                band=None,
                lat=None,
                lon=None,
                startDate=None,
                endDate=None,
                chunkSize=8,
                kmAboveBelow=0,
                kmLeftRight=0):
    """
        modisClient: function for building a modisData object
        """

    m = modisData()

    m.kmABoveBelow = kmAboveBelow
    m.kmLeftRight = kmLeftRight

    if client is None:
        client = setClient()

    m.server = client.wsdl.url

    if product is None:
        prodList = client.service.getproducts()
        return prodList

    m.product = product

    if band is None:
        bandList = client.service.getbands(product)
        return bandList

    m.band = band

    if lat is None or lon is None:
        latLonErr()

    m.latitude = lat
    m.longitude = lon

    # get the date list regardless so we can
    # process it into appropriately sized chunks

    dateList = client.service.getdates(lat, lon, product)

    if startDate is None or endDate is None:
        return dateList

    # count up the total number of dates
    i = -1
    nDates = 0
    while i < dateList.__len__() - 1:
        i = i + 1

        # __debugPrint( 'i=%d'%i )

        thisDate = mkIntDate(dateList[i])

        if thisDate < startDate:
            continue
        if thisDate > endDate:
            break

        nDates = nDates + 1

        m.dateInt.append(thisDate)
        m.dateStr.append(dateList[i])

    __debugPrint(m.dateStr)

    n = 0
    i = -1
    while i < dateList.__len__() - 1:
        i = i + 1

        thisDate = mkIntDate(dateList[i])

        if thisDate < startDate:
            continue
        if thisDate > endDate:
            break

        requestStart = dateList[i]

        j = min(chunkSize, dateList.__len__() - i)

        __debugPrint(
            'i=%d, j=%d, dateList__len__()=%d' % (i, j, dateList.__len__()))
        while mkIntDate(dateList[i + j - 1]) > endDate:
            j = j - 1

        requestEnd = dateList[i + j - 1]
        i = i + j - 1

        # print >> sys.stderr, requestStart, requestEnd

        data = client.service.getsubset(lat, lon, product, band, requestStart,
                                        requestEnd, kmAboveBelow, kmLeftRight)

        # print(data)
        # now fill up the data structure with the returned data...

        if n == 0:

            m.nrows = int(data.nrows)
            m.ncols = int(data.ncols)
            m.cellsize = data.cellsize
            m.scale = data.scale
            m.units = data.units
            m.yllcorner = data.yllcorner
            m.xllcorner = data.xllcorner

            m.data = np.zeros((nDates, m.nrows * m.ncols))

        for j in range(data.subset.__len__()):
            kn = 0
            __debugPrint(data.subset)
            for k in data.subset[j].split(",")[5:]:
                __debugPrint(k)
                try:
                    m.data[n * chunkSize + j, kn] = int(k)
                except ValueError:
                    serverDataErr()

                kn = kn + 1

        n = n + 1

    return (m)


def _nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))


def get_available_products():
    m = modisData()
    client = setClient()
    m.server = client.wsdl.url
    print(client.service.getproducts())
    # print(prodList)


def get_available_bands(product='MOD12A2H'):
    client = setClient()
    print(client.service.getbands(product))


def _get_single_retrieval(date,
                          product='MOD12A2H',
                          band='Lai_500m',
                          quality_control=None,
                          lat=0,
                          lon=0,
                          kmAboveBelow=100,
                          kmLeftRight=100):
    import pandas as pd
    client = setClient()
    # prodList = modisClient(client)
    # bandList = modisClient(client, product='MOD15A2H')
    dateList = modisClient(
        client, product=product, band=band, lat=lat, lon=lon)
    dates = pd.to_datetime(dateList, format='A%Y%j')
    date = pd.to_datetime(date)
    if isinstance(date, pd.Timestamp):
        dates = _nearest(dates, date)
        m = modisClient(
            client,
            product=product,
            band=band,
            lat=lat,
            lon=lon,
            startDate=int(dates.strftime('%Y%j')),
            endDate=int((dates + pd.Timedelta(1, units='D')).strftime('%Y%j')),
            kmAboveBelow=kmAboveBelow,
            kmLeftRight=kmLeftRight)
    else:
        m = modisClient(
            client,
            product=product,
            band=band,
            lat=lat,
            lon=lon,
            startDate=int(dates.min().strftime('%Y%j')),
            endDate=int(date.max().strftime('%Y%j')),
            kmAboveBelow=kmAboveBelow,
            kmLeftRight=kmLeftRight)
    if quality_control is not None:
        modisGetQA(
            m,
            quality_control,
            client=client,
            kmAboveBelow=kmAboveBelow,
            kmLeftRight=kmLeftRight)

    m.applyScale()
    return m


def _fix_array(m):
    return m.data.reshape(m.ncols, m.nrows, order='C')[::-1, :]


def _make_xarray_dataarray(m):
    """Takes the modis object and creates an xarray DataArray.

    Parameters
    ----------
    m : type
        Description of parameter `m`.

    Returns
    -------
    type
        Description of returned object.

    """
    import xarray as xr
    from pandas import to_datetime
    da = xr.DataArray(
        m.data.reshape(m.ncols, m.nrows, order='C')[::-1, :], dims=('x', 'y'))
    da.attrs['long_name'] = m.band
    da.attrs['product'] = m.product
    da.attrs['cellsize'] = m.cellsize
    da.attrs['units'] = m.units
    da.attrs['server'] = m.server
    lon, lat = _get_latlon(m.xllcorner, m.yllcorner, m.cellsize, m.ncols,
                           m.nrows)
    da.name = m.band
    da['time'] = to_datetime(str(m.dateInt[0]), format='%Y%j')
    # print(da)
    # print(lon.shape)
    da.coords['longitude'] = (('x', 'y'), lon)
    da.coords['latitude'] = (('x', 'y'), lat)
    return da


def _get_latlon(xll, yll, cell_width, nx, ny):
    """get the latitude and longitude from a sinusoidal projection.

    Parameters
    ----------
    xll : float
        Description of parameter `xll`.
    yll : float
        Description of parameter `yll`.
    cell_width : float
        Description of parameter `cell_width`.
    nx : int
        Description of parameter `nx`.
    ny : int
        Description of parameter `ny`.

    Returns
    -------
    (float,float)
        returns the 2d arrays of lon, lat

    """
    from pyproj import Proj
    from numpy import linspace, meshgrid
    sinu = Proj(
        '+proj=sinu +a=6371007.181 +b=6371007.181 +units=m +R=6371007.181')
    x = linspace(xll, xll + cell_width * nx, nx)
    y = linspace(yll, yll + cell_width * ny, ny)
    xx, yy = meshgrid(x, y)
    lon, lat = sinu(xx, yy, inverse=True)
    return lon, lat


def open_dataset(date,
                 product='MOD12A2H',
                 band='Lai_500m',
                 quality_control=None,
                 latitude=0,
                 longitude=0,
                 kmAboveBelow=100,
                 kmLeftRight=100):
    import pandas as pd
    try:
        if has_suds is False:
            raise ImportError
    except ImportError:
        print('Please install a suds client')
    date = pd.to_datetime(date)
    m = _get_single_retrieval(
        date,
        product=product,
        band=band,
        quality_control=quality_control,
        lat=latitude,
        lon=longitude,
        kmAboveBelow=kmAboveBelow,
        kmLeftRight=kmLeftRight)
    da = _make_xarray_dataarray(m)
    return da


def open_mfdataset(dates,
                   product='MOD12A2H',
                   band='Lai_500m',
                   quality_control=None,
                   latitude=0,
                   longitude=0,
                   kmAboveBelow=100,
                   kmLeftRight=100):
    import pandas as pd
    import xarray as xr
    import dask
    dates = pd.to_datetime(dates)
    od = dask.delayed(open_dataset)
    das = dask.delayed([
        od(i,
           product=product,
           band=band,
           quality_control=quality_control,
           latitude=latitude,
           longitude=longitude,
           kmAboveBelow=kmAboveBelow,
           kmLeftRight=kmLeftRight) for i in dates
    ])
    da = xr.concat(das.compute(), dim='time')
    da['time'] = dates
    return da
