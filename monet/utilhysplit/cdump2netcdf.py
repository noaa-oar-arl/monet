import datetime
import os
import sys
import xarray as xr
import numpy as np
from monetio.models import hysplit
#import monet.utilhysplit.hysp_func as hf
from netCDF4 import Dataset
#import matplotlib.pyplot as plt

# 01/28/2020 AMC cdump2awips created to make a netcdf file appropriate for input into AWIPS
# hysplit.py was modified in the forked version of MONET to make this work.

# combine_cdump creates a 6 dimensional xarray dataarray object from cdump files.
#
# TO DO: use the hysp_massload function in hysplit.py rather than the one here.


def thickness_hash(xrash):
    delta = get_thickness(xrash)
    xlevs = xrash.z.values
    dhash = dict(zip(xlevs, delta))
    return dhash


def get_thickness(xrash):
    """
    helper function for mass_loading
    """
    alts = list(xrash.z.values)
    b = [0]
    alts2 = alts
    if alts[0] != 0:
        b.extend(alts)
        alts = b

    a2 = alts[1:]
    a3 = alts[0:-1]
    delta = np.array(a2) - np.array(a3)
    # if deposition layer then first layer has
    # thickness of 0.
    if alts2[0] == 0:
        b = [0]
        b.extend(delta)
        delta = b
    return np.array(delta)


def remove_dep(xrash):
    """
    remove the deposition layer.
    helper function for mass_loading
    """
    vals = xrash.z.values
    if vals[0] == 0:
        vals = vals[1:]
    x2 = xrash.sel(z=vals)
    return x2


def mass_loading(xrash, delta=None):
    """
    # input data array with concentration.
    # return a data-array with mass loading through all the columns
    """
    xrash = remove_dep(xrash)
    if not delta:
        delta = get_thickness(xrash)
    else:
        delta = np.array(delta)
    iii = 1
    if delta[0] == 0:
        iii = 1
        start = 1
    else:
        iii = 0
        start = 0
    for yyy in np.arange(start, len(delta)):
        if iii == start:
            ml = xrash.isel(z=yyy) * delta[yyy]
        else:
            ml2 = xrash.isel(z=yyy) * delta[yyy]
            #import matplotlib.pyplot as plt
            # plt.pcolormesh(ml2)
            # plt.show()
            ml = ml + ml2
        iii += 1
    return ml


def meters2FL(meters):
    flight_level = meters * 3.28084 / 100.0
    return int(np.ceil(flight_level / 10.0) * 10)


def get_topbottom(lev):
    top = 'FL' + str(meters2FL(lev[-1]))
    bottom = 'FL' + str(meters2FL(lev[0]))
    print('level', lev[0], bottom)
    print('level', lev[-1], top)
    return top, bottom


def handle_levels(levlist):
    nlev = len(levlist)
    # divide into three pieces
    piece = int(np.floor(nlev / 3.0))
    jjj = 0
    lev1 = levlist[0:piece]
    lev2 = levlist[piece:2 * piece]
    lev3 = levlist[2 * piece:]
    print(piece, lev1, lev2, lev3)
    return lev1, lev2, lev3


# def cdump2awips(flist, outname, format='NETCDF4', d1=None, d2=None):
def cdump2awips(xrash1, dt, outname, mscale=1, munit='unit', format='NETCDF4', d1=None, d2=None):

    #hxr = hysplit.open_dataset(fname, drange=[d1,d2])
    #xrash1, dt = combine_cdump(flist, d1=d1, d2=d2)
    #nra = xrash.values
    # array with top height of levels in the file.
    #levelra = xrash.z.values

    # mass loading should be in g/m2 to compare to satellite.
    # concentration should be in mg/m3 to compare to threshold levels.

    sample_time = np.timedelta64(int(dt), 'h')

    # stack the ensemble and source dimensions so it is one dimension
    xrash = xrash1.stack(ensemble=('ens', 'source'))
    # put dimensionsin correct order.
    xrash.transpose('time', 'ensemble', 'x', 'y', 'z')
    # array with mass loading rather than concentration.
    mass = mass_loading(xrash)
    levelra = xrash.z.values
    nra = xrash.values

    iii = 0
    for tm in xrash.time.values:
        fid = Dataset(outname + str(iii) + '.nc', 'w', format='NETCDF4')

        # GLOBAL ATTRIBUTES
        fid.SourceFiles = 'Kasatochi'
        fid.time_origin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fid.mass_per_hour = mscale
        fid.mass_unit = munit

        print(xrash.shape)
        print(xrash.coords)
        # DEFINE DIMENSIONS
        lat_shape = xrash.shape[2]
        lon_shape = xrash.shape[3]
        lat = fid.createDimension('latitude', lat_shape)
        lon = fid.createDimension('longitude', lon_shape)
        #level = fid.createDimension('levels',len(levelra))

        clevs = [0.2, 2, 4, 10, 100]
        clevels = fid.createDimension('contour_levels', len(clevs))
        # differnt runs with differnt sources
        # source_shape=xrash.coords['source'].shape[0]
        #source = fid.createDimension('source',source_shape)
        # differnt runs with different met data.
        ens_shape = xrash.coords['ensemble'].shape[0]
        ensemble = fid.createDimension('ensid', ens_shape)

        time = fid.createDimension('time', 1)  # one time per file
        bnds = fid.createDimension('bnds', 2)  # two bounds per time.

        #origin = fid.createDimension('origins',hxr.attrs['Number Start Locations'])
        origin = fid.createDimension('origins', 1)
        # Scalar variables

        #latra, lonra = hf.getlatlon(hxr)
        latra = xrash.latitude[:, 0]
        lonra = xrash.longitude[0]
        print('ens shape', ens_shape)
        print('lat shape', lat_shape, latra.shape, xrash.shape)
        print('lon shape', lon_shape, lonra.shape, xrash.shape)
        # Define variables with attributes
        # concid = fid.createVariable('conc', 'f4',
        #                           ('source', 'ensemble','levels','latitude','longitude'))

        # DEFINE A DIFFERENT VARIABLE for EACH LEVEL.
        # DIMENSIONS are ensemble tag, latitude, longitude

        levs = xrash.z.values
        lev1, lev2, lev3 = handle_levels(levs)

        coordlist = ('time', 'ensid', 'latitude', 'longitude')
        concidl1 = fid.createVariable('Flight_levelA', 'f4', coordlist)
        concidl1.units = munit + '/m3'
        concidl1.long_name = 'Concentration Array'
        concidl1.bottomlevel = 'FL0'
        top, bottom = get_topbottom(lev1)
        concidl1.toplevel = top

        concidl2 = fid.createVariable('Flight_levelB', 'f4', coordlist)
        concidl2.units = munit + '/m3'
        concidl2.long_name = 'Concentration Array'
        concidl2.bottomlevel = top
        top, bottom = get_topbottom(lev2)
        concidl2.toplevel = top

        concidl3 = fid.createVariable('Flight_levelC', 'f4', coordlist)
        concidl3.units = munit + '/m3'
        concidl3.long_name = 'Concentration Array'
        concidl3.bottomlevel = top
        top, bottom = get_topbottom(lev3)
        concidl3.toplevel = top

        massid = fid.createVariable('MassLoading', 'f4', coordlist)
        massid.units = munit + '/m2'
        massid.long_name = 'Mass Loading from surface to ' + top

        # Standard Contour levels for concentration in mg/m3
        clevelid = fid.createVariable('Contour_levels', 'f4', ('contour_levels'))
        clevelid[:] = clevs

        # Dimension with different ensemble members.
        ensembleid = fid.createVariable('ensemble', 'str', ('ensid'))
        ensid = fid.createVariable('ensid', 'i4', ('ensid'))
        sourceid = fid.createVariable('source', 'str', ('ensid'))

        latid = fid.createVariable('latitude', 'f4', ('latitude'))
        latid.long_name = 'latitude degrees north from the equator'
        latid.units = 'degrees_north'
        latid.point_spacing = 'even'
        lonid = fid.createVariable('longitude', 'f4', ('longitude'))
        lonid.long_name = 'longitude degrees east from the greenwhich meridian'
        lonid.units = 'degrees_east'
        lonid.point_spacing = 'even'

        #levelid = fid.createVariable('levels','int',('levels'))
        # attributes for levels
        #levelid.long_name = 'Top height of each layer'
        # levelid.units='m'

        timeid = fid.createVariable('time', 'f4', ('time'))
        # attributes for time grid.
        timeid.units = 'days since 1970-01-01 00:00:00'
        timeid.standard_name = 'time'
        timeid.bounds = 'time_bnds'
        timeid.calendar = 'gregorian'

        time_bnds = fid.createVariable('time_bnds', 'f4', ('time', 'bnds'))

        # Put data into variables
        # only one time per file.

        epoch = np.datetime64('1970-01-01T00:00:00Z')
        date1 = xrash.time[iii].values
        t1 = (xrash.time[iii].values - epoch) / np.timedelta64(1, 's')
        # change seconds to days
        t1 = t1 / (24.0 * 60 * 60)

        t2 = ((xrash.time[0].values + sample_time) - epoch) / np.timedelta64(1, 's')
        t2 = t2 / (24.0 * 60 * 60)

        temp = xrash.loc[dict(time=date1)]
        print(temp.values.shape)
        print('date', date1, type(lev1))
        #concid[:] = xrash.loc[:,date1].values
        mult = 1
        concidl1[:] = makeconc(xrash.copy(), date1, list(lev1), mult=mult)

        concidl2[:] = makeconc(xrash.copy(), date1, list(lev2), mult=mult)

        concidl3[:] = makeconc(xrash.copy(), date1, list(lev3), mult=mult)

        massid[:] = makeconc(mass, date1, level=None)

        latid[:] = latra
        lonid[:] = lonra
        #levelid[:] = levelra
        timeid[:] = t1
        time_bnds[:] = [[t1, t2]]
        # these may be duplicated since ensemble and source
        # dimensions are stacked.
        ensembleid[:] = xrash.coords['ens'].values
        sourceid[:] = xrash.coords['source'].values
        ensid[:] = np.arange(1, ens_shape + 1)
        fid.close()
        import sys
        # sys.exit()
        iii += 1


def makeconc(xrash, date1, level, mult=1, tr=True, verbose=False):
    """
    INPUTS
    xrash : xarray data-array
    date1 : datetime.datetime object
    level : list of level names
    RETURNS 
    c1 : data array with concentration from multiple levels combined.
    """
    if not level:
        c1 = mult * xrash.sel(time=date1)
    else:
        dhash = thickness_hash(xrash)
        tlist = []
        total_thickness = 0
        for lev in level:
            tlist.append(dhash[lev])
            total_thickness += dhash[lev]
        c1 = mult * xrash.sel(time=date1, z=level)
        if verbose:
            print('MAX BEFORE ', np.max(c1))
        print('length', len(level), tlist, dhash)
        c1 = mass_loading(c1, tlist)
        c1 = c1 / total_thickness
    if verbose:
        print('Max AFTER', np.max(c1))
    c1 = c1.expand_dims('time')
    # this line is for netcdf awips output
    if tr:
        c1 = c1.transpose('time', 'ensemble', 'y', 'x')
    if verbose:
        print('C1', c1)
    if verbose:
        print(c1.shape)
    return c1


def maketestblist():
    d1 = datetime.datetime(2008, 8, 8, 12)
    d2 = datetime.datetime(2008, 8, 8, 13)
    blist = {}
    flist = []
    dname = '/pub/Scratch/alicec/KASATOCHI/cylindrical/e3/'
    fname = 'wrf.e3.bin'
    flist.append((os.path.join(dname, fname), 'WRF'))
    fname = 'gdas.e3.bin'
    flist.append((os.path.join(dname, fname), 'GDAS'))
    blist['S3'] = flist

    flist1 = []
    dname = '/pub/Scratch/alicec/KASATOCHI/cylindrical/e2/'
    fname = 'wrf.e2.bin'
    flist1.append((os.path.join(dname, fname), 'WRF'))
    fname = 'gdas.e2.bin'
    flist1.append((os.path.join(dname, fname), 'GDAS'))
    blist['S2'] = flist1
    return blist


def maketestncfile():
    blist = maketestblist()
    oname = 'out.nc'
    d1 = datetime.datetime(2008, 8, 8, 12)
    d2 = datetime.datetime(2008, 8, 8, 13)
    cdump2awips(blist, oname, d1=d1, d2=d2)


def maketestra():
    #d1 = datetime.datetime(2008,8,8,10)
    #d2 = datetime.datetime(2008,8,8,13)
    d1 = None
    d2 = None
    blist = maketestblist()
    xrash, dt = combine_cdump(blist, d1=d1, d2=d2)
    return xrash, dt
