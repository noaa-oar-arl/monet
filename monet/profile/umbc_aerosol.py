import os
from builtins import object

import pandas as pd
import xarray as xr


def open_dataset(fname):
    t = CL51()
    return t.add_data(fname)


def open_mfdataset(fname):
    from glob import glob
    from numpy import sort
    t = CL51()
    dsets = []
    for i in sort(glob(fname)):
        dsets.append(t.add_data(i))
    return xr.concat(dsets, dim='time')


class CL51(object):
    """Short summary.

    Attributes
    ----------
    objtype : type
        Description of attribute `objtype`.
    cwd : type
        Description of attribute `cwd`.
    dates : type
        Description of attribute `dates`.
    dset : type
        Description of attribute `dset`.
    daily : type
        Description of attribute `daily`.

    """

    def __init__(self):
        self.objtype = 'TOLNET'
        self.cwd = os.getcwd()
        self.dates = pd.date_range(
            start='2017-09-25', end='2017-09-26', freq='H')
        self.dset = None
        self.daily = False

    def add_data(self, fname):
        """Short summary.

        Parameters
        ----------
        fname : type
            Description of parameter `fname`.

        Returns
        -------
        type
            Description of returned object.

        """
        from h5py import File
        f = File(fname)
        atts = f['Instrument_Attributes']
        data = f['DATA']
        self.dset = self.make_xarray_dataset(data, atts)
        return self.dset

    @staticmethod
    def make_xarray_dataset(data, atts):
        """Short summary.

        Parameters
        ----------
        data : type
            Description of parameter `data`.
        atts : type
            Description of parameter `atts`.

        Returns
        -------
        type
            Description of returned object.

        """
        from numpy import array, ndarray
        # altitude variables
        alt = data['Altitude_m'][:].squeeze()

        # time variables
        time = pd.to_datetime(data['UnixTime_UTC'][:], unit='s')
        # Back Scatter
        bsc = data['Profile_bsc'][:]

        dataset = xr.Dataset()
        dataset['z'] = (('z'), alt)
        dataset['time'] = (('time'), time)
        dataset['x'] = (('x'), [0])
        dataset['y'] = (('y'), [0])

        dataset['bsc'] = (('time', 'z'), bsc)

        for i in list(atts.attrs.keys()):
            # print(type(atts.attrs[i]))
            if isinstance(atts.attrs[i], list) or isinstance(
                    atts.attrs[i], ndarray):
                # print('here')
                dataset.attrs[i] = atts.attrs[i][0]
            else:
                dataset.attrs[i] = atts.attrs[i]

        print(dataset)
        a = dataset.Location_lat.astype(float)
        latitude = float(a)
        a = dataset.Location_lon.astype(float)
        longitude = float(a)

        dataset.coords['latitude'] = (('y', 'x'), array(latitude).reshape(
            1, 1))
        dataset.coords['longitude'] = (('y', 'x'), array(longitude).reshape(
            1, 1))
        return dataset
