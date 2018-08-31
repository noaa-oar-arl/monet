import os
from builtins import object

import pandas as pd
import xarray as xr


class TOLNet(object):
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
        atts = f['INSTRUMENT_ATTRIBUTES']
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
        from numpy import NaN
        # altitude variables
        alt = data['ALT'][:].squeeze()
        altvars = [
            'AirND', 'AirNDUncert', 'ChRange', 'Press', 'Temp', 'TempUncert',
            'O3NDResol', 'PressUncert'
        ]
        # time variables
        tseries = pd.Series(data["TIME_MID_UT_UNIX"][:].squeeze())
        time = pd.Series(pd.to_datetime(tseries, unit='ms'), name='time')
        tseries = pd.Series(data["TIME_START_UT_UNIX"][:].squeeze())
        # stime = pd.to_datetime(tseries, unit='ms')
        tseries = pd.Series(data["TIME_STOP_UT_UNIX"][:].squeeze())
        # etime = pd.to_datetime(tseries, unit='ms')
        # all other variables
        ovars = ['O3MR', 'O3ND', 'O3NDUncert', 'O3MRUncert', 'Precision']
        dset = {}
        for i in ovars:
            val = data[i][:]
            val[data[i][:] < -1.] = NaN
            dset[i] = (['z', 't'], val)
        for i in altvars:
            dset[i] = (['z'], data[i][:].squeeze())

    # coords = {'time': time, 'z': alt, 'start_time': stime, 'end_time': etime}
        attributes = {}
        for i in list(atts.attrs.keys()):
            attributes[i] = atts.attrs[i]
        dataset = xr.Dataset(data_vars=dset, attrs=attributes)
        dataset['time'] = (['t'], time)
        dataset['t'] = dataset['time']
        dataset = dataset.drop('time').rename({'t': 'time'})
        dataset['z'] = alt
        # get latlon
        a, b = dataset.Location_Latitude.decode('ascii').split()
        if b == 'S':
            dataset['latitude'] = -1 * float(a)
        else:
            dataset['latitude'] = float(a)
        a, b = dataset.Location_Longitude.decode('ascii').split()
        if b == 'W':
            dataset['longitude'] = -1 * float(a)
        else:
            dataset['longitude'] = float(a)
        return dataset
