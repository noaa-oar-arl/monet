import os

import pandas as pd
import xarray as xr


class tolnet:
    def __init__(self):
        self.cwd = os.getcwd()
        self.dates = pd.date_range(start='2017-09-25', end='2017-09-26', freq='H')
        self.dataset = None

    def open_data(self, fname):
        from h5py import File
        f = File(fname)
        atts = f['INSTRUMENT_ATTRIBUTES']
        data = f['DATA']
        self.dataset = self.make_xarray_dataset(data, atts)

    @staticmethod
    def make_xarray_dataset(data, atts):
        from numpy import NaN
        # altitude variables
        alt = data['ALT'][:].squeeze()
        altvars = ['AirND', 'AirNDUncert', 'ChRange', 'Press', 'Temp', 'TempUncert', 'O3NDResol', 'PressUncert']
        # time variables
        tseries = pd.Series(data["TIME_MID_UT_UNIX"][:].squeeze())
        time = pd.Series(pd.to_datetime(tseries, unit='ms'), name='time')
        tseries = pd.Series(data["TIME_START_UT_UNIX"][:].squeeze())
        stime = pd.to_datetime(tseries, unit='ms')
        tseries = pd.Series(data["TIME_STOP_UT_UNIX"][:].squeeze())
        etime = pd.to_datetime(tseries, unit='ms')
        # all other variables
        ovars = ['O3MR', 'O3ND', 'O3NDUncert', 'O3MRUncert', 'Precision']
        dset = {}
        for i in ovars:
            val = data[i][:]
            val[data[i][:] < -999.] = NaN
            dset[i] = (['altitude', 'time'], val)
        for i in altvars:
            dset[i] = (['altitude'], data[i][:].squeeze())
        coords = {'time': time, 'altitude': alt, 'start_time': stime, 'end_time': etime}
        attributes = {}
        for i in atts.attrs.keys():
            attributes[i] = atts.attrs[i]
        dataset = xr.Dataset(data_vars=dset, attrs=attributes, coords=coords)
        return dataset
