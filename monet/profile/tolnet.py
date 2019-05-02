import os
from builtins import object

import pandas as pd
import xarray as xr


def open_dataset(fname):
    t = TOLNet()
    return t.add_data(fname)


def tolnet_colormap():
    from matplotlib.colors import ListedColormap
    from numpy import array
    Colors = [
        array([255, 140, 255]) / 255.,
        array([221, 111, 242]) / 255.,
        array([187, 82, 229]) / 255.,
        array([153, 53, 216]) / 255.,
        array([119, 24, 203]) / 255.,
        array([0, 0, 187]) / 255.,
        array([0, 44, 204]) / 255.,
        array([0, 88, 221]) / 255.,
        array([0, 132, 238]) / 255.,
        array([0, 175, 255]) / 255.,
        array([0, 235, 255]) / 255.,
        array([39, 255, 215]) / 255.,
        array([99, 255, 155]) / 255.,
        array([163, 255, 91]) / 255.,
        array([211, 255, 43]) / 255.,
        array([255, 255, 0]) / 255.,
        array([255, 207, 0]) / 255.,
        array([255, 159, 0]) / 255.,
        array([255, 111, 0]) / 255.,
        array([255, 63, 0]) / 255.,
        array([255, 0, 0]) / 255.,
        array([216, 0, 15]) / 255.,
        array([178, 0, 31]) / 255.,
        array([140, 0, 47]) / 255.,
        array([102, 0, 63]) / 255.,
        array([52, 52, 52]) / 255.,
        array([52, 52, 52]) / 255.,
        array([52, 52, 52]) / 255.,
        array([52, 52, 52]) / 255.,
        array([52, 52, 52]) / 255.,
        array([52, 52, 52]) / 255.,
        array([96, 96, 96]) / 255.,
        array([96, 96, 96]) / 255.,
        array([96, 96, 96]) / 255.,
        array([96, 96, 96]) / 255.,
        array([96, 96, 96]) / 255.,
        array([96, 96, 96]) / 255.
    ]
    TNcmap = ListedColormap(Colors)
    TNcmap.set_under([1, 1, 1])
    TNcmap.set_over([0, 0, 0])
    return TNcmap


def tolnet_plot(dset, var='O3MR', units='ppbv'):
    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set_context('notebook')
    cmap = tolnet_colormap()
    Fig, Ax = plt.subplots(figsize=(9, 6))
    dset['z'] /= 1000.  # put in km
    dset[var].attrs['units'] = units
    dset[var].plot(x='time', y='z', cmap=cmap, vmin=0, vmax=300, ax=Ax)
    plt.ylabel("Altitude [km]")
    plt.xlabel("Time [UTC]")
    sns.despine()
    plt.tight_layout(pad=0)
    # plt.colorbar(label="O3 [ppbv]")


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
        # altitude variables
        alt = data['ALT'][:].squeeze()
        altvars = [
            'AirND', 'AirNDUncert', 'ChRange', 'Press', 'Temp', 'TempUncert',
            'PressUncert'
        ]
        # time variables
        tseries = pd.Series(data["TIME_MID_UT_UNIX"][:].squeeze())
        time = pd.Series(pd.to_datetime(tseries, unit='ms'), name='time')
        # all other variables
        ovars = [
            'O3MR', 'O3ND', 'O3NDUncert', 'O3MRUncert', 'O3NDResol',
            'Precision'
        ]

        dataset = xr.Dataset()
        dataset['z'] = (('z'), alt)
        dataset['time'] = (('time'), time)
        for i in ovars:
            dataset[i] = (('z', 'time'), data[i][:])
            dataset[i] = dataset[i].where(dataset[i] > -990)
        for i in altvars:
            print(i)
            dataset[i] = (('z'), data[i][:].squeeze())

        for i in list(atts.attrs.keys()):
            dataset.attrs[i] = atts.attrs[i][0]

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
