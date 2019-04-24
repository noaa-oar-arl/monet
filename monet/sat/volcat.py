import numpy as np
import numpy.ma as ma
import matplotlib.pyplot as plt
from pyhdf.SD import SD, SDC

# ModisHDF class -  gets satellite retrieval information from HDF file.


class VolcatHDF(object):
    """reads data from a hdf file"""

    def __init__(self, fname, verbose=0, datatype="532"):
        self.fname = fname
        self.missing = -999
        self.version = "vcat_ashprop_13_15_16"
        # self.version='volcld_ret_14_15_16'
        self.lat = []
        self.lon = []
        self.height = []
        self.radisu = []
        self.mass = []
        self.get_sd(verbose=verbose)

    def get_sd(self, verbose=1):
        """retrieves data from SD (scientific data) set."""
        dset = SD(self.fname, SDC.READ)
        a = dset.datasets()
        self.attributes = dset.attributes()
        if verbose:
            print("ATTRIBUTES")
            print(self.attributes)
            print(a)
            for key in self.attributes.keys():
                print(key)
                # temp = dset.select(key)
                # print temp.info()
        lat = dset.select("pixel_latitude")
        lon = dset.select("pixel_longitude")
        self.lat = lat.attributes()["scale_factor"] * lat[:, :]
        self.lon = lon.attributes()["scale_factor"] * lon[:, :]

        self.height = dset.select(self.version + "_ash_top_height")[:, :]
        self.radius = dset.select(self.version + "_ash_effective_radius")[:, :]
        self.mass = dset.select(self.version + "_ash_mass_loading")[:, :]
        return 1

    def get_radius(self, units="um"):
        rval = ma.masked_equal(self.radius[:, :], self.missing)
        return rval

    def get_mass_loading(self, units="g/m2"):
        rval = ma.masked_equal(self.mass[:, :], self.missing)
        return rval

    def get_latlon(self):
        """Returns 2d arrays of latitude and longitude"""
        return self.lat, self.lon

    def get_height(self, units="km"):
        """Returns array with retrieved height of the highest layer of ash."""
        """ Default units is km above sea-level"""
        rval = ma.masked_equal(self.height[:, :], self.missing)
        return rval

    def quickplot(self):
        fig = plt.figure(1)
        val = self.get_height()
        lat, lon = self.get_latlon()
        cb = plt.pcolormesh(lon, lat, val)
        plt.colorbar(cb)
        plt.title("height")
        fig = plt.figure(2)
        val = self.get_mass_loading()
        cb = plt.pcolormesh(lon, lat, val)
        plt.colorbar(cb)
        plt.title("mass")
        plt.show()
