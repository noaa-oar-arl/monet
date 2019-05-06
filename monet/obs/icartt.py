""" This module opens data from the ICARTT format and reformats it for use in
MONET. The module makes use of Barron Henderson's PseudoNetCDF
(https://github.com/barronh/pseudonetcdf)and xarray to read the data.  It is
noti ntended to read more than ONE file at a time """

import os

import pandas as pd
import xarray as xr


def add_data(fname):
    ic = ICARTT()
    dset = ic.add_data(fname)
    return dset


def xarray_flight_to_pandas(da, **kwargs):
    ic = ICARTT()
    return ic.get_data(da, **kwargs)


class ICARTT(object):
    """Short summary.
    Reads icartt data file format and gets/reformats 4D coordinate data used
    to manipulate/analyze and/or combine with model data later
    """

    def __init__(self):
        self.objtype = 'ICARTT'
        self.cwd = os.getcwd()
        self.dset = None

    def add_data(self, fname):
        """ This assumes that you have downloaded the specific ICARTT flight data
        Xarray Open/Read the ICARTT flight dataset as pseudonetcdf


        """
        # Xarray Open/Read the ICARTT flight dataset as pseudonetcdf engine
        self.dset = xr.open_dataset(
            fname, engine='pseudonetcdf', decode_times=False)
        return self.dset

    def get_data(self, da, lat_label=None, lon_label=None, alt_label=None):
        """ Comes in as an xarray from add_xarray_Data
        Allows for searching or user specified Lat/Lon variable names
        Sets latitude and longitude as coordinates
        Reads start date and seconds elapsed, converts time coordinate
        """
        possible_lats = [
            'Lat', 'Latitude', 'lat', 'latitude', 'Latitude_Deg',
            'Latitude_deg', 'Lat_deg', 'Lat_degree', 'Lat_Degree',
            'Latitude_degrees', 'Latitude_Degrees', 'Latitude_degree',
            'Latitude_Degree', 'Lat_aircraft', 'Latitude_aircraft'
        ]
        possible_lons = [
            'Lon', 'Longitude', 'lon', 'longitude', 'Longitude_Deg',
            'Longitude_deg', 'Lon_deg', 'Lon_degree', 'Lon_Degree', 'Long',
            'Long_Deg', 'Longitude_degrees', 'Longitude_Degrees',
            'Longitude_degree', 'Latitude_Degree', 'Lon_aircraft',
            'Long_aircraft'
            'Longitude_aircraft'
        ]

        # Get the unknown lat/lon variable names data along flight path
        if lat_label is None and lon_label is None:
            #   Change to dataframe to search the columns for possible lat lons
            dfset = da.to_dataframe()
            dfset['latitude'] = dfset[dfset.columns[dfset.columns.isin(
                possible_lats)]]
            dfset['longitude'] = dfset[dfset.columns[dfset.columns.isin(
                possible_lons)]]
            #   Place latitude/longitude back into xarray
            da['latitude'] = dfset['latitude']
            da['longitude'] = dfset['longitude']
        else:
            da['latitude'] = da[lat_label]
            da['longitude'] = da[lon_label]

        # Set laititude and longitude as x-y coordinates for the file
        da.coords['latitude'] = da['latitude']
        da.coords['longitude'] = da['longitude']

        # Change main 1D dimension name from POINTS to time for easier resampling
        da = da.rename({'POINTS': 'time'}, inplace=True)

        # Convert time from start date and TFLAG in UT seconds from midnight
        da['time'] = pd.to_datetime(da.SDATE.replace(
            ', ', '-')) + pd.to_timedelta(
                da[da.TFLAG], unit='s')

        # If user sets the specified altitude label it will be a coordinate
        if alt_label is None:
            print(
                'No alt_label provided...nothing added as z-coordinate (will result in vert_interp error)'
            )
        else:
            # Set specified 'altitude' as z coordinates for the file
            da.coords['altitude'] = da[alt_label]
        df = da.to_dataframe().reset_index()
        return df
