""" This module opens data from the ICARTT format and reformats it for use in MONETself.
The module makes use of Barron Henderson's PseudoNetCDF (https://github.com/barronh/pseudonetcdf)
and xarray to read the data.  It is not intended to read more than ONE file at a time """

import xarray as xr
from pandas import Series, Timestamp, to_timedelta

possible_lats = [
    'Lat', 'Latitude', 'lat', 'latitude', 'Latitude_Deg', 'Latitude_deg',
    'Lat_deg', 'Lat_degree', 'Lat_Degree'
]
possible_lons = [
    'Lon', 'Longitude', 'lon', 'longitude', 'Longitude_Deg', 'Longitude_deg',
    'Lon_deg', 'Lon_degree', 'Lon_Degree', 'Long', 'Long_Deg'
]


def add_data(fname, time_label='UTC', lat_label=None, lon_label=None):
    dset = xr.open_dataset(fname, engine='pseudonetcdf')
    vars = pd.Series(dset.variables)
    if lat_label is None and lon_label is None:
    if vars.isin(possible_lats).max():
        latvar = vars.loc[vars.isin(possible_lats)][0]
        lonvar = vars.loc[vars.isin(possible_lons)][0]
        dset.coords['longitude'] = dset[lonvar]
        dset.coords['latitude'] = dset[latvar]
    #get the datetimes
    start = dset.SDATE.replace(', ', '-')
    time = Timestamp(start) + pd.to_timedelta(dset[time_name])
