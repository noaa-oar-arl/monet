from monet.plots import savefig

from . import grids, models, monet_accessor, obs, plots, profile, sat, util

# from .monetmodels, obs, plots, util


# __all__ = ['models', 'obs', 'plots', 'sat',
#            'verification', 'util', 'monet_accessor', 'grids']
__all__ = [
    'models', 'obs', 'plots', 'sat', 'util', 'monet_accessor', 'grids',
    'profile'
]


def rename_latlon(ds):
    if 'latitude' in ds.coords:
        return ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif 'Latitude' in ds.coords:
        return ds.rename({'Latitude': 'lat', 'Longitude': 'lon'})
    elif 'Lat' in ds.coords:
        return ds.rename({'Lat': 'lat', 'Lon': 'lon'})
    else:
        return ds


def rename_to_monet_latlon(ds):
    if 'lat' in ds.coords:
        return ds.rename({'lat': 'latitude', 'lon': 'longitude'})
    elif 'Latitude' in ds.coords:
        return ds.rename({'Latitude': 'latitude', 'Longitude': 'longitude'})
    elif 'Lat' in ds.coords:
        return ds.rename({'Lat': 'latitude', 'Lon': 'longitude'})
    elif 'grid_lat' in ds.coords:
        return ds.rename({'grid_lat': 'latitude', 'grid_lon': 'longitude'})
    else:
        return ds

    
def coards_to_netcdf(dset): 
   from numpy import meshgrid, arange 
   lon = dset.lon 
   lat = dset.lat 
   lons, lats = meshgrid(lon,lat) 
   x = arange(len(lon)) 
   y = arange(len(lat)) 
   dset = dset.rename({'lon':'x','lat':'y'}) 
   dset.coords['longitude'] = (('y','x'), lons) 
   dset.coords['latitude'] = (('y','x'), lats) 
   dset['x'] = x 
   dset['y'] = y 
   dset = dset.set_coords(['latitude','longitude']) 
   return dset 
