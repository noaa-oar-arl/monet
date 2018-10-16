# -*- coding: utf-8 -*-
"""
Created on Fri Sep 28 11:41:46 2018

@author: Patrick Campbell
"""
import numpy as np          # numpy
import pandas as pd         # pandas
import matplotlib.pyplot as plt
import monet
import xarray as xr
from obs import icartt
from models import cmaq, combinetool
from util import tools, interp_util, resample
from plots.mapgen import *

obsname = 'C:/Users/drnim/OneDrive/Documents/UMD_NOAA_ARL/Obs/OWLETS2018_UMDAircraft_R0/OWLETS2018_UMDAircraft_RF1_20180617_R0.ict'
modname = 'C:/Users/drnim/OneDrive/Documents/UMD_NOAA_ARL/Mod/aqm.20180617.t12z.cgrid.ncf'
modnamez = 'C:/Users/drnim/OneDrive/Documents/UMD_NOAA_ARL/Mod/aqm.t12z.metcro3d.ncf'


# Read ICARTT data file and add data as xarray
dataread = icartt.add_data(obsname)

# Get flight 'time', 'latitude', and 'longitude', and specified 'altitude' (if specified)
obs = icartt.get_data(dataread, lat_label=None, lon_label=None,
                      alt_label='ALTGPS_m')

# get 3D CTM model data to analyze
mod = cmaq.open_files(modname)
# get 3D CTM model data that includes model layer heights (AGL) to interpolate
# The example below shows for CMAQ using mid-layer height above ground
# Note: If not available, calculate and provide associated xarray height (t,z,x,y) as modzvar
modz = cmaq.open_files(modnamez)
modzvar = modz['ZH']

# Combines model and interpolated flight data both horizontally and vertically
df_test = combinetool.combine_mod_to_flight_data(obs,
                                                 mod,
                                                 modzvar,
                                                 obsvar='O3_ppbv',
                                                 modvar='O3',
                                                 resample=False,
                                                 freq='60S',
                                                 )


df_test.to_csv('final_merge_test.csv')
print('done!')
