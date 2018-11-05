#!/data/aqf2/barryb/anaconda2/envs/patrick_monet/bin/python
"""
Created on Fri Sep 28 11:41:46 2018

@author: Patrick Campbell
"""
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D
import monet
from monet.obs import icartt
from monet.models import cmaq, combinetool
import seaborn as sns

obsname = '/data/aqf/patrickc/data/OWLETS_2018/OWLETS2018_UMDAircraft_RF1_20180617_R0.ict'
modname = '/data/aqf/patrickc/models/NAQFC/tests/aqm.20180617.t12z.cgrid.ncf'
modnamez = '/data/aqf/patrickc/models/NAQFC/tests/aqm.t12z.metcro3d.ncf'


# Read ICARTT data file and add data as xarray
dataread = icartt.add_data(obsname)

# Get flight 'time', 'latitude', and 'longitude', and specified 'altitude' (if specified)
obs_df = icartt.get_data(dataread, lat_label=None, lon_label=None,
                         alt_label='ALTGPS_m')

# get 3D CTM model data to analyze
mod_da = cmaq.open_dataset(modname)
# get 3D CTM model data that includes model layer heights (AGL) to interpolate
# The example below shows for CMAQ using mid-layer height above ground ---mod_da and mod_daz must be of same shape
mod_daz = cmaq.open_dataset(modnamez)
# Set model variable and vertical height fields for interpolation of passed data arrays
col = 'O3'
colz = 'ZH'

# Combines model and interpolated flight data both horizontally and vertically
# df_test = combinetool.combine_mod_to_flight_data(obs,
#                                                 mod,
#                                                 modzvar,
#                                                 obsvar='O3_ppbv',
#                                                 modvar='O3',
#                                                 resample = False,
#                                                 freq = '60S',
#                                                 )

df_test = combinetool.combine_da_to_df_xesmf_strat(
    mod_da[col], mod_daz[colz][0:4, :, :, :], obs_df, method='bilinear', reuse_weights=True)
# fills missing values, -9999, with NaN
df_test_fill = df_test.replace(-9999, np.NaN)
# write to csv to check
df_test_fill.to_csv('df_out_' + col + '.csv')

# set plotting context
sns.set_context('paper')

# Simple O3 Line Analysis plot
plt.figure()
plt.xlabel('time (UTC)')
plt.ylabel('O3 (ppbv)')
plt.plot('time', 'O3', data=df_test_fill, marker='', color='olive',
         linewidth=2, linestyle='solid', label="model")
plt.plot('time', 'O3_ppbv', data=df_test_fill, marker='', color='blue',
         linewidth=2, linestyle='solid', label="observed")
plt.legend()
plt.ylim(40, 90)
# More sophisticated 3D O3 contour plot:
df_diff = df_test_fill['O3'] - \
    df_test_fill['O3_ppbv']  # take mod-obs difference
fig = plt.figure()
ax = fig.gca(projection='3d')
ax.scatter3D(df_test_fill['longitude'], df_test_fill['latitude'],
             df_test_fill['altitude'], c=df_diff, cmap=cm.coolwarm)
ax.set_title('Mod-Obs O3 Difference (ppbv)')
ax.set_xlabel('longitude', rotation=150)
ax.set_ylabel('latitude')
ax.set_zlabel('altitude (m)', rotation=60)
ax.scatter3D(df_test_fill['longitude'], df_test_fill['latitude'],
             df_test_fill['altitude'], c=df_diff, cmap=cm.coolwarm)
fig, ax = plt.subplots()  # get label bar
cax = ax.imshow(np.expand_dims(df_diff, axis=1),
                interpolation='nearest', cmap=cm.coolwarm)
cbar = fig.colorbar(cax, orientation='vertical')


# df_test.plot(x='time',y=col,kind='line')
# plt.show()
# df_test.plot(x='time',y='O3_ppbv',kind='line')
# plt.show()
# df_test.plot(x='O3_ppb',y=col,kind='line')
# plt.show()
