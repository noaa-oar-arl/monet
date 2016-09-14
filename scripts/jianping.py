#pathtopython

import pandas as pd
import aqs
from datetime import datetime
from matplotlib.pyplot import *
import seaborn as sns
import plots

##EDIT THE NUMBER OF YEARS HERE
#####################################################
years = ['2014', '2015', '2016']
#####################################################


dfs = []

for i in years:
    start = i + '-01-01 00'
    end = i + '12-31 23'
    dates = pd.date_range(start, end, freq='H').values.astype('M8[s]').astype(datetime)
    data = aqs.aqs()

    data.dates = dates

    data.load_all_hourly_data(dates, datasets='PM')

    dfs.append(data.df)

df = pd.concat(dfs, ignore_index=True)

# Separate by species
pm25df = df.groupby('Species').get_group('PM2.5')
pm10df = df.groupby('Species').get_group('PM10')
no3df = df.groupby('Species').get_group('NO3f')
so4df = df.groupby('Species').get_group('SO4f')

#########
##lets look at PM2.5

# first make a time series plot over the entire domain of the hourly data
title = 'HOURLY PM2.5 CONUS'
plots.timeseries_single_var(pm25df, title=title, label='AQS PM2.5')
show()

#***************************************************************
####if you wish to save the figure uncomment the line below#####
#************************************************************
#savefig('pm25_hourly.jpg',dpi=100)

# now lets resample to daily data and replot
title = 'Daily PM2.5 CONUS'
plots.timeseries_single_var(pm25df, title=title, label='AQS PM2.5', sample='D')
show()

# resample to weekly
title = 'Weekly PM2.5 CONUS'
plots.timeseries_single_var(pm25df, title=title, label='AQS PM2.5', sample='W')
show()

# resample to monthly
title = 'Monthy PM2.5 CONUS'
plots.timeseries_single_var(pm25df, title=title, label='AQS PM2.5', sample='M')
show()

# replot in each of the EPA regions the month timeseries
regions = pm25df.Regions.unique()
for i in regions:
    title = 'Monthly PM2.5 ' + i
    d = pm25df.loc[pm25df.Regions == i]
    plots.timeseries_single_var(d, title=title, label='AQS PM2.5', sample='M')
    show()
