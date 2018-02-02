
## Introduction to the Model and ObservatioN Evalution Toolkit (MONET)

This is the start to an MONET verification package. Currently, it handles CMAQ 4.7.1+, EPA AQS surface data, EPA AirNow, and the IMPROVE Aerosol data network. Current effort is being added to add in the ASOS network, the Climate Reference Network, sonde data and more. It will compute statistics, make time series, spatial and scatter plots, and more!  

The ARL verification package is meant to be a one stop shop for quick verification and study of CMAQ data (in the future we will want to add more models such as HYSPLIT, NMMB, WRF-ARW, and NGGPS and observations such as NADP, ASOS, iicart flight data, VIIRS, MODIS, GOES-R, etc).  

Please refer to the [publication](http://www.mdpi.com/2073-4433/8/11/210) and the [wiki](https://github.com/noaa-oar-arl/MONET/wiki) for more detail.  

#### Reference

Baker, Barry; Pan, Li.	2017. "Overview of the Model and Observation Evaluation Toolkit (MONET) Version 1.0 for Evaluating Atmospheric Transport Models." Atmosphere 8, no. 11: 210.

### New in MONET

* Additional objects created for the Integrated Surface Database (ISD), the U.S. Climate Reference Network (CRN), Aerosol Robotic Network (AERONET)
* Moved the interpolation to a pyresample ImageContainer.  This restricts users to a nearest neighbor resampling with minimal differences in interpolation differences.  Speed improvements are substantial.
* Moved to xarray for opening CMAQ files.  xarray is an implementation of N-Dimensional pandas dataframes and allows out of memory computation using dask to increase reading and processing.
* Increased usage of dask to read observational data increasing read speed of many files.  

### Basic tutorial for AirNow and AQS.  

Please refer to the [AQS Tutorial ](https://github.com/noaa-oar-arl/MONET/wiki/Compare-CMAQ-to-AQS), [AirNow Tutorial](https://github.com/noaa-oar-arl/MONET/wiki/Comparing-CMAQ-and-AirNow), and [IMPROVE Tutorial](https://github.com/noaa-oar-arl/MONET/wiki/Compare-CMAQ-to-the-IMPROVE-Network)

The function calls are nearly identical (except for retrieving and loading the network dataset the package is identical).  

### Compare more than one simulation to a surface network

Please refer to [Compare Two Simulation Tutorial](https://github.com/noaa-oar-arl/MONET/wiki/Comparing-two-CMAQ-Simulations-Plotting-Overlay-Example). Several examples of how to use the package are shown to make time series with two simulations during July 2016 of NAQFC and NAQFC-Beta.

### Make Spatial Plots

Please refer to [Make Spatial Plot tutorial](https://github.com/noaa-oar-arl/MONET/wiki/Creating-Spatial-Plots-from-AIRNOW-and-CMAQ) for an example of how to make a spatial plot and overlay the surface monitor data.  

### Example Plots

![Time Series](https://github.com/noaa-oar-arl/MONET/blob/master/sample_figures/pm2.5_timeseries.jpg?raw=true)

![Time Series of RMSE](https://github.com/noaa-oar-arl/MONET/blob/master/sample_figures/pm2.5_timeseries_rmse.jpg?raw=true)

![Spatial Plots](https://github.com/noaa-oar-arl/MONET/blob/master/sample_figures/ozone_spatial.jpg?raw=true)

![Scatter Plots](https://github.com/noaa-oar-arl/MONET/blob/master/sample_figures/no2_scatter.jpg?raw=true)
![PDFS Plots](https://github.com/noaa-oar-arl/MONET/blob/master/sample_figures/no2_pdf.jpg?raw=true)
![Difference Scatter Plots](https://github.com/noaa-oar-arl/MONET/blob/master/sample_figures/no2_diffscatter.jpg?raw=true)
![Difference PDFS Plots](https://github.com/noaa-oar-arl/MONET/blob/master/sample_figures/no2_diffpdf.jpg?raw=true)

### Installation

MONET can easily be installed by using pip

```pip install https://github.com/noaa-oar-arl/MONET.git```

### Required Packages
Many of the required packages can be gotten with the Anaconda or Enthought Canopy python packages.

Required packges:

  * pyresample
  * basemap
  * pandas
  * numpy
  * scipy
  * datetime
  * ftplib
  * pywget
  * xarray
  * dask
  * netcdf4-python
