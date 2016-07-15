# arl_verify
## Introductin to the Chemical Transport Model Verification


This is the start to an ARL verification package.  It currently handles CMAQ 4.7.1+, EPA AQS surface data, EPA AIRNOW, and the IMPROVE Aerosol data network.  It will compute statistics, make time series, spatial and scatter plots and more!  

It is meant to be a one stop shop for quick verification and study of CMAQ data (in the future we will want to add more models such as HYSPLIT, NMMB, WRF-ARW and NGGPS).  

In the future the addition of satellite data and more may be included.

### Basic tutorial for AIRNOW and AQS.  

Please refer to the [AQS Tutorial ](https://github.com/bakerbd/arl_verify/wiki/Compare-CMAQ-to-AQS) and [AIRNOW](https://github.com/bakerbd/arl_verify/wiki/Comparing-CMAQ-and-AirNow)

The function calls are nearly identical (except for retrieving and loading the network dataset the package is identical).  

### Compare more than one simulation to a surface network

Please refer to [Compare Two Simulation Tutorial](https://github.com/bakerbd/arl_verify/wiki/Comparing-two-CMAQ-Simulations-Plotting-Overlay-Example).  Several examples of how to use the package are shown to make time series with two simulations during July 2016 of NAQFC and NAQFC-Beta.

### Make Spatial Plots

Please refer to [Make Spatial Plot tutorial](https://github.com/bakerbd/arl_verify/wiki/Creating-Spatial-Plots-from-AIRNOW-and-CMAQ) for an example of how to make a spatial plot and overlay the surface monitor data.  

### Example Plots

![Time Series](https://raw.githubusercontent.com/bakerbd/arl_verify/master/sample_figures/pm2.5_timeseries.jpg)

![Time Series of RMSE](https://raw.githubusercontent.com/bakerbd/arl_verify/master/sample_figures/pm2.5_timeseries_rmse.jpg)

![Spatial Plots](https://github.com/bakerbd/arl_verify/blob/master/sample_figures/ozone_spatial.jpg?raw=true)

![Scatter Plots](https://github.com/bakerbd/arl_verify/blob/master/sample_figures/no2_scatter.jpg?raw=true)
![PDFS Plots](https://github.com/bakerbd/arl_verify/blob/master/sample_figures/no2_pdf.jpg?raw=true)
![Difference Scatter Plots](https://github.com/bakerbd/arl_verify/blob/master/sample_figures/no2_diffscatter.jpg?raw=true)
![Difference PDFS Plots](https://github.com/bakerbd/arl_verify/blob/master/sample_figures/no2_diffpdf.jpg?raw=true)

