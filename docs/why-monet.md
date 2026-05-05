# Overview: Why MONET?

## Features

Retrieving, loading, and combining data and putting into a common format is the core of MONET. MONET uses the [pandas](https://pandas.pydata.org) and [xarray](https://docs.xarray.dev) data formats for data analysis.

- **Open point observations in a common format:** [pandas](https://pandas.pydata.org) excels at working with tabular data or point measurements. It is used for time series analysis and statistical measures.
- **Open model and satellite data in a common format:** [xarray](https://docs.xarray.dev) is used when N-dimensional arrays are needed.
- **Retrieving observational datasets** for given time and space.
- **Efficiently combine/interpolate** model and observational datasets.
- **Provide easy plotting** using proven tools in Python.
- **Perform statistics** between model runs or observations or models and observations.

## Gallery

![Time Series](https://raw.githubusercontent.com/noaa-oar-arl/monet/stable/sample_figures/pm2.5_timeseries.jpg?raw=true)
*Time Series*

![Time Series of RMSE](https://raw.githubusercontent.com/noaa-oar-arl/monet/stable/sample_figures/pm2.5_timeseries_rmse.jpg?raw=true)
*Time Series of RMSE*

![Spatial Plots](https://raw.githubusercontent.com/noaa-oar-arl/monet/stable/sample_figures/ozone_spatial.jpg?raw=true)
*Spatial Plots*

| ![Scatter Plots](https://raw.githubusercontent.com/noaa-oar-arl/monet/stable/sample_figures/no2_scatter.jpg?raw=true) | ![PDFS Plots](https://raw.githubusercontent.com/noaa-oar-arl/monet/stable/sample_figures/no2_pdf.jpg?raw=true) |
| :---: | :---: |
| ![Difference Scatter Plots](https://raw.githubusercontent.com/noaa-oar-arl/monet/stable/sample_figures/no2_diffscatter.jpg?raw=true) | ![Difference PDFS Plots](https://raw.githubusercontent.com/noaa-oar-arl/monet/stable/sample_figures/no2_diffpdf.jpg?raw=true) |
