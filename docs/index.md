# Model and ObservatioN Evaluation Toolkit (MONET)

**MONET** is an open-source project and Python package that aims to create a common platform for atmospheric composition data analysis for weather and air quality models.

MONET was developed to evaluate the Community Multiscale Air Quality Model (CMAQ) for the NOAA National Air Quality Forecast Capability (NAQFC) modeling system. MONET is designed to be a modularized Python package for:

1. Pairing model output to observational data
2. Leveraging the pandas Python package for easy searching
3. Analyzing and visualizing data

This process introduces a convenient method for evaluating model output. MONET processes data that is easily searchable and that can be grouped using meta-data found within the observational datasets. Common statistical metrics (e.g., bias, correlation, and skill scores), plotting routines such as scatter plots, timeseries, spatial plots, and more are included in the package. MONET is well-modularized and can add further observational datasets and different models.

Our goal is to provide easy tools to retrieve, read, and combine datasets in order to speed scientific research. Currently, MONET is able to process several models and observations related to air composition and meteorology.

Please [cite](#reference) our work.

## What's New

MONET v2.3.1 has been released. MONET has re-engineered the way it deals with multidimensional observations or model output by using an [xarray accessor](https://docs.xarray.dev/en/stable/internals/extending-xarray.html) giving MONET a flexible and intuitive way of expanding [xarray](https://docs.xarray.dev) for multidimensional geospatial information commonly used in meteorology, climate and air quality all while making it easier on the user to use MONET and add to it.

!!! important
    MONET also underwent a major restructure with v2.2.0. All I/O functions have been moved to a sister project: [MONETIO](https://github.com/noaa-oar-arl/monetio).

MONET features include:

* **xarray accessor** for both `xarray.DataArray` and `xarray.Dataset` using the `.monet` attribute
* **pandas accessor** for `pandas.DataFrame` using the `.monet` attribute
* Vertical interpolation using [pytspack](https://github.com/noaa-oar-arl/pytspack) via the `.monet.interpolate_vertical` function
* Spatial interpolation using `.monet.remap` including:
    * Nearest neighbor finder
    * Constant latitude interpolation
    * Constant longitude interpolation
    * Remap DataArray to current grid using `xregrid` (ESMF/ESMPy based)
    * Find nearest i,j or lat,lon
* Simplified [combine tool](api.md#monet.util.combinetool) to combine point source data with multidimensional xarray objects

## Reference

Baker, Barry; Pan, Li. 2017. "Overview of the Model and Observation Evaluation Toolkit (MONET) Version 1.0 for Evaluating Atmospheric Transport Models." Atmosphere 8, no. 11: 210. [doi:10.3390/atmos8110210](https://doi.org/10.3390/atmos8110210).

## Get in Touch

Ask questions, suggest features or view source code [on GitHub](https://github.com/noaa-oar-arl/monet).
