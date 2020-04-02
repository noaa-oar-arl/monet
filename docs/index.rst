Model and ObservatioN Evaluation Toolkit (MONET)
=================================================

**MONET** is an open source project and Python package that aims to create a
common platform for atmospheric composition data analysis for weather and
air quality models.

MONET was developed to evaluate the Community Multiscale Air Quality Model (CMAQ)
for the NOAA National Air Quality Forecast Capability (NAQFC) modeling system. MONET
is designed to be a modularized Python package for (1) pairing model output to observational
data in space and time; (2) leveraging the pandas Python package for easy searching
and grouping; and (3) analyzing and visualizing data. This process introduces a
convenient method for evaluating model output. MONET processes data that is easily
searchable and that can be grouped using meta-data found within the observational
datasets. Common statistical metrics (e.g., bias, correlation, and skill scores),
plotting routines such as scatter plots, timeseries, spatial plots, and more are
included in the package. MONET is well modularized and can add further observational
datasets and different models.

Our goals is to provide easy tools to retrieve, read and combine datasets in
order to speed scientific research.  Currently, MONET is able to process
several models and observations related to air composition and meteorology.

Please site our work.

What's New
^^^^^^^^^^

MONET v2.2.0 has been released.  MONET has re-engineered the way it deals with
multidimensional observations or model output by using an Xarray Accessor giving
MONET a flexible and intuitive way of expanding Xarray for multidimensional
geospatial information commonly used in meteorology, climate and air quality all while
making it easier on the user to use MONET and add to it.

MONET also underwent a major restructure with v2.2.0. All I/O functions have been
moved to a sister repository https://github.com/noaa-oar-arl/monetio.

Features include:

  * Xarray Accessor for both xarray.DataArray and xarray.Dataset using the .monet attribute
  * Pandas Accessor for pandas.DataFrame using the .monet attribute
  * vertical interpolation using python-stratify (https://github.com/SciTools-incubator/python-stratify) using the .monet.stratify function
  * spatial interpolation using .monet.remap including:
    - Nearest neighbor finder
    - Constant latitude interpolation
    - Constant longitude interpolation
    - remap DataArray to current grid using pyresample nearest neighbor or xesmf
    - remap entire dataset to current grid using pyresample nearest neighbor or xesmf
    - find nearest i,j or lat lon
    - interpolate to constant latitude or longitude
  * Simplified combine tool to combine point source data with multidimensional xarray objects

Reference
^^^^^^^^^

Baker, Barry; Pan, Li. 2017. “Overview of the Model and Observation
Evaluation Toolkit (MONET) Version 1.0 for Evaluating Atmospheric
Transport Models.” Atmosphere 8, no. 11: 210




.. toctree::
   :maxdepth: 4
   :caption: Getting Started

   why-monet
   installing
   monet-accessor
   monet_wcoss
   tutorial
..   observations
..   models



Get in touch
------------

- Ask questions, suggest features or view source code `on GitHub`_.

.. _on GitHub: https://github.com/noaa-oar-arl/MONET


**Help & Reference**

.. toctree::
   :maxdepth: 10
   :caption: Help * Reference

   api
