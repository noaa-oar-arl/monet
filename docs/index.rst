Model and ObservatioN Evalatution Toolkit (MONET)
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
   observations
   models
   tutorial
   monet_wcoss

Get in touch
------------

- Ask questions, suggest features or view source code `on GitHub`_.

.. _on GitHub: https://github.com/noaa-oar-arl/MONET


Supported datasets
------------------

**Supported Models**

* `HYSPLIT <https://www.ready.noaa.gov/HYSPLIT.php/>`_
* `CMAQ <https://www.epa.gov/cmaq/>`_
* `CAMx <http://www.camx.com/about/default.aspx/>`_
* FV3-CHEM (comming soon)
* WRF-CHEM (comming soon)

**Supported Observations**

* `AirNow <https://www.airnow.gov/>`_
* `AQS <https://www.epa.gov/aqs/>`_
* `AERONET <https://aeronet.gsfc.nasa.gov/>`_
* `CRN <https://www.ncdc.noaa.gov/crn/>`_
* `TOLNet <https://www-air.larc.nasa.gov/missions/TOLNet/>`_
* `CEMS <https://www.epa.gov/emc/emc-continuous-emission-monitoring-systems/>`_
* `IMPROVE <http://vista.cira.colostate.edu/Improve/>`_
* `ISH <https://www.ncdc.noaa.gov/isd/>`_





**Help & Reference**

.. toctree::
   :maxdepth: 10
   :caption: Help * Reference

   api
