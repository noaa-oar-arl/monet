.. MONET documentation master file, created by
   sphinx-quickstart on Thu May 31 14:30:30 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Model and ObservatioN Evalatution Toolkit (MONET)
=================================================

**MONET** is an open source project and Python package that aims to create a
common platform for atmospheric composition data analysis for weather and
air quality models.

Our goals is to provide easy tools to retrieve, read and combine datasets in
order to speed scientific research.  Currently, MONET is able to process
several models and observations related to air composition and meteorology.

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

Documentation
-------------

**Getting Started**

* :doc:`why-monet`
* :doc:`installing`
* :doc:`observations`

.. toctree::
   :maxdepth: 3
   :caption: Getting Started

   why-monet
   installing
   observations

Get in touch
------------

- Ask questions, suggest features or view source code `on GitHub`_.

.. _on GitHub: https://github.com/noaa-oar-arl/MONET


Reference
^^^^^^^^^

Baker, Barry; Pan, Li. 2017. “Overview of the Model and Observation
Evaluation Toolkit (MONET) Version 1.0 for Evaluating Atmospheric
Transport Models.” Atmosphere 8, no. 11: 210

**Help & Reference**

.. toctree::
   :maxdepth: 1
   :caption: Help * Reference

   api

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
