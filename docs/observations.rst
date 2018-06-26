******************
Observations
******************

This section will aid in how to use MONET to load observational datasets available.

First we will import several libraries to aid for in the future.

.. code::   python

    import numpy as np          # numpy
    import pandas as pd         # pandas
    from monet.obs import *     # observations from MONET
    import matplotlib.pyplot as plt # plotting
    import seaborn as sns       # better color palettes
    import cartopy.crs as ccrs  # map projections
    import cartopy.feature as cfeature # politcal and geographic features

AirNow
------

AirNow is the near realtime dataset for air composition and meteorology measurements.

    "The U.S. EPA AirNow program is the national repository of real time air quality data and forecasts for the United States. AirNow is the vehicle for providing timely Air Quality Index (AQI) information to the public, media outlets, other federal agencies and their applications, and to the research community. The system is managed by the U.S. EPA’s Office of Air Quality Planning and Standards Outreach and Information Division, Information Transfer Group in Research Triangle Park (RTP), North Carolina. AirNow is currently hosted and operated at a contractor facility, known as the AirNow Data Management Center (DMC), which currently resides outside of RTP." - https://www.airnow.gov/index.cfm?action=ani.airnowUS

AirNow_ data can be dowloaded from the Amazon S3 server and aggregated using the
monet.obs.airnow class.  For example,lets say that we want to look at data from
2018-05-01 to 2018-05-05.

Now we need to set the dates. We want hourly data so lets set the hourly flag

.. code::   python

    dates = pd.date_range(start='2018-05-01', end='2018-05-05', freq='H')

Now a simple one stop command to return the pandas :py:class:`~pandas.DataFrame`
of the aggregated data on the given dates.  MONET reads the hourly data from AirNow.

.. code::   python

    df = airnow.add_data(dates)

This provides a structured :py:class:`~pandas.DataFrame`.

.. code::   python

    df.head()

Some users may want to keep a local copy of the data and not have to retrive the data
each time they want to access the data.  There is a simple kwarg that can be used to
download the data, *download=True*.  By default, *download* is set to False.

.. code::   python

    df = airnow.add_data(dates,download=True)

This format may be less intuitive.  It is the "long" ASCII format found in the files
where each index is a single record with a single variable and value.  This may be
incovinient but there are a helper function in *monet.utils.tools* to
assist in reshaping the pandas :py:class:`~pandas.DataFrame` so that it can be more convenient.

.. code::   python

    from monet.utils.tools import unstack
    df2 = unstack(df2)
    df2.head()

Available Measurements
^^^^^^^^^^^^^^^^^^^^^^

* O3 (OZONE)
* PM2.5
* PM10
* SO2
* NO2
* CO
* NO2
* NOx
* NO
* Wind Speed and Direction (WS, WDIR)
* Temperature (TEMP)
* Relative Humidity (RH)
* Solar Radiation (SRAD)

EPA AQS
-------

MONET is able to use the EPA AQS data that is collected and reported on an hourly and daily time scale.

    "The Air Quality System (AQS) contains ambient air pollution data collected by EPA, state, local, and tribal air pollution control agencies from over thousands of monitors.  AQS also contains meteorological data, descriptive information about each monitoring station (including its geographic location and its operator), and data quality assurance/quality control information.  AQS data is used to:
    assess air quality,
    evaluate State Implementation Plans for non-attainment areas,
    prepare reports for Congress as mandated by the Clean Air Act." - https://www.epa.gov/aqs

We will begin by loading hourly ozone concentrations from 2018.  The EPA AQS data
is seperated into yearly files and seperate files for hourly and daily data.  The
files are also seperated by which variable is measured.  For instance, hourly ozone files
for the entire year of 2018 are found in https://aqs.epa.gov/aqsweb/airdata/hourly_44201_2018.zip.
We will first load a single variable and then add multiple later on.

.. code::  python

  #first determine the dates
  dates = pd.date_range(start='2018-01-01', end='2018-12-31', freq='H')
  # load the data
  df = aqs.add_data(dates, param=['OZONE'])

If you would rather daily data to get the 8HR max ozone concentration or daily maximum
concentration you can add the *daily* kwarg.

.. code::   python

  df = aqs.add_data(dates, param=['OZONE'], daily=True)

As in AirNow you can download the data to the local disk using the *download*

.. code::   python

  df = aqs.add_data(dates, param=['OZONE'], daily=True, download=True)


Available Measurements
^^^^^^^^^^^^^^^^^^^^^^

* O3 (OZONE)
* PM2.5 (PM2.5)
* PM2.5_frm (PM2.5)
* PM10
* SO2
* NO2
* CO
* NONOxNOy
* VOC
* Speciated PM (SPEC)
* Speciated PM10 (PM10SPEC)
* Wind Speed and Direction (WIND, WS, WDIR)
* Temperature (TEMP)
* Relative Humidity and Dew Point Temperature (RHDP)

Loading Multiple Measurements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's load variables PM10 and OZONE using hourly data to get an idea of how to get multiple variables:

.. code::   python

  df = aqs.add_data(dates, param=['OZONE','PM10'])

Loading Specfic Network
^^^^^^^^^^^^^^^^^^^^^^^

Sometimes you may want to load a specific network that is available in the AQS data
files.  For instance, lets load data from the Chemical Speciation Network (CSN; https://www3.epa.gov/ttnamti1/speciepg.html).
As of writting this tutorial we will load the 2017 data as it is complete.

.. code::   python

    dates = pd.date_range(start='2017-01-01', end='2018-01-01', freq='H')
    df = aqs.add_data(dates,param=['SPEC'], network='CSN', daily=True )

Available Networks
^^^^^^^^^^^^^^^^^^

* NCORE (https://www3.epa.gov/ttn/amtic/ncore.html)
* CSN (https://www3.epa.gov/ttnamti1/speciepg.html)
* CASTNET (https://www.epa.gov/castnet)
* IMPROVE (http://vista.cira.colostate.edu/Improve/)
* PAMS (https://www3.epa.gov/ttnamti1/pamsmain.html)
* SCHOOL AIR TOXICS (https://www3.epa.gov/ttnamti1/airtoxschool.html)
* NEAR ROAD (NO2; https://www3.epa.gov/ttn/amtic/nearroad.html)
* NATTS (https://www3.epa.gov/ttnamti1/natts.html)

AERONET
-------

    "The AERONET (AErosol RObotic NETwork) project is a federation of ground-based
    remote sensing aerosol networks established by NASA and PHOTONS (PHOtométrie pour le Traitement Opérationnel de Normalisation Satellitaire; Univ. of Lille 1, CNES, and CNRS-INSU)
    and is greatly expanded by networks (e.g., RIMA, AeroSpan, AEROCAN, and CARSNET) and collaborators from national agencies, institutes, universities, individual scientists, and partners. Fo more than 25 years, the project has provided long-term, continuous and readily accessible public domain database of aerosol optical, microphysical and radiative properties for aerosol research and characterization, validation of satellite retrievals, and synergism with other databases. The network imposes standardization of instruments, calibration, processing and distribution.

    AERONET collaboration provides globally distributed observations of spectral aerosol optical depth (AOD), inversion products, and precipitable water in diverse aerosol regimes. Version 3 AOD data are computed for three data quality levels: Level 1.0 (unscreened), Level 1.5 (cloud-screened and quality controlled), and Level 2.0 (quality-assured). Inversions, precipitable water, and other AOD-dependent products are derived from these levels and may implement additional quality checks. " -https://aeronet.gsfc.nasa.gov

MONET uses the AERONET web services to access data. All data products available through
their web service portal is available in MONET except for the raw sky scans.  This includes the AOD and SSA as well
as the inversion products.

Available Measurements
^^^^^^^^^^^^^^^^^^^^^^

.. csv-table:: AOD and SDA Measurements
   :header: "Product", "Explanation"
   :widths: 20, 20

   "AOD10", "Aerosol Optical Depth Level 1.0"
   "AOD15", "Aerosol Optical Depth Level 1.5"
   "AOD20", "Aerosol Optical Depth Level 2.0"
   "AOD15", "Aerosol Optical Depth Level 1.5"
   "SDA10", "SDA Retrieval Level 1.0"
   "SDA15", "SDA Retrieval Level 1.5"
   "SDA20", "SDA Retrieval Level 2.0"
   "TOT10", "Total Optical Depth based on AOD Level 1.0 (all points only)"
   "TOT15", "Total Optical Depth based on AOD Level 1.5 (all points only)"
   "TOT20", "Total Optical Depth based on AOD Level 2.0 (all points only)"

.. csv-table:: Inversion Products
   :header: "Product", "Explanation"
   :widths: 20, 20

   "SIZ",	"Size distribution"
   "RIN",	"Refractive indicies (real and imaginary)"
   "CAD",	"Coincident AOT data with almucantar retrieval"
   "VOL",	"Volume concentration, volume mean radius, effective radius and standard deviation"
   "TAB",	"AOT absorption"
   "AOD",	"AOT extinction"
   "SSA",	"Single scattering albedo"
   "ASY",	"Asymmetry factor"
   "FRC",	"Radiative Forcing"
   "LID",	"Lidar and Depolarization Ratios"
   "FLX",	"Spectral flux"
   "ALL",	"All of the above retrievals (SIZ to FLUX) in one file"
   "PFN*",	"Phase function (available for only all points data format: AVG=10)"

Loading AOD and SDA
^^^^^^^^^^^^^^^^^^^

Aeronet is global data so we are going to look at a single day to speed this along.
First we need to create a datetime array

.. code::   python

  dates = pd.date_range(start='2017-09-25',end='2017-09-26',freq='H')

Now lets assume that we want to read the Aerosol Optical Depth Level 1.5 data that is
cloud-screened and quality controlled.

.. code::   python

  df = aeroent.add_data(dates=dates, product='AOD15')
  df.head()

Now sometimes you want only data over a specific region.  To do this lets define a
latitude longitude box *[latmin,lonmin,latmax,lonmax]* over northern Africa

.. code::   python

  df = aeroent.add_data(dates=dates, product='AOD15', latlonbox=[2.,-21,38,37])
  df[['latitude','longitude']].describe()

To download inversion products you must supply the *inv_type* kwarg.  It accepts either
"ALM15" or "ALM20" from the AERONET web services.  Lets get the size distribution
from data over northern Africa

.. code:: python

  df = aeroent.add_data(dates=dates, product='SIZ', latlonbox=[2.,-21,38,37], inv_type='ALM15')


NADP
----

NADP is a composed of five regional networks; NTN, AIRMoN, AMoN, AMNet, and MDN.
MONET allows you to read data from any of the five networks with a single call by
specifying the wanted network.

To add data from any of the networks it is a simple call using the nadp object.  As
an example, to load data from the NTN network the call would look like:

.. code:: python

  df = nadp.add_data(dates, network='NTN')

To read data from another network simply replace the network with the name of the
wanted network.  The network name must be a string but is case insensitive.

NTN
^^^

    "The NTN is the only network providing a long-term record of precipitation chemistry across the United States.

    Sites predominantly are located away from urban areas and point sources of pollution. Each site has a precipitation
    chemistry collector and gage. The automated collector ensures that the sample is exposed only during precipitation (wet-only-sampling)."
    - http://nadp.slh.wisc.edu/NTN/

Available Measurements
======================

* H+ (ph)
* Ca2+ (ca)
* Mg2+ (mg)
* Na+ (na)
* K+ (k)
* SO42- (so4)
* NO3- (no3)
* Cl- (cl)
* NH4+ (nh4)

MDN
^^^

    "The MDN is the only network providing a longterm record of total mercury (Hg) concentration and deposition in precipitation in the United States and Canada. All MDN sites follow standard procedures and have uniform precipitation chemistry collectors and gages. The automated collector has the same basic design as the NTN collector but is modified to preserve mercury. Modifications include a glass funnel, connecting tube, bottle for collecting samples, and an insulated enclosure to house this sampling train. The funnel and connecting tube reduce sample exposure to the open atmosphere and limit loss of dissolved mercury. As an additional sample preservation measure, the collection bottle is charged with 20 mL of a one percent hydrochloric acid solution."
    - http://nadp.slh.wisc.edu/MDN/

Available Measurements
======================

* net concentration of methyl mercury in ng/L (conc)
* precipitation amount (in inches) reported by the raingage for the entire sampling period. (raingage)
* Mg2+ (mg)
* Na+ (na)
* K+ (k)
* SO42- (so4)
* NO3- (no3)
* Cl- (cl)
* NH4+ (nh4)

IMPROVE
-------

to do...

OpenAQ
------

to do.....

CEMS
----

to do.....

Climate Reference Network
-------------------------

to do.....

Integrated Surface Database
---------------------------

to do.....
