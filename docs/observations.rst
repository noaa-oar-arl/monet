
Observations
============

MONET currently supports and is continuously adding obervational datasets. Each
observation has a method 'add_data' that will retrieve if needed, read and aggregate
data for the user into either a pandas dataframe or xarray dataset.

Currently Supported Observational Datasets
------------------------------------------

* AirNow
* AQS
* IMPROVE
* crn
* aeronet
* CEMS
* ISD
* TOLNet


AirNow
------

AirNow_ data can be dowloaded and aggregated using the monet.obs.airnow class.  For example,
lets say that we want to look at data from 2018-05-01 to 2018-05-05.  Begin by importing the AirNow class and we will use pandas as a helper

.. code:: python

    from monet.obs import airnow
    import pandas as pd

Now we need to set the dates. We want hourly data so lets set the hourly flag

.. code:: python

    dates = pd.date_range(start='2018-05-01', end='2018-05-05', freq='H')

Now a simple one stop command to return the pandas DataFrame of the aggregated data.

.. code:: python

    df = airnow.add_data(dates)

This provides a structured dataframe.

.. code:: python

    df.head()
        time     siteid                 site  utcoffset variable     ...     cmsa_name  msa_code msa_name  state_name  epa_region
    0 2018-05-01  000010102           St. John's         -4    OZONE     ...           NaN       NaN      NaN          CC          CA
    1 2018-05-01  000010401          Mount Pearl         -4    OZONE     ...           NaN       NaN      NaN          CC          CA
    3 2018-05-01  000010401          Mount Pearl         -4    PM2.5     ...           NaN       NaN      NaN          CC          CA
    5 2018-05-01  000010501  Grand Falls Windsor         -4    OZONE     ...           NaN       NaN      NaN          CC          CA
    6 2018-05-01  000010601            Goose Bay         -4    OZONE     ...           NaN       NaN      NaN          CC          CA



AQS
---

AQS_ data can be dowloaded and aggregated using the monet.obs.airnow class.  For example,
lets say that we want to look at ozone concentration data from 2018-05-01 to 2018-05-05.  Begin by importing the AQS class and we will use pandas as a helper

.. code:: python

    from monet.obs import aqs
    import pandas as pd

Now we need to set the dates. We want hourly data so lets set the hourly flag

.. code:: python

    dates = pd.date_range(start='2018-05-01', end='2018-05-05', freq='H')

Now a simple one stop command to return the pandas DataFrame of the aggregated data.

.. code:: python

    df = aqs.add_data(dates,param=['OZONE'])

This provides a structured dataframe.

.. code:: python

    df.head()


.. _AirNow: https://www.airnow.gov
.. _AQS: https://www.epa.gov/aqs/
