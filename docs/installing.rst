Installation
============

Required dependencies
---------------------

- Python 2.7 [1]_, 3.4, 3.5, or 3.6
- `numpy <http://www.numpy.org/>`__ (1.11 or later)
- `pandas <http://pandas.pydata.org/>`__ (0.18.0 or later)
- `xarray <http://xarray.pydata.org/>`__ (0.10 or later)
- `dask <http://dask.pydata.org/>`__
- `pseudonetcdf <https://github.com/barronh/pseudonetcdf/>`__
- `xesmf <https://github.com/pytroll/pyresample/>`__ (1.9.0 or later)
- `netcdf4 <http://unidata.github.io/netcdf4-python/>`__
- `matplotlib <https://matplotlib.org/>`__
- `seaborn <https://seaborn.pydata.org/>`__
- `cartopy <https://scitools.org.uk/cartopy/docs/latest/>`__


For parallel computing
~~~~~~~~~~~~~~~~~~~~~~

- `dask.array <http://dask.pydata.org>`__ (0.9.0 or later): required for

For plotting
~~~~~~~~~~~~

- `matplotlib <http://matplotlib.org/>`__: required for :ref:`plotting`
- `cartopy <http://scitools.org.uk/cartopy/>`__: recommended for
  :ref:`plot-maps`
- `seaborn <https://stanford.edu/~mwaskom/software/seaborn/>`__: for better
  color palettes


Instructions
------------

MONET itself is a pure Python package, but some of it's dependencies may not be.
The simplest way to install MONET is to install it from the conda-forge feedstock::

    $ conda install -c conda-forge monet

This will install all of the dependencies needed by MONET and MONET itself.

xesmf is an optional dependency and can be installed easily from conda-forge,
Note xesmf is not available on windows due to the dependency on esmpy and esmf.::

    $ conda install -c conda-forge monet

To install MONET from source code you must install with pip.  This can be done directly
from the github page::

    $ pip install git+https://github.com/noaa-oar-arl/MONET.git

or you can manually download it from GitHub and install it using the setup.py::

    $ git clone https://github.com/noaa-oar-arl/MONET.git
    $ cd MONET
    $ pip install .

.. [1] MONET has dropped support for python 2.7 and requires python 3.6+.  For more information see the
   following references:

      - `Python 3 Statement <http://www.python3statement.org/>`__
      - `Tips on porting to Python 3 <https://docs.python.org/3/howto/pyporting.html>`__
