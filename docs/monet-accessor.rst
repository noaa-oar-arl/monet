*********************
MONET XArray Accessor
*********************

MONET can add georeferecing tools to xarray‘s data structures. These tools can be
accessed via a special .monet attribute, available for both xarray.DataArray and
xarray.Dataset objects after a simple import monet in your code.

Initializing the Accessor
--------------------------

All you have to do is import monet after you import xarray.


.. code-block:: python

    import xarray as xr
    import monet as m

    cmaqfile = monet.__path__ + '/../data/aqm.t12z.aconc.ncf'

    from monet.models import cmaq

    c = cmaq.open_dataset(cmaqfile)

    c.O3[0,0,:,:].monet.quick_map()

.. image:: tutorial/CMAQ_hi_volcano_files/o3_example_quickmap.png


Interpolation Accessors
-----------------------

The MONET accessor provides several useful interpolation routines including:
Getting the nearest point to a given latitude and longitude, interpolating to a
constant latitude or longitude, interpolating to a vertical levels, and remapping
entire 2D :py:class::`~xarray.DataArray` or :py::class::`xarray.DataSet`.

Find Nearest Lat lon point
^^^^^^^^^^^^^^^^^^^^^^^^^^

To find the nearest latitude longitude point you just need to use the
.monet.nearest_latlon method. In this example we will continue to use the CMAQ
test file above.  We will find the closest grid points to lat=20.5, lon=-157.4.

.. code-block:: python

    c.monet.nearest_latlon(lat=20.5,lon=-157.4)

.. parsed-literal::

  <xarray.Dataset>
  Dimensions:    (time: 48, x: 1, y: 1, z: 1)
  Coordinates:
  * time       (time) datetime64[ns] 2018-05-17T12:00:00 2018-05-17T13:00:00 ...
    latitude   (y, x) float64 dask.array<shape=(1, 1), chunksize=(1, 1)>
    longitude  (y, x) float64 dask.array<shape=(1, 1), chunksize=(1, 1)>
  Dimensions without coordinates: x, y, z
  Data variables:
    O3         (time, z, y, x) float32 dask.array<shape=(48, 1, 1, 1), chunksize=(48, 1, 1, 1)>

Notice that the length of the dimensions are now (time: 48, x: 1, y: 1, z: 1).
If you wanted to only find the nearest location for a single variable you can
use the accessor on the :py:class:`~xarray.DataArray`.

.. code:: python

    c.O3.monet.nearest_latlon(lat=20.5,lon=-157.4)

.. parsed-literal::

  <xarray.DataArray 'O3' (time: 48, z: 1, y: 1, x: 1)>
  dask.array<shape=(48, 1, 1, 1), dtype=float32, chunksize=(48, 1, 1, 1)>
  Coordinates:
   * time       (time) datetime64[ns] 2018-05-17T12:00:00 2018-05-17T13:00:00 ...
     latitude   (y, x) float64 dask.array<shape=(1, 1), chunksize=(1, 1)>
     longitude  (y, x) float64 dask.array<shape=(1, 1), chunksize=(1, 1)>
  Dimensions without coordinates: z, y, x
  Attributes:
     long_name:   O3
     units:       ppbV
     var_desc:    Variable O3
     _FillValue:  nan
