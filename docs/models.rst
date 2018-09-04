******
Models
******

MONET is capable of opening output from several different models including CMAQ,
HYSPLIT and CAMx.  MONET opens all N-Dimensional output in the :py:class:`~xarray.Dataset`.

CMAQ
----

CMAQ is a 3D photochemical grid model developed at the U.S. EPA to simulate air
composition.  MONET is able to read the output IOAPI output and format it to be
compatible with it's datastream.

As an example, lets open some CMAQ data from the Hawaiian volcanic eruption in 2018.
First we will set the path to the data files


.. code-block:: python

    import monet

    cmaqfile = monet.__path__ + '/../data/aqm.t12z.aconc.ncf'

    from monet.models import cmaq, hysplit, camx

    c = cmaq.open_files(cmaqfile)


This will return an :py:class:`~xarray.Dataset`.  The dataset is also still stored
in the :py:class:`~cmaq` object as :py:class:`~cmaq.dset`.  To view a ncdump style output
of the xarray dataset you can simply print the dataset.

.. code:: python

  print(c)
  <xarray.Dataset>
  Dimensions:    (DATE-TIME: 2, VAR: 41, time: 45, x: 80, y: 52, z: 1)
  Coordinates:
      latitude   (y, x) float32 ...
      longitude  (y, x) float32 ...
    * time       (time) datetime64[ns] 2018-05-17T12:00:00 2018-05-17T13:00:00 ...
  Dimensions without coordinates: DATE-TIME, VAR, x, y, z
  Data variables:
      TFLAG      (time, VAR, DATE-TIME) int32 dask.array<shape=(45, 41, 2), chunksize=(45, 41, 2)>
      O3         (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      NO2        (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      NO         (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      NO3        (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      N2O5       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      HNO3       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      HONO       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      PNA        (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      CO         (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      FORM       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ALD2       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      PAN        (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      NTR        (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      XO2N       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      SO2        (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ASO4I      (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ASO4J      (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ANH4I      (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ANH4J      (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ANO3I      (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ANO3J      (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AORGAI     (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AORGAJ     (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AORGPAI    (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AORGPAJ    (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AORGBI     (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AORGBJ     (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AECI       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AECJ       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      A25I       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      A25J       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      NUMATKN    (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      NUMACC     (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      SRFATKN    (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      SRFACC     (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AH2OI      (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      AH2OJ      (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ACLI       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ACLJ       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ANAI       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
      ANAJ       (time, z, y, x) float32 dask.array<shape=(45, 1, 52, 80), chunksize=(45, 1, 52, 80)>
  Attributes:
      IOAPI_VERSION:  $Id: @(#) ioapi library version 3.1 $                    ...
      EXEC_ID:        ????????????????                                         ...
      FTYPE:          1
      CDATE:          2018142
      CTIME:          135716
      WDATE:          2018142
      WTIME:          135716
      SDATE:          2018137
      STIME:          120000
      TSTEP:          10000
      NTHIK:          1
      NCOLS:          80
      NROWS:          52
      NLAYS:          1
      NVARS:          41
      GDTYP:          2
      P_ALP:          19.0
      P_BET:          21.0
      P_GAM:          -157.5
      XCENT:          -157.5
      YCENT:          20.53
      XORIG:          -480000.0
      YORIG:          -312000.0
      XCELL:          12000.0
      YCELL:          12000.0
      VGTYP:          1
      VGTOP:          200.0
      VGLVLS:         [1.       0.089794]
      GDNAM:          AQF_HI
      UPNAM:          OPACONC
      VAR-LIST:       O3              NO2             NO              NO3      ...
      FILEDESC:       Concentration file output                                ...
      HISTORY:

All MONET xarray objects have common coordinate names (latitude and longitude) and dimension names (time, x, y, z).  It retains the
original attributes of the file and variable names.  MONET will precalculate some variables while loading the data in a lazy fashion, i.e. it
will not actually do the computation (not stored in memory) until needed:

.. code:: python

    pm25 = cmaq.PM25

where nox is a :py:class:`~xarray.DataArray` as it is a single variable.  To quickly plot this on a map we can use the utility function
in :py:class:`~monet.plots.mapgen`.

.. code:: python

  ax = monet.plots.draw_map()
  pm25[10,0,:,:].plot(x='longitude',y='latitude',ax=ax)

CAMx
----

to do ...

HYSPLIT
-------

to do ...
