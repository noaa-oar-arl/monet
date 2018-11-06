******************
Opening Model Data
******************

MONET is capable of opening output from several different models.  This tutorial will
demonstrate how to open, extract variables and quickly display the results.

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
    gridcro2d = monet.__path__ + '/../data/aqm.t12z.grdcro2d.ncf'

    from monet.models import *

    c = cmaq.open_dataset(flist=cmaqfile, grid=gridcro2d)

This will return an :py:class:`~xarray.Dataset`.  The dataset is also still stored
in the :py:class:`~cmaq` object as :py:class:`~cmaq.dset`.

more here


CAMx
----
