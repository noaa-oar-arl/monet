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

.. code-block:: python

    from monet.models import *

    c = cmaq.open_files(flist='')
