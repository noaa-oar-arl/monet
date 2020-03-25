Installing on WCOSS
===================

If you have access to the NOAA WCOSS machines you can create your own python environments very
easily using intel-python.  First, it is suggested to create a .condarc file that lives in your `$HOME` folder to point to
a location that will house your conda environments.  Below is a sample .condarc file.
::

  channels:
    - intel
    - conda-forge
    - defaults
  envs_dirs:
    - /gpfs/dell2/emc/verification/noscrub/User.Name/conda/envs
  pkgs_dirs:
    - /gpfs/dell2/emc/verification/noscrub/User.Name/conda/pkgs

Next you should start a new environment by cloning the default environment to a new name. This can be done in a
single command.
::
  conda create -n myenv --clone="/usrx/local/prod/intel/2019UP05/intelpython3"

A prompt should come up and tell you to activate the environment you just created, myenv.

::
  source activate myenv

From here you can install any package the same way you could on regular anaconda installations.
::
  conda install -c bbakernoaa monet 
