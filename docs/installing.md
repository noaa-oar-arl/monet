# Installation

## Required Dependencies

- Python 3.10 or later
- [numpy](https://numpy.org)
- [pandas](https://pandas.pydata.org)
- [xarray](https://docs.xarray.dev)
- [dask](https://docs.dask.org)
- [netcdf4](https://unidata.github.io/netcdf4-python/)
- [matplotlib](https://matplotlib.org/)
- [seaborn](https://seaborn.pydata.org/)
- [cartopy](https://scitools.org.uk/cartopy/docs/latest/)
- [pooch](https://github.com/fatiando/pooch)
- [pydecorate](https://github.com/pytroll/pydecorate)
- [mpi4py](https://mpi4py.readthedocs.io/)
- [xregrid](https://github.com/bbakernoaa/xregrid)
- [monet-stats](https://github.com/noaa-oar-arl/monet-stats)
- [pytspack](https://github.com/noaa-oar-arl/pytspack)

## Instructions

The simplest way to install MONET and its complex dependencies (like ESMF for `xregrid`) is using `conda` or `micromamba`:

```bash
conda install -c conda-forge monet
```

This will install MONET along with all required dependencies.

### Installing from Source

To install MONET from source, you can use `pip`. It is recommended to install it in a pre-configured environment with dependencies already installed (especially those requiring compilation like `esmpy`).

```bash
pip install git+https://github.com/noaa-oar-arl/monet.git
```

Or manually:

```bash
git clone https://github.com/noaa-oar-arl/monet.git
cd monet
pip install .
```

### Development Environment

For developers, you can create a dedicated environment using the provided `environment-dev.yml` file:

```bash
conda env create -f environment-dev.yml
conda activate monet-dev
pip install -e .
```
