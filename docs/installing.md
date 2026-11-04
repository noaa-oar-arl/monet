# Installation

## Required Dependencies

- Python 3.11 or later
- [numpy](https://numpy.org)
- [pandas](https://pandas.pydata.org)
- [xarray](https://docs.xarray.dev)
- [netcdf4](https://unidata.github.io/netcdf4-python/)
- [matplotlib](https://matplotlib.org/)
- [seaborn](https://seaborn.pydata.org/)
- [cartopy](https://scitools.org.uk/cartopy/docs/latest/)
- [pooch](https://github.com/fatiando/pooch)
- [AXIS (axis-regrid)](https://github.com/NOAA-EMC/HELM/tree/develop/libs/axis)
- [pytspack](https://github.com/noaa-oar-arl/pytspack)
- [monet-stats](https://github.com/noaa-oar-arl/monet-stats)
- [dask](https://docs.dask.org)
- [pillow](https://python-pillow.org)
- [scipy](https://scipy.org)

## Optional Dependencies

Some features require additional dependencies:
- [geopandas](https://geopandas.org), [rasterio](https://rasterio.readthedocs.io), [shapely](https://shapely.readthedocs.io): For advanced spatial masking (`pip install monet[regions]`)
- [monetio](https://github.com/noaa-oar-arl/monetio): For reading various model and observation formats (`pip install monet[obs]`)

## Instructions

MONET uses AXIS for horizontal and vertical regridding. The `pytspack` dependency
is retained for other existing utilities. AXIS is
installed from the pinned HELM commit
`dda4cb5cb2b05f082c8637c4ad594fb74d8f8671`; the distribution is `axis-regrid`
and the Python import is `axis`.

### Native prerequisites

Install a C++20 compiler (GCC >=13 or Clang >=16), CMake >=3.21, and MPI with C++
development headers. AXIS also requires Kokkos and KokkosKernels 5.1; its build can
fetch them when no installed packages are found. PROJ is optional for projected grids.
No published AXIS wheel is assumed by this version of MONET. A pip install builds
native code and does not require ESMF or a conda environment.

On macOS, match the compiler and OpenMP library to your Kokkos installation. For
an Apple-Clang-built Homebrew Kokkos, install AXIS with Apple Clang and the same
Homebrew libomp library:

```bash
CXX=/usr/bin/clang++ CC=/usr/bin/clang \
CXXFLAGS="-include bit -I$(brew --prefix libomp)/include" \
CMAKE_PREFIX_PATH="$(brew --prefix)" \
pip install \
  -Ccmake.define.OpenMP_CXX_FLAGS="-Xpreprocessor -fopenmp" \
  -Ccmake.define.OpenMP_CXX_LIB_NAMES=omp \
  -Ccmake.define.OpenMP_omp_LIBRARY="$(brew --prefix libomp)/lib/libomp.dylib" \
  -Ccmake.define.KokkosKernels_ADD_DEFAULT_ETI=OFF \
  "axis-regrid @ git+https://github.com/NOAA-EMC/HELM.git@dda4cb5cb2b05f082c8637c4ad594fb74d8f8671#subdirectory=libs/axis"
```

The `-include bit` flag works around a missing C++20 header in fetched KokkosKernels
5.1.1. Loading two different OpenMP runtime libraries into one process can crash;
use one matching compiler/runtime throughout the native dependencies.

When combining AXIS with packages such as Numba and `monetio`'s `grib2io`, inspect
all of their OpenMP dependencies with `otool -L` and choose one runtime for the
whole process. For example, Numba may use Homebrew's `libomp`; in that case,
build AXIS with `-Ccmake.define.OpenMP_omp_LIBRARY="$(brew --prefix libomp)/lib/libomp.dylib"`.
Matching only AXIS and `grib2io` is not sufficient if another imported package
uses a different runtime.

Also inspect `grib2io/iplib` and its `libip_d` dependency. An `iplib` extension
that resolves `libip_d` from a different Conda environment can load yet another
OpenMP runtime. If grib2io's own interpolation functions are not needed, rebuild
grib2io without that optional component; MONET's AXIS regridding does not use it:

```bash
python -m pip install --force-reinstall --no-deps \
  --config-settings=cmake.define.GRIB2IO_WITH_IP=OFF \
  "grib2io @ git+https://github.com/NOAA-MDL/grib2io.git@b9bc80b0c7d950d1f955fe96a97d965198ad57a1"
```

If CMake cannot find `grib2.h`, set `G2C_INCDIR` and `G2C_LIBDIR` to the
NCEPLIBS-g2c `include` and `lib` directories before running the install command.

### Installing from source

With the native prerequisites available:

```bash
git clone https://github.com/noaa-oar-arl/monet.git
cd monet
pip install .
```

`pyproject.toml` declares the exact AXIS git URL. For a standalone AXIS installation:

```bash
pip install "axis-regrid @ git+https://github.com/NOAA-EMC/HELM.git@dda4cb5cb2b05f082c8637c4ad594fb74d8f8671#subdirectory=libs/axis"
```

To verify the engine is available:

```bash
python -c "from monet.util.resample import has_axis; print(has_axis)"
```

If the native library cannot load, regridding raises an installation error with the
original loader error attached. MONET's other functionality remains importable.

### Python 3.11 Notes

Developers may be interested in the [Python 3.11 release notes](https://docs.python.org/3/whatsnew/3.11.html). The most relevant change for MONET is a general speedup, but review the summary for other potential impacts.

### Development Environment

For developers, you can create a dedicated environment using the provided `environment-dev.yml` file:

```bash
conda env create -f environment-dev.yml
conda activate monet-dev
pip install -e .
```
