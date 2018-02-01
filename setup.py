from __future__ import print_function

import os
import sys
from warnings import warn

try:
    from setuptools import setup
except:
    from distutils.core import setup


def find_packages():
    import os
    packages = []
    walker = os.walk('src')
    prefix = os.path.join(os.path.curdir, 'src')
    for thisdir, itsdirs, itsfiles in walker:
        if '__init__.py' in itsfiles:
            packages.append(thisdir[len(prefix) - 1:])

    return packages


packages = find_packages()

setup(
    name='monet',
    version='1.1',
    url='https://github.com/noaa-oar-arl/MONET',
    license='MIT',
    author='Barry D. Baker',
    author_email='barry.baker@noaa.gov',
    maintainer='Barry Baker',
    maintainer_email='barry.baker@noaa.gov',
    author_email='Barry.Baker@noaa.gov',
    packages=packages,
    package_dir={'': 'monet'},
    description='The Model and Observation Evaluation Toolkit (MONET)',
    install_requires=['numpy', 'pandas', 'wget', 'pyresample', 'netcdf4', 'pynio', 'xarray', 'dask', 'matplotlib', 'seaborn', 'pseudonetcdf'],
    dependency_links=["git+ssh://git@github.com/barronh/pseudonetcdf.git@develop", "git+ssh://git@github.com/barronh/xarray.git@pnc-backend"]
    classifiers=['Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3',
                 'Operating System :: MacOS',
                 'Operating System :: Microsoft :: Windows',
                 'Operating System :: POSIX',
                 'Topic :: Scientific/Engineering',
                 'Topic :: Scientific/Engineering :: Atmospheric Science',
                 ]
)
