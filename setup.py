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
    walker = os.walk('monet')
    prefix = os.path.join(os.path.curdir, 'monet')
    for thisdir, itsdirs, itsfiles in walker:
        if '__init__.py' in itsfiles:
            packages.append(thisdir[len(prefix) - 1:])

    return packages


packages = find_packages()

setup(
    name='monet',
    version='1.2',
    url='https://github.com/noaa-oar-arl/MONET',
    license='MIT',
    author='Barry D. Baker',
    author_email='barry.baker@noaa.gov',
    maintainer='Barry Baker',
    maintainer_email='barry.baker@noaa.gov',
    packages=packages,
    package_dir={'': 'src'},
    keywords=['model','verification','hysplit','cmaq','atmosphere','camx','evaluation'],
    description='The Model and Observation Evaluation Toolkit (MONET)',
    install_requires=['numpy', 'pandas', 'wget', 'pyresample', 'netcdf4', 'pynio', 'xarray', 'dask', 'matplotlib', 'seaborn', 'pseudonetcdf'],
    dependency_links=["git+ssh://git@github.com/barronh/pseudonetcdf.git@develop", "git+ssh://git@github.com/barronh/xarray.git@pnc-backend"]
)
