from __future__ import print_function

import os
import sys
from warnings import warn

try:
    from setuptools import setup, find_packages
except:
    from distutils.core import setup

setup(
    name='monet',
    version='1.2',
    url='https://github.com/noaa-oar-arl/MONET',
    license='MIT',
    author='Barry D. Baker',
    author_email='barry.baker@noaa.gov',
    maintainer='Barry Baker',
    maintainer_email='barry.baker@noaa.gov',
    packages=find_packages(),
    keywords=[
        'model', 'verification', 'hysplit', 'cmaq', 'atmosphere', 'camx',
        'evaluation'
    ],
    description='The Model and Observation Evaluation Toolkit (MONET)',
    install_requires=[
        'f90nml', 'numpy', 'pandas', 'wget', 'pyresample', 'netcdf4', 'xarray',
        'dask', 'matplotlib', 'seaborn', 'pseudonetcdf'
    ],
    dependency_links=[
        "git+ssh://git@github.com/barronh/pseudonetcdf.git@develop",
        "git+ssh://git@github.com/barronh/xarray.git@pnc-backend"
    ])
