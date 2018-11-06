from __future__ import print_function

import os
import sys
from warnings import warn

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

setup(
    name='monet',
    version='2.1',
    url='https://github.com/noaa-oar-arl/MONET',
    license='MIT',
    author='Barry D. Baker',
    author_email='barry.baker@noaa.gov',
    maintainer='Barry Baker',
    maintainer_email='barry.baker@noaa.gov',
    packages=find_packages(),
    packaged_data={'monet': ['data/*.txt',
                             'data/*.dat', 'data/*.hdf', 'data/*.ncf']},
    keywords=[
        'model', 'verification', 'hysplit', 'cmaq', 'atmosphere', 'camx',
        'evaluation'
    ],
    description='The Model and Observation Evaluation Toolkit (MONET)',
    install_requires=[
        'numpy>=1.6','cython', 'pandas', 'pyresample', 'netcdf4', 'xarray', 'dask',
        'matplotlib', 'seaborn', 'pseudonetcdf', 'future', 'sphinx',
        'pandoc','proj4','cartopy'],
    dependency_links=[
        "git+ssh://git@github.com/barronh/pseudonetcdf.git","git+ssh://github.com/QuLogic/cartopy.git@requirements"
    ])
