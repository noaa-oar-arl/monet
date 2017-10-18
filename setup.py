from distutils.core import setup

setup(
    name='MONET',
    version='1.0',
    packages=['MONET'],
    url='https://github.com/noaa-oar-arl/MONET',
    license='MIT',
    author='Barry D. Baker',
    author_email='Barry.Baker@noaa.gov',
    description='The Model and Observation Evaluation Toolkit (MONET)',
    install_requires=['numpy pandas wget pyresample netcdf4 pynio xarray dask matplotlib seaborn'],
)
