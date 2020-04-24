try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

setup(name='monet',
      version='2.2.2',
      url='https://github.com/noaa-oar-arl/MONET',
      license='MIT',
      include_package_data=True,
      author='Barry D. Baker',
      author_email='barry.baker@noaa.gov',
      maintainer='Barry Baker',
      maintainer_email='barry.baker@noaa.gov',
      packages=find_packages(),
      package_data={
          '': [
              'data/*.txt', 'data/*.dat', 'data/*.hdf', 'data/*.ncf',
              'data/*.jpg', 'data/*.png'
          ]
      },
      keywords=[
          'model', 'verification', 'hysplit', 'cmaq', 'atmosphere', 'camx',
          'evaluation'
      ],
      description='The Model and Observation Evaluation Toolkit (MONET)',
      install_requires=[
          'pandas', 'netcdf4', 'xarray', 'dask', 'pyresample', 'matplotlib',
          'seaborn', 'cartopy', 'pydecorate', 'global_land_mask'
      ],
      extra_requires={'xesmf;platform_system!="Windows"'})
