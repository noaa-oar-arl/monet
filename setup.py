from distutils.core import setup

setup(
    name='MONET',
    version='v1.0',
    packages=['MONET'],
    package_dir={'': 'monet'},
    url='https://github.com/noaa-oar-arl/MONET',
    license='',
    author='Barry Baker - NOAA ARL',
    author_email='barry.baker@noaa.gov',
    install_requires=['numpy',
                      'pyresample',
                      'basemap',
                      'pytz',
                      'pandas',
                      'xarray',
                      'dask',
                      'wget',
                      'matplotlib',
                      'seaborn'],
    description='The Model and ObservatioN Evaluation Toolkit (MONET) is designed to allow easy and quick analysis of chemical transport models and relevant observations'
)
