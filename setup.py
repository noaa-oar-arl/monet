try:
    from setuptools import find_packages, setup
except ImportError:
    from distutils.core import setup

setup(
    name="monet",
    version="2.2.3",
    url="https://github.com/noaa-oar-arl/MONET",
    license="MIT",
    include_package_data=True,
    author="Barry D. Baker",
    author_email="barry.baker@noaa.gov",
    maintainer="Barry Baker",
    maintainer_email="barry.baker@noaa.gov",
    packages=find_packages(),
    package_data={
        "": ["data/*.txt", "data/*.dat", "data/*.hdf", "data/*.ncf", "data/*.jpg", "data/*.png"]
    },
    keywords=["model", "verification", "hysplit", "cmaq", "atmosphere", "camx", "evaluation"],
    description="The Model and Observation Evaluation Toolkit (MONET)",
    install_requires=[
        "pandas",
        "netcdf4",
        "xarray",
        "dask",
        "pyresample",
        "matplotlib",
        "seaborn",
        "cartopy",
        "pydecorate",
    ],
    extra_requires={'xesmf;platform_system!="Windows"'},
)
