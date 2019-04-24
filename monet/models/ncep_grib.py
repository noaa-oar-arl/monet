# FV3-CHEM READER
import xarray as xr


def open_dataset(fname):
    """Open a single dataset from fv3chem outputs (nemsio or grib2 currently)

    Parameters
    ----------
    fname : string
        Filename to be opened

    Returns
    -------
    xarray.Dataset
        Description of returned object.

    """
    names, grib = _ensure_mfdataset_filenames(fname, engine="pynio")
    try:
        if grib:
            f = xr.open_mfdataset(names)
            f = _fix_grib2(f)
        else:
            raise ValueError
    except ValueError:
        print(
            """File format not recognized. Note that you must preprocess the
              files with nemsio2nc4 or fv3grib2nc4 available on github."""
        )
    return f


def open_mfdataset(fname):
    """Open a multiple files from fv3chem outputs (nemsio or grib2 currently)

    Parameters
    ----------
    fname : string
        Filenames to be opened

    Returns
    -------
    xarray.Dataset
        Description of returned object.

    """
    names, grib = _ensure_mfdataset_filenames(fname)
    try:
        if grib:
            f = xr.open_mfdataset(names, concat_dim="time", engine="pynio")
            f = _fix_grib2(f)
        else:
            raise ValueError
    except ValueError:
        print(
            """File format not recognized. Note that you must preprocess the
             files with nemsio2nc4 or fv3grib2nc4 available on github. Do not
             mix and match file types.  Ensure all are the same file format."""
        )
    return f


def _ensure_mfdataset_filenames(fname):
    """Checks if grib or nemsio data

    Parameters
    ----------
    fname : string or list of strings
        Description of parameter `fname`.

    Returns
    -------
    type
        Description of returned object.

    """
    from glob import glob
    from numpy import sort
    import six

    if isinstance(fname, six.string_types):
        names = sort(glob(fname))
    else:
        names = sort(fname)
    nemsios = [True for i in names if "nemsio" in i]
    gribs = [True for i in names if "grb2" in i or "grib2" in i]
    grib = False
    if len(gribs) >= 1:
        grib = True
    return names, grib


def _rename_func(f, rename_dict):
    """General renaming function for all file types

    Parameters
    ----------
    f : xarray.Dataset
        Description of parameter `f`.
    rename_dict : dict
        Description of parameter `rename_dict`.

    Returns
    -------
    xarray.Dataset
        Description of returned object.

    """
    final_dict = {}
    for i in f.data_vars.keys():
        if "midlayer" in i:
            rename_dict[i] = i.split("midlayer")[0]
    for i in rename_dict.keys():
        if i in f.data_vars.keys():
            final_dict[i] = rename_dict[i]
    f = f.rename(final_dict)


def _fix_grib2(f):
    """Internal function to rename and create latitude and longitude 2d coordinates

    Parameters
    ----------
    f : xarray.Dataset
        xarray.Dataset from a grib2 data file processed by fv3grib2nc4.

    Returns
    -------
    xarray.Dataset
        Description of returned object.

    """
    from numpy import meshgrid

    latitude = f.lat_0.values
    longitude = f.lon_0.values
    f["latitude"] = range(len(f.latitude))
    f["longitude"] = range(len(f.longitude))
    f = f.rename({"latitude": "y", "longitude": "x"})
    lon, lat = meshgrid(longitude, latitude)
    f["longitude"] = (("y", "x"), lon)
    f["latitude"] = (("y", "x"), lat)
    f = f.set_coords(["latitude", "longitude"])
    return f
