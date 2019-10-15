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
    names, nemsio, grib = _ensure_mfdataset_filenames(fname)
    try:
        if nemsio:
            f = xr.open_mfdataset(names)
            f = _fix_nemsio(f)
            f = _fix_time_nemsio(f, names[0])
            # f['geoht'] = _calc_nemsio_hgt(f)
        elif grib:
            f = xr.open_mfdataset(names)
            f = _fix_grib2(f)
        else:
            raise ValueError
    except ValueError:
        print('''File format not recognized. Note that you must preprocess the
              files with nemsio2nc4 or fv3grib2nc4 available on github.''')
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
    names, nemsio, grib = _ensure_mfdataset_filenames(fname)
    try:
        if nemsio:
            f = xr.open_mfdataset(names, concat_dim='time')
            f = _fix_nemsio(f)
            f = _fix_time_nemsio(f, names)
            # f['geoht'] = _calc_nemsio_hgt(f)
        elif grib:
            f = xr.open_mfdataset(names, concat_dim='time')
            f = _fix_grib2(f)
        else:
            raise ValueError
    except ValueError:
        print('''File format not recognized. Note that you must preprocess the
             files with nemsio2nc4 or fv3grib2nc4 available on github. Do not
             mix and match file types.  Ensure all are the same file format.'''
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
    nemsios = [True for i in names if 'nemsio' in i]
    gribs = [True for i in names if 'grb2' in i or 'grib2' in i]
    grib = False
    nemsio = False
    if len(nemsios) >= 1:
        nemsio = True
    elif len(gribs) >= 1:
        grib = True
    return names, nemsio, grib


def _fix_time_nemsio(f, fname):
    """Short summary.

    Parameters
    ----------
    f : type
        Description of parameter `f`.
    fname : type
        Description of parameter `fname`.

    Returns
    -------
    type
        Description of returned object.

    """
    from pandas import Timedelta, to_datetime
    time = None
    print(fname)
    if len(f.time) > 1:
        tarray = []
        for t, fn in zip(f.time.to_index(), fname):
            hour = int([i for i in fn.split('.') if 'atmf' in i][0][-3:])
            tdelta = Timedelta(hour, unit='h')
            tarray.append(t + tdelta)
        time = to_datetime(tarray)
    else:
        hour = int([i for i in fname.split('.') if 'atmf' in i][0][-3:])
        tdelta = Timedelta(hour, unit='h')
        time = f.time.to_index() + tdelta
    f['time'] = time
    return f


def _fix_nemsio(f):
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
    # f = _rename_func(f, rename_dict)
    lat = f.lat.values
    lon = f.lon.values
    lon, lat = meshgrid(lon, lat)
    f = f.rename({'lat': 'y', 'lon': 'x', 'lev': 'z'})
    f['longitude'] = (('y', 'x'), lon)
    f['latitude'] = (('y', 'x'), lat)
    f = f.set_coords(['latitude', 'longitude'])
    f = _rename_func(f, {})
    try:
        f['geohgt'] = _calc_nemsio_hgt(f)
    except:
        print('geoht calculation not completed')
    # try:
    #     from pyresample import utils
    #     f['longitude'] = utils.wrap_longitudes(f.longitude)
    # except ImportError:
    #     print(
    #         'Users may need to wrap longitude values for plotting over 0 degrees'
    #     )
    return f


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
        if 'midlayer' in i:
            rename_dict[i] = i.split('midlayer')[0]
    for i in rename_dict.keys():
        if i in f.data_vars.keys():
            final_dict[i] = rename_dict[i]
    f = f.rename(final_dict)
    try:
        f = f.rename({'pp25': 'pm25', 'pp10': 'pm10'})
    except ValueError:
        print('PM25 and PM10 are not available')
    return f


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
    rename_dict = {
        'AOTK_aerosol_EQ_Total_Aerosol_aerosol_size_LT_2eM05_aerosol_wavelength_GE_5D45eM07_LE_5D65eM07_entireatmosphere':
        'pm25aod550',
        'AOTK_chemical_Total_Aerosol_aerosol_size__2e_05_aerosol_wavelength_5_45e_07_5_65e_07_entireatmosphere':
        'pm25aod550',
        'AOTK_aerosol_EQ_Dust_Dry_aerosol_size_LT_2eM05_aerosol_wavelength_GE_5D45eM07_LE_5D65eM07_entireatmosphere':
        'dust25aod550',
        'AOTK_chemical_Dust_Dry_aerosol_size__2e_05_aerosol_wavelength_5_45e_07_5_65e_07_entireatmosphere':
        'dust25aod550',
        'AOTK_aerosol_EQ_Sea_Salt_Dry_aerosol_size_LT_2eM05_aerosol_wavelength_GE_5D45eM07_LE_5D65eM07_entireatmosphere':
        'salt25aod550',
        'AOTK_chemical_Sea_Salt_Dry_aerosol_size__2e_05_aerosol_wavelength_5_45e_07_5_65e_07_entireatmosphere':
        'salt25aod550',
        'AOTK_aerosol_EQ_Sulphate_Dry_aerosol_size_LT_2eM05_aerosol_wavelength_GE_5D45eM07_LE_5D65eM07_entireatmosphere':
        'sulf25aod550',
        'AOTK_chemical_Sulphate_Dry_aerosol_size__2e_05_aerosol_wavelength_5_45e_07_5_65e_07_entireatmosphere':
        'sulf25aod550',
        'AOTK_aerosol_EQ_Particulate_Organic_Matter_Dry_aerosol_size_LT_2eM05_aerosol_wavelength_GE_5D45eM07_LE_5D65eM07_entireatmosphere':
        'oc25aod550',
        'AOTK_chemical_Particulate_Organic_Matter_Dry_aerosol_size__2e_05_aerosol_wavelength_5_45e_07_5_65e_07_entireatmosphere':
        'oc25aod550',
        'AOTK_aerosol_EQ_Black_Carbon_Dry_aerosol_size_LT_2eM05_aerosol_wavelength_GE_5D45eM07_LE_5D65eM07_entireatmosphere':
        'bc25aod550',
        'AOTK_chemical_Black_Carbon_Dry_aerosol_size__2e_05_aerosol_wavelength_5_45e_07_5_65e_07_entireatmosphere':
        'bc25aod550',
        'COLMD_aerosol_EQ_Total_Aerosol_aerosol_size_LT_1eM05_entireatmosphere':
        'tc_aero10',
        'COLMD_chemical_Total_Aerosol_aerosol_size__1e_05_aerosol_wavelength_____code_table_4_91_255_entireatmosphere':
        'tc_aero10',
        'COLMD_aerosol_EQ_Total_Aerosol_aerosol_size_LT_2D5eM06_entireatmosphere':
        'tc_aero25',
        'COLMD_chemical_Total_Aerosol_aerosol_size__2_5e_06_aerosol_wavelength_____code_table_4_91_255_entireatmosphere':
        'tc_aero25',
        'COLMD_aerosol_EQ_Dust_Dry_aerosol_size_LT_2D5eM06_entireatmosphere':
        'tc_dust25',
        'COLMD_chemical_Dust_Dry_aerosol_size__2_5e_06_aerosol_wavelength_____code_table_4_91_255_entireatmosphere':
        'tc_dust25',
        'COLMD_aerosol_EQ_Sea_Salt_Dry_aerosol_size_LT_2D5eM06_entireatmosphere':
        'tc_salt25',
        'COLMD_chemical_Sea_Salt_Dry_aerosol_size__2_5e_06_aerosol_wavelength_____code_table_4_91_255_entireatmosphere':
        'tc_salt25',
        'COLMD_aerosol_EQ_Black_Carbon_Dry_aerosol_size_LT_2D36eM08_entireatmosphere':
        'tc_bc236',
        'COLMD_chemical_Black_Carbon_Dry_aerosol_size__2_36e_08_aerosol_wavelength_____code_table_4_91_255_entireatmosphere':
        'tc_bc236',
        'COLMD_aerosol_EQ_Particulate_Organic_Matter_Dry_aerosol_size_LT_4D24eM08_entireatmosphere':
        'tc_oc424',
        'COLMD_chemical_Particulate_Organic_Matter_Dry_aerosol_size__4_24e_08_aerosol_wavelength_____code_table_4_91_255_entireatmosphere':
        'tc_oc424',
        'COLMD_aerosol_EQ_Sulphate_Dry_aerosol_size_LT_2D5eM06_entireatmosphere':
        'tc_sulf25',
        'COLMD_chemical_Sulphate_Dry_aerosol_size__2_5e_06_aerosol_wavelength_____code_table_4_91_255_entireatmosphere':
        'tc_sulf25',
        'PMTF_chemical_Dust_Dry_aerosol_size__2_5e_06_aerosol_wavelength_____code_table_4_91_255_surface':
        'sfc_dust25',
        'PMTF_chemical_Sea_Salt_Dry_aerosol_size__2_5e_06_aerosol_wavelength_____code_table_4_91_255_surface':
        'sfc_salt25',
        'PMTF_chemical_Total_Aerosol_aerosol_size__2_5e_06_aerosol_wavelength_____code_table_4_91_255_surface':
        'sfc_pm25',
        'PMTF_aerosol_EQ_Total_Aerosol_aerosol_size_LT_2D5eM06_surface':
        'sfc_pm25',
        'PMTC_aerosol_EQ_Total_Aerosol_aerosol_size_LT_1eM05_surface':
        'sfc_pm10',
        'PMTF_aerosol_EQ_Sea_Salt_Dry_aerosol_size_LT_2D5eM06_surface':
        'sfc_salt25',
        'PMTF_aerosol_EQ_Dust_Dry_aerosol_size_LT_2D5eM06_surface':
        'sfc_dust25',
        'PMTF_chemical_Dust_Dry_aerosol_size___2e_07__2e_06_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'dustmr1p1',
        'PMTF_chemical_Dust_Dry_aerosol_size___2e_06__3_6e_06_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'dustmr2p5',
        'PMTC_chemical_Dust_Dry_aerosol_size___3_6e_06__6e_06_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'dustmr4p8',
        'PMTC_chemical_Dust_Dry_aerosol_size___6e_06__1_2e_05_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'dustmr9p0',
        'PMTC_chemical_Dust_Dry_aerosol_size___1_2e_05__2e_05_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'dustmr16p0',
        'PMTF_chemical_Sea_Salt_Dry_aerosol_size___2e_07__1e_06_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'saltmr0p6',
        'PMTC_chemical_Sea_Salt_Dry_aerosol_size___1e_06__3e_06_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'saltmr2p0',
        'PMTC_chemical_Sea_Salt_Dry_aerosol_size___3e_06__1e_05_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'saltmr6p5',
        'PMTC_chemical_Sea_Salt_Dry_aerosol_size___1e_05__2e_05_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'saltmr10p5',
        'PMTF_chemical_Sulphate_Dry_aerosol_size__1_39e_07_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'sulfmr1p36',
        'PMTF_chemical_chemical_62016_aerosol_size__4_24e_08_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'aero1_mr0p0424',
        'PMTF_chemical_chemical_62015_aerosol_size__4_24e_08_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'aero2_mr0p0424',
        'PMTF_chemical_chemical_62014_aerosol_size__2_36e_08_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'aero1_mr0p0236',
        'PMTF_chemical_chemical_62013_aerosol_size__2_36e_08_aerosol_wavelength_____code_table_4_91_255_1hybridlevel':
        'aero2_mr0p0236',
        'level':
        'z'
    }
    latitude = f.latitude.values
    longitude = f.longitude.values
    f['latitude'] = range(len(f.latitude))
    f['longitude'] = range(len(f.longitude))
    f = _rename_func(f, rename_dict)
    f = f.rename({'latitude': 'y', 'longitude': 'x'})
    lon, lat = meshgrid(longitude, latitude)
    f['longitude'] = (('y', 'x'), lon)
    f['latitude'] = (('y', 'x'), lat)
    f = f.set_coords(['latitude', 'longitude'])
    # try:
    #     from pyresample import utils
    #     f['longitude'] = utils.wrap_longitudes(f.longitude)
    # except ImportError:
    #     print(
    #         'Users may need to wrap longitude values for plotting over 0 degrees'
    #     )
    return f


def _calc_nemsio_hgt(f):
    """Calculates the geopotential height from the variables hgtsfc and delz.

    Parameters
    ----------
    f : xarray.DataSet
        the NEMSIO opened object.  Can be lazily loaded.

    Returns
    -------
    xr.DataArray
        Geoptential height with varialbes, coordinates and variable attributes.

    """
    sfc = f.hgtsfc
    dz = f.delz
    z = dz + sfc
    z = z.rolling(z=len(f.z), min_periods=1).sum()
    z.name = 'geohgt'
    z.attrs['long_name'] = 'Geopotential Height'
    z.attrs['units'] = 'm'
    return z


def calc_nemsio_pressure(dset):
    """Calculate the pressure in mb form a nemsio file xarray.Dataset.
    Currently a slow loop.  Looking for a way to speed this up.  Recommend not
    using this for large datasets.  First subset your data and then use to not
    run out of memory

    Parameters
    ----------
    dset : xarray.Dataset
        nemsio dataset opened

    Returns
    -------
    xarray.DataArray
        Description of returned object.

    """
    # sfc = dset.pressfc.load() / 100.
    # dpres = dset.dpres.load() / 100. * -1.
    sfc = dset.pressfc / 100.
    dpres = dset.dpres / 100. * -1.
    dpres[:, 0, :, :] = sfc + dpres[:, 0, :, :]
    pres = dpres.rolling(z=len(dset.z), min_periods=1).sum()
    pres.name = 'press'
    pres.attrs['units'] = 'mb'
    pres.attrs['long_name'] = 'Mid Layer Pressure'
    return pres
