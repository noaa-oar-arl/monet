# This is the driver for all verify objects

# this is done to make creating verifications easier

def vaqs(concpath='', gridcro='', met2dpath='', datapath='', combine=True, radius=12000., neighbors=5,
               interp='gauss', species='all'):
    """

    :param concpath: The path to the concetration file / files: example: 'CMAQ/aqm.*.aconc.ncf'
    :param gridcro: Path to the GRIDCRO2D file: example: 'aqm.t12z.gridcro2d.ncf'
    :param met2dpath: The path to the metcro2d file / files: example: 'CMAQ/aqm.*.metcro2d.ncf'
    :param datapath: Path to the observational data: example 'DATA' or 'DATA'
    :param combine: True or false.  if False it will not interpolate CMAQ results to Observations
    :param radius: 'used for interpolation.  Radius is in meters'
    :param neighbors: 'number of neighbors used in interpolation
    :param interp: 'interpolation method.  Valid answers are: 'nearest', 'idw', 'gauss'
                    note.  if idw you must supply a weight_func.  Example: weight_func=lambda r: 1/r**2
    :param species: defaults to all available data,  Can enter 'PM' for just pm10 pm25 and speciated pm
    :return: verify_aqs() object
    """
    from verify_aqs import verify_aqs
    va = verify_aqs()
    va.cmaq.open_cmaq(file=concpath)
    va.cmaq.set_gridcro2d(filename=gridcro)
    va.cmaq.get_dates()
    if met2dpath != '':
        va.cmaq.open_metcro2d(met2dpath)
    va.aqs.datadir = datapath
    va.aqs.load_all_hourly_data(va.cmaq.dates, datasets=species)
    va.aqs.monitor_file = __file__[:-15] + '/data/monitoring_site_locations.dat'

    va.aqs.read_monitor_file()
    if combine:
        va.combine(interp=interp, radius=radius, neighbors=neighbors)
    return va


def vairnow(concpath='', gridcro='', met2dpath='', datapath='', combine=True, radius=12000., neighbors=5,
                  interp='gauss', airnowoutput='', user='', passw=''):
    """

    :param concpath: The path to the concetration file / files: example: 'CMAQ/aqm.*.aconc.ncf'
    :param gridcro: Path to the GRIDCRO2D file: example: 'aqm.t12z.gridcro2d.ncf'
    :param met2dpath: The path to the metcro2d file / files: example: 'CMAQ/aqm.*.metcro2d.ncf'
    :param datapath: Path to the observational data: example 'DATA' or 'DATA'
    :param combine: True or false.  if False it will not interpolate CMAQ results to Observations
    :param radius: 'used for interpolation.  Radius is in meters'
    :param neighbors: 'number of neighbors used in interpolation
    :param interp: 'interpolation method.  Valid answers are: 'nearest', 'idw', 'gauss'
                    note.  if idw you must supply a weight_func.  Example: weight_func=lambda r: 1/r**2
    :return: verify_aqs() object
    """
    from verify_airnow import verify_airnow
    va = verify_airnow()
    va.cmaq.open_cmaq(file=concpath)
    va.airnow.username = user
    va.airnow.password = passw
    va.cmaq.set_gridcro2d(gridcro)
    va.cmaq.get_dates()
    va.airnow.dates = va.cmaq.dates[va.cmaq.indexdates]
    if met2dpath != '':
        va.cmaq.open_metcro2d(met2dpath)
    if datapath[-4:] == '.hdf':
        from pandas import read_hdf
        va.airnow.df = read_hdf(datapath)
    else:
        va.airnow.download_hourly_files(path=datapath)
        va.airnow.aggragate_files(airnowoutput)
        va.airnow.monitor_file = __file__[:-15] + '/data/monitoring_site_locations.dat'
        va.airnow.read_monitor_file()
    va.airnow.datadir = datapath
    if combine:
        va.combine(interp=interp, radius=radius, neighbors=neighbors)
    return va


def vimprove(concpath='', gridcro='', met2dpath='', datapath='', combine=True, radius=12000., neighbors=5,
                   interp='gauss'):
    """

    :param concpath: The path to the concetration file / files: example: 'CMAQ/aqm.*.aconc.ncf'
    :param gridcro: Path to the GRIDCRO2D file: example: 'aqm.t12z.gridcro2d.ncf'
    :param met2dpath: The path to the metcro2d file / files: example: 'CMAQ/aqm.*.metcro2d.ncf'
    :param datapath: Path to the observational data: example 'DATA' or 'DATA'
    :param combine: True or false.  if False it will not interpolate CMAQ results to Observations
    :param radius: 'used for interpolation.  Radius is in meters'
    :param neighbors: 'number of neighbors used in interpolation
    :param interp: 'interpolation method.  Valid answers are: 'nearest', 'idw', 'gauss'
                    note.  if idw you must supply a weight_func.  Example: weight_func=lambda r: 1/r**2
    :return: verify_aqs() object
    """
    from verify_improve import verify_improve
    va = verify_improve()
    va.cmaq.open_cmaq(file=concpath)
    va.cmaq.set_gridcro2d(gridcro)
    va.cmaq.get_dates()
    va.improve.dates = va.cmaq.dates
    if met2dpath != '':
        va.cmaq.open_metcro2d(met2dpath)
    if datapath[-4:] == '.hdf':
        print 'Reading file: ' + datapath
        from pandas import read_hdf
        va.improve.df = read_hdf(datapath)
    else:
        va.improve.open(datapath)
    if combine:
        va.combine(interp=interp, radius=radius, neighbors=neighbors)
    return va
