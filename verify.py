# This is the driver for all verify objects

# this is done to make creating verifications easier


def aqs_verify(concpath='', gridcro='', met2dpath='', datapath='', combine=True, radius=12000. * 1.5, neighbors=10,
               interp='gauss'):
    from verify_aqs import verify_aqs
    va = verify_aqs()
    va.cmaq.open(file=concpath)
    va.cmaq.set_gridcro2d(gridcro)
    va.cmaq.get_dates()
    if met2dpath != '':
        va.cmaq.open_metcro2d(met2dpath)
    va.aqs.datadir = datapath
    va.aqs.load_all_hourly_data(va.cmaq.dates)
    va.aqs.monitor_file = '/data/aqf/barryb/monitoring_site_locations.dat'
    va.aqs.read_monitor_file()
    if combine:
        va.combine(interp=interp, radius=radius, neighbors=neighbors)
    return va


def airnow_verify(concpath='', gridcro='', met2dpath='', datapath='', combine=True, radius=12000. * 1.5, neighbors=10,
                  interp='gauss', airnowoutput=''):
    from verify_airnow import verify_airnow
    va = verify_airnow()
    va.cmaq.open(file=concpath)
    va.cmaq.set_gridcro2d(gridcro)
    va.cmaq.get_dates()
    va.airnow.dates = va.cmaq.dates
    if met2dpath != '':
        va.cmaq.open_metcro2d(met2dpath)
    if datapath[-4:] == '.hdf':
        from pandas import read_hdf
        va.airnow.df = read_hdf(datapath)
    else:
        va.airnow.download_hourly_files(datapath)
        va.airnow.aggragate_files(airnowoutput)
    va.airnow.datadir = datapath
    va.airnow.monitor_file = '/data/aqf/barryb/monitoring_site_locations.dat'
    va.airnow.read_monitor_file()
    if combine:
        va.combine(interp=interp, radius=radius, neighbors=neighbors)
    return va


def improve_verify(concpath='', gridcro='', met2dpath='', datapath='', combine=True, radius=12000. * 1.5, neighbors=10,
                   interp='gauss'):
    from verify_improve import verify_improve
    va = verify_improve()
    va.cmaq.open(file=concpath)
    va.cmaq.set_gridcro2d(gridcro)
    va.cmaq.get_dates()
    va.improve.dates = va.cmaq.dates
    if met2dpath != '':
        va.cmaq.open_metcro2d(met2dpath)
    if datapath[-4:] == '.hdf':
        from pandas import read_hdf
        va.improve.df = read_hdf(datapath)
    else:
        va.improve.open(datapath)
    if combine:
        va.combine(interp=interp, radius=radius, neighbors=neighbors)
    return va
