# verify is the main application.
from datetime import datetime

import matplotlib.pyplot as plt
from numpy import array, where
from pandas import DataFrame, concat

import mystats
import plots
from aqs import aqs
from cmaq import cmaq


class verify_aqs:
    def __init__(self):
        self.aqs = aqs()
        self.cmaq = cmaq()
        self.df = None
        self.cmaqo3 = None
        self.cmaqnox = None
        self.cmaqnoy = None
        self.cmaqpm25 = None
        self.cmaqso2 = None
        self.cmaqco = None
        self.cmaqisop = None
        self.cmaqbenz = None
        self.cmaqethane = None
        self.cmaqtol = None
        self.cmaqxyl = None
        self.cmaqpm10 = None
        self.df8hr = None


    def combine(self, interp='nearest', radius=12000. * 2, neighbors=10., weight_func=lambda r: 1 / r ** 2):
        """
        :return:
        """
        from interpolation import interp_to_obs_new
        from numpy import sort

        print '==============================================================='
        print 'Simulation Start Date: ', self.cmaq.conc.TSTEP.to_index()[0].strftime('%Y-%m-%d %H:%M')
        print 'Simulation End   Date: ', self.cmaq.conc.TSTEP.to_index()[-1].strftime('%Y-%m-%d %H:%M')
        print '===============================================================\n'
        self.aqs.df.dropna(subset=['Latitude','Longitude'],inplace=True)
        comparelist = sort(self.aqs.df.Species.unique())
        self.aqs.df.rename(columns={'GMT_Offset':'utcoffset'},inplace=True)
        print 'List of species available to compare:',comparelist
        dfs = []
        g = self.aqs.df.groupby('Species')
        for i in comparelist:
            if i == 'OZONE':
#                try:
                    print 'Interpolating Ozone:'
                    dfo3 = g.get_group(i)
                    fac = self.check_cmaq_units(param='O3', aqs_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='O3').compute() * fac
                    self.cmaqo3 = cmaq
                    dfo3 = interp_to_obs_new(cmaq, dfo3, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
#                    dfo3.Obs, dfo3.CMAQ = dfo3.Obs, dfo3.CMAQ
                    dfo3.Units = 'PPB'
                    dfs.append(dfo3)
#                except:
#                    pass
            elif i == 'PM2.5':
                try:
                    print 'Interpolating PM2.5:'
                    dfpm25 = g.get_group(i)
                    fac = self.check_cmaq_units(param='PM25', aqs_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='PM25').compute() * fac
                    dfpm25 = interp_to_obs_new(cmaq, dfpm25, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm25)
                except:
                    pass
            elif i == 'CO':
                try:
                    if 'CO' not in self.cmaq.keys:
                        pass
                    else:
                        print 'Interpolating CO:'
                        dfco = g.get_group(i)
                        fac = self.check_cmaq_units(param='CO', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='CO').compute() * fac
                        dfco = interp_to_obs_new(cmaq, dfco, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqco = cmaq
                        dfs.append(dfco)
                except:
                    pass
            elif i == 'NOY':
                # con = ('NOY' not in self.cmaq.keys()) |
                try:
                    if 'NOY' not in self.cmaq.keys:
                        pass
                    else:
                        print 'Interpolating NOY:'
                        dfnoy = g.get_group(i)
                        fac = self.check_cmaq_units(param='NOY', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NOY').compute() * fac
                        dfnoy = interp_to_obs_new(cmaq, dfnoy, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqnoy = cmaq
                        dfs.append(dfnoy)
                except:
                    pass
            elif i == 'SO2':
                try:
                    if 'SO2' not in self.cmaq.keys:
                        pass
                    else:
                        print 'Interpolating SO2'
                        dfso2 = g.get_group(i)
                        fac = self.check_cmaq_units(param='SO2', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='SO2').compute() * fac
                        dfso2 = interp_to_obs_new(cmaq, dfso2, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqso2 = cmaq
                        dfs.append(dfso2)
                except:
                    pass
            elif i == 'NOX':
                try:
                    if ('NO' not in self.cmaq.keys) | ('NO2' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating NOX:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='NOX', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NOX').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqno = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'NO':
                try:
                    if ('NO' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating NO:'
                        dfno = g.get_group(i)
                        fac = self.check_cmaq_units(param='NO', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NO').compute() * fac
                        dfno = interp_to_obs_new(cmaq, dfno, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqno = cmaq
                        dfs.append(dfno)
                except:
                    pass
            elif i == 'NO2':
                try:
                    if ('NO2' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating NO2:'
                        dfno2 = g.get_group(i)
                        fac = self.check_cmaq_units(param='NO2', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NO2').compute() * fac
                        dfno2 = interp_to_obs_new(cmaq, dfno2, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqno2 = cmaq
                        dfs.append(dfno2)
                except:
                    pass
            elif i == 'SO4f':
                try:
                    if ('PM25_SO4' in self.cmaq.keys) | ('ASO4J' in self.cmaq.keys) | ('ASO4I' in self.cmaq.keys):
                        print 'Interpolating PSO4:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='SO4f', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='SO4f').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqpso4 = cmaq
                        dfs.append(dfnox)
                    else:
                        pass
                except:
                    pass
            elif i == 'PM10':
                try:
                    #if ('PM_TOTAL' in self.cmaq.keys) | ('ASO4K' in self.cmaq.keys):
                    print 'Interpolating PM10:'
                    dfnox = g.get_group(i)
                    fac = self.check_cmaq_units(param='PM10', aqs_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='PM10').compute() * fac
                    dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                    self.cmaqpm10 = cmaq
                    dfs.append(dfnox)
                    #else:
                    #    pass
                except:
                    pass
            elif i == 'NO3f':
                try:
                    if ('PM25_NO3' in self.cmaq.keys) | ('ANO3J' in self.cmaq.keys) | ('ANO3I' in self.cmaq.keys):
                        print 'Interpolating PNO3:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='NO3f', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NO3F').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqpno3 = cmaq
                        dfs.append(dfnox)
                    else:
                        pass
                except:
                    pass
            elif i == 'ECf':
                try:
                    if ('PM25_EC' in self.cmaq.keys) | ('AECI' in self.cmaq.keys) | ('AECJ' in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating PEC:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='ECf', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='ECf').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqpno3 = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'OCf':
                try:
                    if ('APOCJ' in self.cmaq.keys):
                        print 'Interpolating OCf:'
                        dfpm = g.get_group(i)
                        fac = self.check_cmaq_units(param='OCf', improve_param=i)
                        cmaqvar = self.cmaq.get_surface_cmaqvar(param='OC').compute() * fac
                        dfpm = interp_to_obs_new(cmaqvar, dfpm, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqoc = cmaqvar
                        dfs.append(dfpm)
                except:
                    pass
            elif i == 'ETHANE':
                try:
                    if ('ETHA' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating Ethane:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='ETHA', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='ETHA').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqethane = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'BENZENE':
                try:
                    if ('BENZENE' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating BENZENE:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='BENZENE', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='BENZENE').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqbenz = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'TOLUENE':
                try:
                    if ('TOL' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating Toluene:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='TOL', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='TOL').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqtol = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'ISOPRENE':
                try:
                    if ('ISOP' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating Isoprene:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='ISOP', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='ISOP').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqisop = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'O-XYLENE':
                try:
                    if ('XYL' not in self.cmaq.keys):
                        print 'Variable \"XYL\" not in CMAQ keys'
                        pass
                    else:
                        print 'Interpolating Xylene'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='XYL', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='XYL').compute() * fac
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        self.cmaqxyl = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'WS':
                try:
                    if (self.cmaq.metcro2d is None) | ('WSPD10' not in self.cmaq.metcrokeys):
                        pass
                    else:
                        print 'Interpolating WS:'
                        dfnox = g.get_group(i)
                        cmaq = self.cmaq.get_metcro2d_cmaqvar(param='WSPD10')
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'TEMP':
                try:
                    if (self.cmaq.metcro2d is None) | ('TEMP2' not in self.cmaq.metcrokeys):
                        pass
                    else:
                        print 'Interpolating TEMP:'
                        dfnox = g.get_group(i)
                        cmaq = self.cmaq.get_metcro2d_cmaqvar(param='TEMP2')
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'WD':
                try:
                    if (self.cmaq.metcro2d is None) | ('WDIR10' not in self.cmaq.metcrokeys):
                        pass
                    else:
                        print 'Interpolating WD:'
                        dfnox = g.get_group(i)
                        cmaq = self.cmaq.get_metcro2d_cmaqvar(param='WDIR10')
                        dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius)
                        dfs.append(dfnox)
                except:
                    pass
        self.df = concat(dfs)
        print self.df.keys()
        self.df.dropna(subset=['Obs','CMAQ'],inplace=True)
        # if self.aqs.monitor_df is None:
        #     print '\n=========================================================================================='
        #     print 'Please load the Monitor Site Meta-Data to calculate 8hr Ozone: airnow.read_monitor_file()\n'
        #     print 'run: \'df = calc_aqs_8hr_max_calc()\''
        #     print '==========================================================================================\n'
        # else:
        #     print 'Calculating Daily 8 Hr Max Ozone....\n'
        #     self.df8hr = self.calc_aqs_8hr_max_calc()
        self.df.SCS = self.df.SCS.values.astype('int32')
        self.print_info()

    def combine_daily(self, interp='nearest', radius=12000. * 2, neighbors=10., weight_func=lambda r: 1 / r ** 2):
        """

        :param interp: Interpolation method 'nearest','guassian','
        :param radius:
        :param neighbors:
        :param weight_func:
        :return:
        """
        from numpy import sort
        import interpolation as interp
        print 'Acquiring Dates of CMAQ Simulation'
        print '==============================================================='
        self.cmaq.get_dates()
        print 'Simulation Start Date: ', self.cmaq.dates[0].strftime('%Y-%m-%d %H:%M')
        print 'Simulation End   Date: ', self.cmaq.dates[-1].strftime('%Y-%m-%d %H:%M')
        print '==============================================================='
        self.aqs.d_df = self.ensure_values_indomain(self.aqs.d_df)
        comparelist = sort(self.aqs.d_df.Species.unique())
        print 'List of species available to compare:',comparelist
        dfs = []
        g = self.aqs.d_df.groupby('Species')
        comparelist=['OZONE']
        for i in comparelist:
            if i == 'OZONE':
#                try:
                    print 'Interpolating Ozone:'
                    dfo3 = g.get_group(i)
                    print 'test'
#                    print dfo3
                    fac = 1000. #self.check_cmaq_units(param='O3', aqs_param=i)
                    print 'test2'
                    cmaq = self.cmaq.get_surface_cmaqvar(param='O3').compute() * fac
                    self.cmaqo3 = cmaq
                    print 'here',cmaq
                    dfo3 = interp.interp_to_pt_obs(cmaq, dfo3, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                    print 'here2'
                    dfo3.Obs, dfo3.CMAQ = dfo3.Obs, dfo3.CMAQ
                    dfo3.Units = 'PPB'
                    print dfo3
                    dfs.append(dfo3)
#                except:
#                    pass
            elif i == 'PM2.5':
                try:
                    print 'Interpolating PM2.5:'
                    dfpm25 = g.get_group(i)
                    fac = self.check_cmaq_units(param='PM25', aqs_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='PM25').compute() * fac
                    dfpm25 = interp.interp_to_pt_obs(cmaq, dfpm25, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm25)
                except:
                    pass
            elif i == 'CO':
                try:
                    if 'CO' not in self.cmaq.keys:
                        pass
                    else:
                        print 'Interpolating CO:'
                        dfco = g.get_group(i)
                        fac = self.check_cmaq_units(param='CO', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='CO').compute() * fac
                        dfco = interp.interp_to_pt_obs(cmaq, dfco, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqco = cmaq
                        dfs.append(dfco)
                except:
                    pass
            elif i == 'NOY':
                # con = ('NOY' not in self.cmaq.keys()) |
                try:
                    if 'NOY' not in self.cmaq.keys:
                        pass
                    else:
                        print 'Interpolating NOY:'
                        dfnoy = g.get_group(i)
                        fac = self.check_cmaq_units(param='NOY', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NOY').compute() * fac
                        dfnoy = interp.interp_to_pt_obs(cmaq, dfnoy, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqnoy = cmaq
                        dfs.append(dfnoy)
                except:
                    pass
            elif i == 'SO2':
                try:
                    if 'SO2' not in self.cmaq.keys:
                        pass
                    else:
                        print 'Interpolating SO2'
                        dfso2 = g.get_group(i)
                        fac = self.check_cmaq_units(param='SO2', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='SO2').compute() * fac
                        dfso2 = interp.interp_to_pt_obs(cmaq, dfso2, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqso2 = cmaq
                        dfs.append(dfso2)
                except:
                    pass
            elif i == 'NOX':
                try:
                    if ('NO' not in self.cmaq.keys) | ('NO2' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating NOX:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='NOX', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NOX').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqno = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'NO':
                try:
                    if ('NO' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating NO:'
                        dfno = g.get_group(i)
                        fac = self.check_cmaq_units(param='NO', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NO').compute() * fac
                        dfno = interp.interp_to_pt_obs(cmaq, dfno, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqno = cmaq
                        dfs.append(dfno)
                except:
                    pass
            elif i == 'NO2':
                try:
                    if ('NO2' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating NO2:'
                        dfno2 = g.get_group(i)
                        fac = self.check_cmaq_units(param='NO2', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NO2').compute() * fac
                        dfno2 = interp.interp_to_pt_obs(cmaq, dfno2, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqno2 = cmaq
                        dfs.append(dfno2)
                except:
                    pass
            elif i == 'SO4f':
                try:
                    if ('PM25_SO4' in self.cmaq.keys) | ('ASO4J' in self.cmaq.keys) | ('ASO4I' in self.cmaq.keys):
                        print 'Interpolating PSO4:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='SO4f', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='SO4f').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqpso4 = cmaq
                        dfs.append(dfnox)
                    else:
                        pass
                except:
                    pass
            elif i == 'PM10':
                try:
                    #if ('PM_TOTAL' in self.cmaq.keys) | ('ASO4K' in self.cmaq.keys):
                    print 'Interpolating PM10:'
                    dfnox = g.get_group(i)
                    fac = self.check_cmaq_units(param='PM10', aqs_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='PM10').compute() * fac
                    dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                    self.cmaqpm10 = cmaq
                    dfs.append(dfnox)
                    #else:
                    #    pass
                except:
                    pass
            elif i == 'NO3f':
                try:
                    if ('PM25_NO3' in self.cmaq.keys) | ('ANO3J' in self.cmaq.keys) | ('ANO3I' in self.cmaq.keys):
                        print 'Interpolating PNO3:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='NO3f', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='NO3F').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqpno3 = cmaq
                        dfs.append(dfnox)
                    else:
                        pass
                except:
                    pass
            elif i == 'ECf':
                try:
                    if ('PM25_EC' in self.cmaq.keys) | ('AECI' in self.cmaq.keys) | ('AECJ' in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating PEC:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='ECf', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='ECf').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqpno3 = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'OCf':
                try:
                    if ('APOCJ' in self.cmaq.keys):
                        print 'Interpolating OCf:'
                        dfpm = g.get_group(i)
                        fac = self.check_cmaq_units(param='OCf', improve_param=i)
                        cmaqvar = self.cmaq.get_surface_cmaqvar(param='OC').compute() * fac
                        dfpm = interp.interp_to_pt_obs(cmaqvar, dfpm, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqoc = cmaqvar
                        dfs.append(dfpm)
                except:
                    pass
            elif i == 'ETHANE':
                try:
                    if ('ETHA' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating Ethane:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='ETHA', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='ETHA').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqethane = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'BENZENE':
                try:
                    if ('BENZENE' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating BENZENE:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='BENZENE', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='BENZENE').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqbenz = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'TOLUENE':
                try:
                    if ('TOL' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating Toluene:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='TOL', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='TOL').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqtol = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'ISOPRENE':
                try:
                    if ('ISOP' not in self.cmaq.keys):
                        pass
                    else:
                        print 'Interpolating Isoprene:'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='ISOP', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='ISOP').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqisop = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'O-XYLENE':
                try:
                    if ('XYL' not in self.cmaq.keys):
                        print 'Variable \"XYL\" not in CMAQ keys'
                        pass
                    else:
                        print 'Interpolating Xylene'
                        dfnox = g.get_group(i)
                        fac = self.check_cmaq_units(param='XYL', aqs_param=i)
                        cmaq = self.cmaq.get_surface_cmaqvar(param='XYL').compute() * fac
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        self.cmaqxyl = cmaq
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'WS':
                try:
                    if (self.cmaq.metcro2d is None) | ('WSPD10' not in self.cmaq.metcrokeys):
                        pass
                    else:
                        print 'Interpolating WS:'
                        dfnox = g.get_group(i)
                        cmaq = self.cmaq.get_metcro2d_cmaqvar(param='WSPD10')
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'TEMP':
                try:
                    if (self.cmaq.metcro2d is None) | ('TEMP2' not in self.cmaq.metcrokeys):
                        pass
                    else:
                        print 'Interpolating TEMP:'
                        dfnox = g.get_group(i)
                        cmaq = self.cmaq.get_metcro2d_cmaqvar(param='TEMP2')
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        dfs.append(dfnox)
                except:
                    pass
            elif i == 'WD':
                try:
                    if (self.cmaq.metcro2d is None) | ('WDIR10' not in self.cmaq.metcrokeys):
                        pass
                    else:
                        print 'Interpolating WD:'
                        dfnox = g.get_group(i)
                        cmaq = self.cmaq.get_metcro2d_cmaqvar(param='WDIR10')
                        dfnox = interp.interp_to_pt_obs(cmaq, dfnox, self.cmaq.latitude.values,self.cmaq.longitude.values, radius=radius,daily=True)
                        dfs.append(dfnox)
                except:
                    pass
        print dfs
        self.df = concat(dfs)
        self.df.dropna(subset=['Obs','CMAQ'],inplace=True)
        if self.aqs.monitor_df is None:
            print '\n=========================================================================================='
            print 'Please load the Monitor Site Meta-Data to calculate 8hr Ozone: airnow.read_monitor_file()\n'
            print 'run: \'df = calc_aqs_8hr_max_calc()\''
            print '==========================================================================================\n'
        else:#        self.df.SCS = self.df.SCS.values.astype('int32')
            self.print_info()

    def print_info(self):
        print 'Ready to Compare!!!!!!!!!!!!!\n'
        print 'Available Functions:\n'
        print '    aqs_spatial(df, param=\'OZONE\', path='', region='', date=\'YYYY-MM-DD HH:MM\')'
        print '    aqs_spatial_8hr(df, path=\'cmaq-basemap-conus.p\', region=\'northeast\', date=\'YYYY-MM-DD\')'
        print '    compare_param(param=\'OZONE\', region=\'\', city=\'\',timeseries=False, scatter=False, pdfs=False,diffscatter=False, diffpdfs=False,timeseries_error=False)\n'
        print 'Species available to compare:'
        print '    ', self.df.Species.unique()
        print ''
        print 'Defined EPA Regions:'
        print '    ', self.df.EPA_region.unique()

    def compare_param(self, param='OZONE', site='', city='', state='', region='', epa_region='',timeseries=False, scatter=False,
                      pdfs=False, diffscatter=False, diffpdfs=False, timeseries_rmse=False, timeseries_mb=False,
                      taylordiagram=False, fig=None, label=None, footer=False, dia=None,marker=None):
        from numpy import NaN
        from utils import get_epa_location_df
        df2,title = get_epa_location_df(self.df.copy(),param,site=site,city=city,state=state,region=region,epa_region=epa_region)
        if timeseries:
            plots.timeseries_param(df2, title=title, label=label, fig=fig, footer=footer)
        if scatter:
            plots.scatter_param(df2, title=title, label=label, fig=fig, footer=footer)
        if pdfs:
            plots.kdeplots_param(df2, title=title, label=label, fig=fig, footer=footer)
        if diffscatter:
            plots.diffscatter_param(df2, title=title)
        if diffpdfs:
            plots.diffpdfs_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_rmse:
            plots.timeseries_rmse_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_mb:
            plots.timeseries_mb_param(df2, title=title, label=label, fig=fig, footer=footer)
        if taylordiagram:
            if marker is None: marker = 'o'
            if fig is None:
                dia = plots.taylordiagram(df2, label=label, dia=dia, addon=False,marker=marker)
                return dia
            else:
                dia = plots.taylordiagram(df2, label=label, dia=dia, addon=True,marker=marker)
                plt.legend()
                return dia

    def spatial(self, df, param='OZONE', date=None, imshow_args={},scatter_args={'s':20, 'edgecolors':'w','lw':.25},barb_args={},barbs=False,Obs=True):
        """
        :param param: Species Parameter: Acceptable Species: 'OZONE' 'PM2.5' 'CO' 'NOY' 'SO2' 'SO2' 'NOX'
        :param region: EPA Region: 'Northeast', 'Southeast', 'North_Central', 'South_Central', 'Rockies', 'Pacific'
        :param date: If not supplied will plot all time.  Put in 'YYYY-MM-DD HH:MM' for single time
        :return:
        """
        from numpy.ma import masked_less

        if Obs:
            try:
                g = df.groupby('Species')
                df2 = g.get_group(param)
            except KeyError:
                print param + ' Species not available!!!!'
                exit
        param = param.upper()
        if param == 'OZONE':
            cmaq = self.cmaqo3
        elif param == 'PM2.5':
            cmaq = self.cmaqpm25
        elif param == 'CO':
            cmaq = self.cmaqco
        elif param == 'NOY':
            cmaq = self.cmaqnoy
        elif param == 'SO2':
            cmaq = self.cmaqso2
        elif param == 'NOX':
            cmaq = self.cmaqnox
        elif param == 'ISOPRENE':
            cmaq = self.cmaqisop
        elif param == 'PM10':
            cmaq = self.cmaqpm10
        elif param == 'NO3f':
            cmaq = self.cmaqpno3
        elif param == 'SO4f':
            cmaq = self.cmaqpso4
        else:
            try:
                if not isinstance(self.cmaq.metcro2d, type(None)):
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param=param)
            except:
                pass
            try:
                cmaq = self.cmaq.get_surface_cmaqvar(param=param)
            except:
                exit
        m = self.cmaq.map
        if isinstance(cmaq, type(None)):
            print 'This parameter is not in the CMAQ file: ' + param
        else:
            dts = cmaq.TSTEP.to_index()
            if isinstance(date,type(None)):
                index = where(dts == dts[0])[0][0]
            else:
                index = where(dts.isin([date]))[0][0]
 #           print index                                                                                                                                                                                                   
 #           print cmaq[index,:,]                                                                                                                                                                                          
            f,ax,c,cmap,vmin,vmax = plots.make_spatial_plot2(cmaq[index, :, :].squeeze(), m,plotargs=imshow_args)
            if not isinstance(self.cmaq.metcro2d, type(None)) and barbs==False:
                ws = self.cmaq.metcro2d.variables['WSPD10'][index, :, :, :].squeeze()
                wdir = self.cmaq.metcro2d.variables['WDIR10'][index, :, :, :].squeeze()
                plots.wind_barbs(ws, wdir, self.cmaq.gridobj, m, **barbs_args)
            if Obs:
                scatter_args['vmin'] = vmin
                scatter_args['vmax'] = vmax
                scatter_args['cmap'] = cmap
                df2 = df2.loc[df2.datetime == dts[index]]
                plots.spatial_scatter(df2, m, plotargs=scatter_args)
                c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')

    def spatial_8hr(self, path='', region='', date='', xlim=[], ylim=[]):
        if not isinstance(self.df8hr, DataFrame):
            print '    Calculating 8 Hr Max Ozone '
            self.df8hr = self.calc_aqs_8hr_max_calc()
        print '    Creating Map'
        m = self.cmaq.choose_map(path, region)
        if date == '':
            for index, i in enumerate(self.df8hr.datetime_local.unique()):
                plots.eight_hr_spatial_scatter(self.df8hr, m, i.strftime('%Y-%m-%d'))
                c = plt.colorbar()
                c.set_label('8Hr Ozone Bias ( pbbV )')
                plt.title(i.strftime('%Y-%m-%d'))
                plt.tight_layout()
                if len(xlim) > 1:
                    plt.xlim([min(xlim), max(xlim)])
                    plt.ylim([min(ylim), max(ylim)])
        else:
            plots.eight_hr_spatial_scatter(self.df8hr, m, date=date)
            c = plt.colorbar()
            c.set_label('8Hr Ozone Bias ( pbbV )')
            plt.title(date)
            plt.tight_layout()

    @staticmethod
    def calc_stats2(df):
        mb = mystats.MB(df.Obs.values, df.cmaq.values)  # mean bias
        r2 = mystats.R2(df.Obs.values, df.cmaq.values)  # pearsonr ** 2
        ioa = mystats.IOA(df.Obs.values, df.cmaq.values)  # Index of Agreement
        rmse = mystats.RMSE(df.Obs.values, df.cmaq.values)
        return mb, r2, ioa, rmse

    def interp_to_aqs(self, cmaqvar, df, interp='nearest', r=12000., n=7, weight_func=lambda r: 1 / r ** 2):
        """
        This function interpolates variables (2d surface) in time to measurement sites

        :param cmaqvar: this is the CMAQ 3D variable
        :param df: The aqs
        :param interp: inteprolation method 'nearest',idw,guass
        :param r: radius of influence
        :param n: number of nearest neighbors to include
        :param weight_func: the user can set a defined method of interpolation
                            example:
                                lambda r: 1 / r ** 2
        :return: df
        """
        from pyresample import geometry, kd_tree
        from pandas import concat, Series, merge
        from numpy import append, empty, vstack, NaN
        from gc import collect
        dates = self.cmaq.dates[self.cmaq.indexdates]
        lat = self.cmaq.latitude
        lon = self.cmaq.longitude
        grid1 = geometry.GridDefinition(lons=lon, lats=lat)
        vals = array([], dtype=cmaqvar.dtype)
        date = array([], dtype='O')
        site = array([], dtype=df.SCS.dtype)
        print '    Interpolating using ' + interp + ' method'
        for i, j in enumerate(dates):
            con = df.datetime == j
            print j
            try:
                lats = df[con].Latitude.values
                lons = df[con].Longitude.values
                grid2 = geometry.GridDefinition(lons=vstack(lons), lats=vstack(lats))
                if interp.lower() == 'nearest':
                    val = kd_tree.resample_nearest(grid1, cmaqvar[i, :, :].squeeze(), grid2, radius_of_influence=r,
                                                   fill_value=NaN, nprocs=2).squeeze()
                elif interp.lower() == 'idw':
                    val = kd_tree.resample_custom(grid1, cmaqvar[i, :, :].squeeze(), grid2, radius_of_influence=r,
                                                  fill_value=NaN, neighbours=n, weight_funcs=weight_func,
                                                  nprocs=2).squeeze()
                elif interp.lower() == 'gauss':
                    val = kd_tree.resample_gauss(grid1, cmaqvar[i, :, :].squeeze(), grid2, radius_of_influence=r,
                                                 sigmas=r / 2., fill_value=NaN, neighbours=n, nprocs=2).squeeze()
                vals = append(vals, val)
                dd = empty(lons.shape[0], dtype=date.dtype)
                dd[:] = j
                date = append(date, dd)
                site = append(site, df[con].SCS.values)
                collect()
            except:
                pass

        vals = Series(vals)
        date = Series(date)
        site = Series(site)
        dfs = concat([vals, date, site], axis=1, keys=['CMAQ', 'datetime', 'SCS'])
        df = merge(df, dfs, how='left', on=['SCS', 'datetime'])

        return df

    def interp_to_improve_unknown(self, cmaqvar, df, varname):
        from scipy.interpolate import griddata
        dates = self.cmaq.dates[self.cmaq.indexdates]
        lat = self.cmaq.latitude
        lon = self.cmaq.longitude

        con = df.datetime == self.cmaq.dates[self.cmaq.indexdates][0]
        new = df[con]
        print '   Interpolating values to AQS Sites, Date : ', self.cmaq.dates[self.cmaq.indexdates][0].strftime(
            '%B %d %Y   %H utc')
        cmaq_val = DataFrame(
            griddata((lon.flatten(), lat.flatten()), cmaqvar[self.cmaq.indexdates[0], :, :].flatten(),
                     (new.Longitude.values, new.Latitude.values), method='nearest'),
            columns=[varname], index=new.index)
        new = new.join(cmaq_val)
        for i, j in enumerate(self.cmaq.dates[self.cmaq.indexdates][1:]):
            print '   Interpolating values to AQS Sites, Date : ', j.strftime('%B %d %Y %H utc')
            con = df.datetime == j
            newt = df[con]
            cmaq_val = DataFrame(griddata((lon.flatten(), lat.flatten()), cmaqvar[i + 1, :, :].flatten(),
                                          (newt.Longitude.values, newt.Latitude.values), method='nearest'),
                                 columns=[varname], index=newt.index)
            newt = newt.join(cmaq_val)
            new = new.append(newt)
        return new

    def check_cmaq_units(self, param='O3', aqs_param='OZONE'):
        aunit = self.aqs.df[self.aqs.df.Species == aqs_param].Units.unique()[0]
        if aunit == 'UG/M3'.lower():
            fac = 1.
        elif aunit == 'PPB'.lower():
            fac = 1000.
        elif aunit == 'ppbC':
            fac = 1000.
            if aqs_param == 'ISOPRENE':
                fac *= 5.
            elif aqs_param == 'BENZENE':
                fac *= 6.
            elif aqs_param == 'TOLUENE':
                fac *= 7.
            elif aqs_param == 'O-XYLENE':
                fac *= 8.
        else:
            fac = 1.
        return fac

    @staticmethod
    def calc_stats(pandasobj):
        mb = mystats.MB(pandasobj['Obs_value'].values, pandasobj['CMAQ'].values)  # mean bias
        mdnb = mystats.AC(pandasobj['Obs_value'].values, pandasobj['CMAQ'].values)  # median bias
        fe = mystats.FE(pandasobj['Obs_value'].values, pandasobj['CMAQ'].values)  # fractional error
        r2 = mystats.R2(pandasobj['Obs_value'].values, pandasobj['CMAQ'].values)  # pearsonr ** 2
        d1 = mystats.d1(pandasobj['Obs_value'].values, pandasobj['CMAQ'].values)  # modifed index of agreement
        e1 = mystats.E1(pandasobj['Obs_value'].values,
                        pandasobj['CMAQ'].values)  # Modified Coefficient of Efficiency, E1
        ioa = mystats.IOA(pandasobj['Obs_value'].values, pandasobj['CMAQ'].values)  # Index of Agreement
        rmse = mystats.RMSE(pandasobj['Obs_value'].values, pandasobj['CMAQ'].values)
        return mb, mdnb, fe, r2, d1, e1, ioa, rmse

    def ensure_values_indomain(self,df):
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        con = ((df.Latitude.values > lat.min()) & (df.Latitude.values < lat.max()) & (
            df.Longitude.values > lon.min()) & (df.Longitude.values < lon.max()))
        return df[con].copy()

    def calc_aqs_8hr_max_calc(self):
        from numpy import unique
        r = self.df.groupby('Species').get_group('OZONE')
        r.index = r.datetime_local
        vals, index = unique(r.SCS.values, return_index=True)
        g = r.groupby('SCS')['Obs', 'CMAQ', 'Latitude', 'Longitude']
        m = g.rolling(8, center=True, win_type='boxcar').mean()
        q = m.reset_index(level=0)
        # q.index = self.df.groupby('Species').get_group('OZONE').datetime_local
        k = q.groupby('SCS').resample('1D').max()
        kk = k.reset_index(level=1)
        kkk = kk.reset_index(drop='SCS').dropna()
        kkk = self.aqs.add_metro_metadata2(kkk)
#re        kkk['Region'] = ''
        kkk['Species'] = 'OZONE'
#        kkk['State_Name'] = ''
        kkk['Units'] = 'PPB'
        # for i, j in enumerate(vals):
        #     kkk.loc[kkk.SCS == j, 'Region'] = r.Region.values[index[i]]
        #     kkk.loc[kkk.SCS == j, 'State_Name'] = r.State_Name.values[index[i]]
        #     kkk.loc[kkk.SCS == j, 'County_Name'] = r.County_Name.values[index[i]]

        return kkk
