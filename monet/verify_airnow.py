# verify is the main application.
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
from numpy import array, where

import mystats
import plots
from airnow import airnow
from cmaq import cmaq


class verify_airnow:
    def __init__(self):
        self.airnow = airnow()
        self.cmaq = cmaq()
        self.se_states = array(
            ['Alabama', 'Florida', 'Georgia', 'Mississippi', 'North Carolina', 'South Carolina', 'Tennessee',
             'Virginia', 'West Virginia'], dtype='|S14')
        self.ne_states = array(['Connecticut', 'Delaware', 'District Of Columbia', 'Maine', 'Maryland', 'Massachusetts',
                                'New Hampshire', 'New Jersey', 'New York', 'Pennsylvania', 'Rhode Island', 'Vermont'],
                               dtype='|S20')
        self.nc_states = array(
            ['Illinois', 'Indiana', 'Iowa', 'Kentucky', 'Michigan', 'Minnesota', 'Missouri', 'Ohio', 'Wisconsin'],
            dtype='|S9')
        self.sc_states = array(['Arkansas', 'Louisiana', 'Oklahoma', 'Texas'], dtype='|S9')
        self.r_states = array(['Arizona', 'Colorado', 'Idaho', 'Kansas', 'Montana', 'Nebraska', 'Nevada', 'New Mexico',
                               'North Dakota', 'South Dakota', 'Utah', 'Wyoming'], dtype='|S12')
        self.p_states = array(['California', 'Oregon', 'Washington'], dtype='|S10')
        self.df = None
        self.cmaqo3 = None
        self.cmaqnox = None
        self.cmaqnoy = None
        self.cmaqpm25 = None
        self.cmaqpm10 = None
        self.cmaqso2 = None
        self.cmaqco = None
        self.cmaqno = None
        self.cmaqno2 = None
        self.df8hr = None

    def combine(self, interp='nearest', radius=12000. * 2, neighbors=10., weight_func=lambda r: 1 / r ** 2):
        ###3 different interpolations
        ###interp = 'nearest' for nearest neightbor interpolation
        ###interp = 'idw' for nearest neight interpolation
        ###interp = 'gauss' for gaussian weighting 
        ###   if idw is selected you need to provide the weighting function. something like:
        ###
        ###   weight_func = lambda r: 1/r**2 for inverse distance squared
        ###
        ###   weight_func = lambda r: 1/r    for inverse distance squared
        ###   
        # get the lats and lons for CMAQ
        lat = self.cmaq.latitude
        lon = self.cmaq.longitude

        # get the CMAQ dates
        print 'Acquiring Dates of CMAQ Simulation'
        print '==============================================================='
        self.cmaq.get_dates()
        print 'Simulation Start Date: ', self.cmaq.dates[0].strftime('%Y-%m-%d %H:%M')
        print 'Simulation End   Date: ', self.cmaq.dates[-1].strftime('%Y-%m-%d %H:%M')
        print '===============================================================\n'
        if self.cmaq.metcro2d is None:
            print 'METCRO2D file not loaded.  To include MET variables please load self.cmaq.open_metcro2d(\'filename\')\n'
        self.ensure_values_indomain()
        comparelist = self.airnow.df.Species.unique()
        g = self.airnow.df.groupby('Species')
        dfs = []
        for i in comparelist:
            if i == 'OZONE':
                try:
                    print 'Interpolating Ozone:'
                    dfo3 = g.get_group(i)
                    fac = self.check_cmaq_units(param='O3', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='O3') * fac
                    self.cmaqo3 = cmaq
                    dfo3 = self.interp_to_airnow(cmaq, dfo3, interp=interp, r=radius, weight_func=weight_func)
                    dfs.append(dfo3)
                except:
                    pass
            elif i == 'PM2.5':
                try:
                    print 'Interpolating PM2.5:'
                    dfpm25 = g.get_group(i)
                    fac = self.check_cmaq_units(param='PM25', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='PM25') * fac
                    dfpm25 = self.interp_to_airnow(cmaq, dfpm25, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm25)
                except:
                    pass
            elif i == 'PM10':
                try:
                    print 'Interpolating PM10:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='PM10', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='PM10') * fac
                    dfpm = self.interp_to_airnow(cmaq, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqpm10 = cmaq
                    dfs.append(dfpm)
                except:
                    pass
            elif i == 'CO':
                try:
                    print 'Interpolating CO:'
                    dfco = g.get_group(i)
                    fac = self.check_cmaq_units(param='CO', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='CO') * fac
                    dfco = self.interp_to_airnow(cmaq, dfco, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqco = cmaq
                    dfs.append(dfco)
                except:
                    pass
            elif i == 'NOY':
                try:
                    print 'Interpolating NOY:'
                    dfnoy = g.get_group(i)
                    fac = self.check_cmaq_units(param='NOY', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NOY') * fac
                    dfnoy = self.interp_to_airnow(cmaq, dfnoy, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqnoy = cmaq
                    dfs.append(dfnoy)
                except:
                    pass
            elif i == 'NO':
                try:
                    print 'Interpolating NO:'
                    dfnoy = g.get_group(i)
                    fac = self.check_cmaq_units(param='NO', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NO') * fac
                    dfnoy = self.interp_to_airnow(cmaq, dfnoy, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqno = cmaq
                    dfs.append(dfnoy)
                except:
                    pass
            elif i == 'NO2':
                try:
                    print 'Interpolating NO2:'
                    dfnoy = g.get_group(i)
                    fac = self.check_cmaq_units(param='NO2', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NO2') * fac
                    dfnoy = self.interp_to_airnow(cmaq, dfnoy, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqno2 = cmaq
                    dfs.append(dfnoy)
                except:
                    pass
            elif i == 'SO2':
                try:
                    print 'Interpolating SO2'
                    dfso2 = g.get_group(i)
                    fac = self.check_cmaq_units(param='SO2', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='SO2') * fac
                    dfso2 = self.interp_to_airnow(cmaq, dfso2, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqso2 = cmaq
                    dfs.append(dfso2)
                except:
                    pass
            elif i == 'NOX':
                try:
                    print 'Interpolating NOX:'
                    dfnox = g.get_group(i)
                    fac = self.check_cmaq_units(param='NOX', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NOX') * fac
                    dfnox = self.interp_to_airnow(cmaq, dfnox, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqnox = cmaq
                    dfs.append(dfnox)
                except:
                    pass
            elif i == 'WD':
                try:
                    print 'Interpolating Wind Direction:'
                    dfmet = g.get_group(i)
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param='WDIR10')
                    dfmet = self.interp_to_airnow(cmaq, dfmet, interp=interp, r=radius, weight_func=weight_func)
                    dfs.append(dfmet)
                except:
                    pass
            elif i == 'WS':
                try:
                    print 'Interpolating Wind Speed:'
                    dfmet = g.get_group(i)
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param='WSPD10')
                    dfmet = self.interp_to_airnow(cmaq, dfmet, interp=interp, r=radius, weight_func=weight_func)
                    dfs.append(dfmet)
                except:
                    pass
            elif i == 'TEMP':
                try:
                    print 'Interpolating 2 Meter Temperature:'
                    dfmet = g.get_group(i)
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param='TEMP2')
                    dfmet = self.interp_to_airnow(cmaq, dfmet, interp=interp, r=radius, weight_func=weight_func)
                    dfmet.Obs += 273.15
                    dfs.append(dfmet)
                except:
                    pass

        self.df = pd.concat(dfs)
        if self.airnow.monitor_df is None:
            print '\n=========================================================================================='
            print 'Please load the Monitor Site Meta-Data to calculate 8hr Ozone: airnow.read_monitor_file()\n'
            print 'run: \'df = calc_airnow_8hr_max_calc()\'\n'
            print '===========================================================================================\n'
        else:
            if ('O3' in self.cmaq.keys):
                print 'Calculating Daily 8 Hr Max Ozone....\n'
                self.df8hr = self.calc_airnow_8hr_max_calc()
            else:
                pass
        self.df.SCS = self.df.SCS.values
        self.df.dropna(inplace=True, subset=['Obs', 'CMAQ'])
        self.print_available()

    def print_available(self):
        print 'Available Functions:\n'
        print '    airnow_spatial(df, param=\'OZONE\', path='', region='', date=\'YYYY-MM-DD HH:MM:SS\')'
        print '    airnow_spatial_8hr(df, path=\'cmaq-basemap-conus.p\', region=\'northeast\', date=\'YYYY-MM-DD\')'
        print '    compare_param(param=\'OZONE\', city=\'\', region=\'\', timeseries=False, scatter=False, pdfs=False,diffscatter=False, diffpdfs=False,timeseries_error=False)\n'
        print 'Species available to compare:'
        print '    ', self.df.Species.unique()

    def compare_param(self, param='OZONE', site='', city='', state='', region='', timeseries=False, scatter=False,
                      pdfs=False,
                      diffscatter=False, diffpdfs=False, timeseries_rmse=False, timeseries_mb=False,
                      taylordiagram=False, fig=None, label=None, footer=True, dia=None):
        from numpy import NaN
        df = self.df.copy()[
            ['datetime', 'datetime_local', 'Obs', 'CMAQ', 'Species', 'MSA_Name', 'Region', 'SCS', 'Units', 'Latitude',
             'Longitude', 'State_Name']]
        df.Obs[df.Obs < -990] = NaN
        df.dropna(subset=['Obs'], inplace=True)
        g = df.groupby('Species')
        new = g.get_group(param)
        if site != '':
            if site in new.SCS.unique():
                df2 = new.loc[new.SCS == site]
            title = site
        elif city != '':
            names = df.MSA_Name.dropna().unique()
            for i in names:
                if city.upper() in i.upper():
                    name = i
                    print name
            df2 = new[new['MSA_Name'] == name].copy().drop_duplicates()
            title = name
        elif state != '':
            df2 = new[new['State_Name'].str.upper() == state.upper()].copy().drop_duplicates()
            title = state
        elif region != '':
            df2 = new[new['Region'].str.upper() == region.upper()].copy().drop_duplicates()
            title = region
        else:
            df2 = new
            title = 'Domain'
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
            if fig is None:
                plots.taylordiagram(df2, label=label, dia=dia, addon=False)
            else:
                plots.taylordiagram(df2, label=label, dia=dia, addon=True)

    def spatial(self, df, param='OZONE', path='', region='', date='', xlim=[], ylim=[], vmin=0, vmax=150, ncolors=15,
                cmap='YlGnBu'):
        """
        :param param: Species Parameter: Acceptable Species: 'OZONE' 'PM2.5' 'CO' 'NOY' 'SO2' 'SO2' 'NOX'
        :param region: EPA Region: 'Northeast', 'Southeast', 'North Central', 'South Central', 'Rockies', 'Pacific'
        :param date: If not supplied will plot all time.  Put in 'YYYY-MM-DD HH:MM' for single time
        :return:
        """

        g = df.groupby('Species')
        df2 = g.get_group(param)
        param = param.upper()
        if param == 'OZONE':
            cmaq = self.cmaqo3
        elif param == 'PM2.5':
            cmaq = self.cmaqpm25
        elif param == 'PM10':
            cmaq = self.cmaqpm10
        elif param == 'CO':
            cmaq = self.cmaqco
        elif param == 'NOY':
            cmaq = self.cmaqnoy
        elif param == 'SO2':
            cmaq = self.cmaqso2
        elif param == 'NOX':
            cmaq = self.cmaqnox
        elif param == 'NO':
            cmaq = self.cmaqno
        elif param == 'NO2':
            cmaq = self.cmaqno2
        else:
            cmaq = None
        m = self.cmaq.choose_map(path, region)
        print param
        if isinstance(cmaq, type(None)):
            print 'This parameter is not in the CMAQ file: ' + param
        else:
            if date == '':
                for index, i in enumerate(self.cmaq.dates):
                    c = plots.make_spatial_plot(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index],
                                                m, vmin=vmin, vmax=vmax, ncolors=ncolors, cmap=cmap)
                    plots.spatial_scatter(df2, m, i.strftime('%Y-%m-%d %H:%M:%S'), vmin=vmin, vmax=vmax,
                                          ncolors=ncolors, cmap=cmap)
                    c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
                    if len(xlim) > 1:
                        plt.xlim([min(xlim), max(xlim)])
                        plt.ylim([min(ylim), max(ylim)])
                    plt.savefig(str(index + 10) + param + '.jpg', dpi=100)
                    plt.close()

            else:
                index = where(self.cmaq.dates == datetime.strptime(date, '%Y-%m-%d %H:%M'))[0][0]
                c = plots.make_spatial_plot(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index], m,
                                            vmin=vmin, vmax=vmax, ncolors=ncolors, cmap=cmap)
                plots.spatial_scatter(df2, m, self.cmaq.dates[index].strftime('%Y-%m-%d %H:%M:%S'), vmin=vmin,
                                      vmax=vmax, ncolors=ncolors, cmap=cmap)
                c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
                if len(xlim) > 1:
                    plt.xlim([min(xlim), max(xlim)])
                    plt.ylim([min(ylim), max(ylim)])

    def airnow_spatial_8hr(self, path='', region='', date='', xlim=[], ylim=[]):
        if not isinstance(self.df8hr, pd.DataFrame):
            print '    Calculating 8 Hr Max Ozone '
            self.df8hr = self.calc_airnow_8hr_max_calc()
        print '    Creating Map'
        m = self.cmaq.choose_map(path, region)
        if date == '':
            for index, i in enumerate(self.df8hr.datetime_local.unique()):
                plots.eight_hr_spatial_scatter(self.df8hr, m, i.strftime('%Y-%m-%d'))
                c = plt.colorbar()
                c.set_label('8Hr Ozone Bias (pbbV)')
                plt.title(i.strftime('%Y-%m-%d') + ' Obs - CMAQ')
                plt.tight_layout()
                if len(xlim) > 1:
                    plt.xlim([min(xlim), max(xlim)])
                    plt.ylim([min(ylim), max(ylim)])
        else:
            plots.eight_hr_spatial_scatter(self.df8hr, m, date=date)
            c = plt.colorbar()
            c.set_label('8Hr Ozone Bias (pbbV)')
            plt.title(date + ' Obs - CMAQ')
            plt.tight_layout()

    @staticmethod
    def calc_stats2(df):
        mb = mystats.MB(df.Obs.values, df.cmaq.values)  # mean bias
        r2 = mystats.R2(df.Obs.values, df.cmaq.values)  # pearsonr ** 2
        ioa = mystats.IOA(df.Obs.values, df.cmaq.values)  # Index of Agreement
        rmse = mystats.RMSE(df.Obs.values, df.cmaq.values)
        return mb, r2, ioa, rmse

    def interp_to_airnow(self, cmaqvar, df, interp='nearest', r=12000., n=5, weight_func=lambda r: 1 / r ** 2,
                         label='CMAQ'):
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
        from pandas import concat
        from numpy import append, empty, vstack, NaN

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
            if max(con):
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

        vals = pd.Series(vals)
        date = pd.Series(date)
        site = pd.Series(site)
        dfs = concat([vals, date, site], axis=1, keys=[label, 'datetime', 'SCS'])
        df = pd.merge(df, dfs, how='left', on=['SCS', 'datetime'])

        return df

    def get_pm25spec(self, interp='nearest', radius=12000., neighbors=5., weight_func=lambda r: 1 / r ** 2):

        # get
        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='CAf')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='PCA')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'PCA Variables not found'
            pass

        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='clf')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='PCL')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'PCl Variables not found'
            pass

        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='so4f')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='PSO4')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'PSO4 Variables not found'
            pass

        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='no3f')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='PNO3')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'PNO3 Variables not found'
            pass

        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='nh4f')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='PNH4')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'PNH4 Variables not found'
            pass

        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='oc')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='POC')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'Organic Carbon Variables not found'
            pass

        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='kf')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='PK')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'Organic Carbon Variables not found'
            pass

        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='naf')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='PNA')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'PNA Variables not found'
            pass

        try:
            g = self.df.groupby('Species').get_group('PM2.5')
            var = self.cmaq.get_surface_cmaqvar(param='ecf')
            df = self.interp_to_airnow(var, g, interp=interp, r=radius, weight_func=weight_func, label='PEC')
            self.df = pd.merge(self.df, df, how='left', on=self.df.columns.tolist())
        except:
            print 'PEC Variables not found'
            pass

    def check_cmaq_units(self, param='O3', airnow_param='OZONE'):
        aunit = self.airnow.df[self.airnow.df.Species == airnow_param].Units.unique()[0]
        if aunit == 'UG/M3':
            fac = 1.
        elif aunit == 'PPB':
            fac = 1000.
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

    def ensure_values_indomain(self):
        lat = self.cmaq.latitude
        lon = self.cmaq.longitude

        con = ((self.airnow.df.Latitude.values > lat.min()) & (self.airnow.df.Latitude.values < lat.max()) & (
            self.airnow.df.Longitude.values > lon.min()) & (self.airnow.df.Longitude.values < lon.max()))
        self.airnow.df = self.airnow.df[con].copy()

    def calc_airnow_8hr_max_calc(self):
        r = self.df.groupby('Species').get_group('OZONE')
        r.index = r.datetime_local
        g = r.groupby('SCS')['Obs', 'CMAQ', 'Latitude', 'Longitude']
        m = g.rolling(8, center=True, win_type='boxcar').mean()
        q = m.reset_index(level=0)
        # q.index = self.df.groupby('Species').get_group('OZONE').datetime_local
        k = q.groupby('SCS').resample('1D').max()
        kk = k.reset_index(level=1)
        kkk = kk.reset_index(drop='SCS').dropna()
        kkk = self.airnow.get_station_locations_remerge(kkk)
        return kkk

    def write_table(self, df=None, param='OZONE', fname='table.txt', threasholds=[70, 1e5], site=None, city=None,
                    region=None,
                    state=None,
                    append=False,
                    label='CMAQ'):
        from StringIO import StringIO
        single = False
        if not isinstance(df, None):
            single = True
            pass
        else:
            df = self.df.groupby('Species').get_group(param)
        if not isinstance(site, None):
            try:
                df = df.groupby('SCS').get_group(site)
                single = True
                name = site
            except KeyError:
                print 'Site Number not valid.  Enter a valid SCS'
                return
        elif not isinstance(city, None):
            try:
                names = df.get_group('MSA_Name').dropna().unique()
                name = [j for j in names if city.upper() in j.upper()]
                df = self.df.groupby('Species').get_group(param).groupby('MSA_Name').get_group(name[0])
                single = True
            except KeyError:
                print ' City either does not contain montiors for ' + param
                print '     or City Name is not valid.  Enter a valid City name: self.df.MSA_Name.unique()'
                return
        elif not isinstance(state, None):
            try:
                names = df.get_group('State_Name').dropna().unique()
                name = [j for j in names if state.upper() in j.upper()]
                df = self.df.groupby('Species').get_group(param).groupby('State_Name').get_group(name[0])
            except KeyError:
                print 'State not valid. Please enter valid 2 digit state'
                return
        elif not isinstance(region, None):
            try:
                names = df.get_group('MSA_Name').dropna().unique()
                name = [j for j in names if region.upper() in j.upper()]
                df = df.groupby('Region').get_group(name[0])
            except KeyError:
                print 'Region not valid.  Enter a valid Region'
                return
        if single:
            d = mystats.stats_table(df, threasholds[0], threasholds[1])
            d['label'] = name
            dd = pd.DataFrame(d, dindex=[0])
        else:
            d = mystats.stats_table(df, threasholds[0], threasholds[1])
            d['Region'] = 'Domain'
            dd = pd.DataFrame(d, index=[0])
            for i in dd.Region.dropna().unique():
                try:
                    dff = df.groupby('Region').get_group(i)
                    dt = mystats.stats_table(df, threasholds[0], threasholds[1])
                    dt['Region'] = i
                    dft = pd.DataFrame(dt, index=[0])
                    dd = pd.concat([dd, dft])
                except KeyError:
                    pass
        stats = ['N', 'Obs', 'Mod', 'MB', 'NMB', 'RMSE', 'R', 'IOA', 'POD', 'FAR']
        print dd[stats]
        out = StringIO()
        dd.to_string(out, float_format='%10.3f', columns=stats)
        out.seek(0)
        with open(fname, 'w') as f:
            if single:
                f.write('This is the statistics table for parameter=' + param + ' for area ' + name + '\n')
            else:
                f.write('This is the statistics table for parameter=' + param + '\n')
            f.write('\n')
            f.writelines(out.readlines())
