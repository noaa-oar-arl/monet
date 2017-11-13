# verify is the main application.
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
from numpy import array, where, sort

import mystats
import plots
from airnow import airnow
from cmaq import cmaq


class verify_airnow:
    def __init__(self):
        self.airnow = airnow()
        self.cmaq = cmaq()
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
        self.cmaqtemp = None
        self.cmaqsrad = None
        self.cmaqws = None
        self.df8hr = None

    def combine(self, radius=12000. * 2):
        from interpolation import interp_to_obs_new
        # get the CMAQ dates
        print 'Acquiring Dates of CMAQ Simulation'
        print '==============================================================='
        print 'Simulation Start Date: ', self.cmaq.conc.TSTEP.to_index()[0].strftime('%Y-%m-%d %H:%M')
        print 'Simulation End   Date: ', self.cmaq.conc.TSTEP.to_index()[-1].strftime('%Y-%m-%d %H:%M')
        print '===============================================================\n'
        if self.cmaq.metcro2d is None:
            print 'METCRO2D file not loaded.  To include MET variables please load self.camx.open_metcro2d(\'filename\')\n'
        self.airnow.df.dropna(subset=['Latitude', 'Longitude'], inplace=True)
        comparelist = sort(self.airnow.df.Species.unique())
        g = self.airnow.df.groupby('Species')
        dfs = []
        for i in comparelist:
            if i == 'OZONE':
                # try:
                print 'Interpolating Ozone:'
                dfo3 = g.get_group(i)
                fac = self.check_cmaq_units(param='O3', airnow_param=i)
                cmaq = self.cmaq.get_var(lay=0, param='O3').compute() * fac
                self.cmaqo3 = cmaq
                dfo3 = interp_to_obs_new(cmaq, dfo3, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                         radius=radius)
                dfs.append(dfo3)
                # except:
                #    pass
            elif i == 'PM2.5':
                try:
                    print 'Interpolating PM2.5:'
                    dfpm25 = g.get_group(i)
                    fac = self.check_cmaq_units(param='PM25', airnow_param=i)
                    cmaq = self.cmaq.get_var(lay=0, param='PM25').compute() * fac
                    dfpm25 = interp_to_obs_new(cmaq, dfpm25, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                               radius=radius)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm25)
                except:
                    pass
            elif i == 'PM10':
                try:
                    print 'Interpolating PM10:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='PM10', airnow_param=i)
                    cmaq = self.cmaq.get_var(lay=0, param='PM10').compute() * fac
                    dfpm = interp_to_obs_new(cmaq, dfpm, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                             radius=radius)
                    self.cmaqpm10 = cmaq
                    dfs.append(dfpm)
                except:
                    pass
            elif i == 'CO':
                try:
                    print 'Interpolating CO:'
                    dfco = g.get_group(i)
                    fac = self.check_cmaq_units(param='CO', airnow_param=i)
                    cmaq = self.cmaq.get_var(lay=0, param='CO').compute() * fac
                    dfco = interp_to_obs_new(cmaq, dfco, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                             radius=radius)
                    self.cmaqco = cmaq
                    dfs.append(dfco)
                except:
                    pass
            elif i == 'NOY':
                try:
                    print 'Interpolating NOY:'
                    dfnoy = g.get_group(i)
                    fac = self.check_cmaq_units(param='NOY', airnow_param=i)
                    cmaq = self.cmaq.get_var(lay=0, param='NOY').compute() * fac
                    dfnoy = interp_to_obs_new(cmaq, dfnoy, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                              radius=radius)
                    self.cmaqnoy = cmaq
                    dfs.append(dfnoy)
                except:
                    pass
            elif i == 'NO':
                try:
                    print 'Interpolating NO:'
                    dfnoy = g.get_group(i)
                    fac = self.check_cmaq_units(param='NO', airnow_param=i)
                    cmaq = self.cmaq.get_var(lay=0, param='NO').compute() * fac
                    dfnoy = interp_to_obs_new(cmaq, dfnoy, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                              radius=radius)
                    self.cmaqno = cmaq
                    dfs.append(dfnoy)
                except:
                    pass
            elif i == 'NO2':
                try:
                    print 'Interpolating NO2:'
                    dfnoy = g.get_group(i)
                    fac = self.check_cmaq_units(param='NO2', airnow_param=i)
                    cmaq = self.cmaq.get_var(lay=0, param='NO2').compute() * fac
                    dfnoy = interp_to_obs_new(cmaq, dfnoy, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                              radius=radius)
                    self.cmaqno2 = cmaq
                    dfs.append(dfnoy)
                except:
                    pass
            elif i == 'SO2':
                try:
                    print 'Interpolating SO2'
                    dfso2 = g.get_group(i)
                    fac = self.check_cmaq_units(param='SO2', airnow_param=i)
                    cmaq = self.cmaq.get_var(lay=0, param='SO2').compute() * fac
                    dfso2 = interp_to_obs_new(cmaq, dfso2, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                              radius=radius)
                    self.cmaqso2 = cmaq
                    dfs.append(dfso2)
                except:
                    pass
            elif i == 'NOX':
                try:
                    print 'Interpolating NOX:'
                    dfnox = g.get_group(i)
                    fac = self.check_cmaq_units(param='NOX', airnow_param=i)
                    cmaq = self.cmaq.get_var(lay=0, param='NOX').compute() * fac
                    dfnox = interp_to_obs_new(cmaq, dfnox, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                              radius=radius)
                    self.cmaqnox = cmaq
                    dfs.append(dfnox)
                except:
                    pass
            elif (i == 'WD') & (self.cmaq.metcro2d is not None):
                try:
                    print 'Interpolating Wind Direction:'
                    dfmet = g.get_group(i)
                    self.cmaqwdir = self.cmaq.get_metcro2d_cmaqvar(lay=0, param='WDIR10')
                    dfmet = interp_to_obs_new(self.cmaqwdir, dfmet, self.cmaq.latitude.values,
                                              self.cmaq.longitude.values, radius=radius)
                    dfs.append(dfmet)
                except:
                    pass
            elif (self.cmaq.metcro2d is not None) & (i == 'WS'):
                try:
                    print 'Interpolating Wind Speed:'
                    dfmet = g.get_group(i)
                    self.cmaqws = self.cmaq.get_metcro2d_cmaqvar(lay=0, param='WSPD10')

                    dfmet = interp_to_obs_new(self.cmaqws, dfmet, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                              radius=radius)
                    dfs.append(dfmet)
                except:
                    pass
            elif (self.cmaq.metcro2d is not None) & (i == 'TEMP'):
                try:
                    print 'Interpolating 2 Meter Temperature:'
                    dfmet = g.get_group(i)
                    self.cmaqtemp = self.cmaq.get_metcro2d_cmaqvar(lay=0, param='TEMP2') - 273.15
                    dfmet = interp_to_obs_new(self.cmaqtemp, dfmet, self.cmaq.latitude.values,
                                              self.cmaq.longitude.values, radius=radius)
                    dfs.append(dfmet)
                except:
                    pass
            elif (self.cmaq.metcro2d is not None) & (i == 'RHUM'):
                try:
                    print 'Interpolating Relative Humidity:'
                    dfmet = g.get_group(i)
                    self.cmaqrh = self.cmaq.get_metcro2d_cmaqvar(lay=0, param='RH')
                    dfmet = interp_to_obs_new(self.cmaqrh, dfmet, self.cmaq.latitude.values, self.cmaq.longitude.values,
                                              radius=radius)
                    dfs.append(dfmet)
                except:
                    pass
            elif (self.cmaq.metcro2d is not None) & (i == 'SRAD'):
                try:
                    print 'Interpolating Short Wave Radiation:'
                    dfmet = g.get_group(i)
                    self.cmaqsrad = self.cmaq.get_metcro2d_cmaqvar(lay=0, param='RGRND')
                    dfmet = interp_to_obs_new(self.cmaqsrad, dates, dfmet, self.cmaq.latitude.values,
                                              self.cmaq.longitude.values, radius=radius)
                    dfs.append(dfmet)
                except:
                    pass
                #        print dfs
        self.df = pd.concat(dfs)
        self.df.dropna(subset=['Obs', 'CMAQ'], inplace=True)

        self.df.SCS = self.df.SCS.values
        self.df.dropna(inplace=True, subset=['Obs', 'CMAQ'])
        self.print_available()

    def print_available(self):
        print 'Available Functions:\n'
        print '    airnow_spatial(df, param=\'OZONE\', path='', region='', date=\'YYYY-MM-DD HH:MM:SS\')'
        print '    airnow_spatial_8hr(df, path=\'camx-basemap-conus.p\', region=\'northeast\', date=\'YYYY-MM-DD\')'
        print '    compare_param(param=\'OZONE\', city=\'\', region=\'\', timeseries=False, scatter=False, pdfs=False,diffscatter=False, diffpdfs=False,timeseries_error=False)\n'
        print 'Species available to compare:'
        print '    ', self.df.Species.unique()

    def compare_param(self, param='OZONE', site='', city='', state='', epa_region='', region='', timeseries=False,
                      scatter=False,
                      pdfs=False,
                      diffscatter=False, diffpdfs=False, timeseries_rmse=False, timeseries_mb=False,
                      taylordiagram=False, ax=None, label=None, footer=False, dia=None, marker=None):
        from numpy import NaN
        from utils import get_epa_location_df
        df2, title = get_epa_location_df(self.df.copy(), param, site=site, city=city, state=state, region=region,
                                         epa_region=epa_region)
        df2 = df2.groupby('Species').get_group(param)
        if timeseries:
            if ax is None:
                ax = plots.timeseries_param_new(df2, col='Obs',title=title, label=label, ax=ax, plotargs={'color':'darkslategrey'},fillargs={'color':'darkslategrey','alpha':.2})
            ax = plots.timeseries_param_new(df2, col='CMAQ',title=title, label=label, ax=ax, fillargs={'alpha':.2})
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
                dia = plots.taylordiagram(df2, label=label, dia=dia, addon=False, marker=marker)
                return dia
            else:
                dia = plots.taylordiagram(df2, label=label, dia=dia, addon=True, marker=marker)
                plt.legend()
                return dia

    def spatial(self, df, param='OZONE', date=None, imshow_args={},
                scatter_args={'s': 20, 'edgecolors': 'w', 'lw': .25}, barbs_args={}, barbs=False, Obs=True,
                ncolors=None, discrete=False):
        """
        :param param: Species Parameter: Acceptable Species: 'OZONE' 'PM2.5' 'CO' 'NOY' 'SO2' 'SO2' 'NOX'
        :param region: EPA Region: 'Northeast', 'Southeast', 'North_Central', 'South_Central', 'Rockies', 'Pacific'
        :param date: If not supplied will plot all time.  Put in 'YYYY-MM-DD HH:MM' for single time
        :return:
        """
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
            try:
                if not isinstance(self.cmaq.metcro2d, type(None)):
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param=param)
            except:
                pass
            try:
                cmaq = self.cmaq.get_var(param=param)
            except:
                exit
        m = self.cmaq.map
        if isinstance(cmaq, type(None)):
            print 'This parameter is not in the CMAQ file: ' + param
        else:
            dts = cmaq.TSTEP.to_index()
            if isinstance(date, type(None)):
                index = where(dts == dts[0])[0][0]
            else:
                index = where(dts.isin([date]))[0][0]
            f, ax, c, cmap, vmin, vmax = plots.make_spatial_plot2(cmaq[index, :, :].squeeze(), m, plotargs=imshow_args,
                                                                  ncolors=ncolors, discrete=discrete)
            plt.tight_layout()
            if not isinstance(self.cmaq.metcro2d, type(None)) and barbs == False:
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

    def spatial_contours(self, df, param='OZONE', path='', region='', date='', xlim=[], ylim=[], ncolors=None):
        """
        :param param: Species Parameter: Acceptable Species: 'OZONE' 'PM2.5' 'CO' 'NOY' 'SO2' 'SO2' 'NOX'
        :param region: EPA Region: 'Northeast', 'Southeast', 'North_Central', 'South_Central', 'Rockies', 'Pacific'
        :param date: If not supplied will plot all time.  Put in 'YYYY-MM-DD HH:MM' for single time
        :return:
        """
        import colorbars
        from numpy import unique

        g = df.groupby('Species')
        df2 = g.get_group(param)
        param = param.upper()
        cmap = None
        if param == 'OZONE':
            cmaq = self.cmaqo3
            cmap, levels = colorbars.o3cmap()
        elif param == 'PM2.5':
            cmaq = self.cmaqpm25
            cmap, levels = colorbars.pm25cmap()
        elif param == 'PM10':
            cmaq = self.cmaqpm10
            cmap, levels = colorbars.pm25cmap()
        elif param == 'CO':
            cmaq = self.cmaqco
            cmap, levels = colorbars.noxcmap()
        elif param == 'NOY':
            cmaq = self.cmaqnoy
            cmap, levels = colorbars.noxcmap()
        elif param == 'SO2':
            cmaq = self.cmaqso2
            cmap, levels = colorbars.so2cmap()
        elif param == 'NOX':
            cmaq = self.cmaqnox
            cmap, levels = colorbars.noxcmap()
        elif param == 'NO':
            cmaq = self.cmaqno
            cmap, levels = colorbars.noxcmap()
        elif param == 'NO2':
            cmaq = self.cmaqno2
            cmap, levels = colorbars.noxcmap()
        elif param == 'SRAD':
            cmaq = self.cmaqsrad
            cmap, levels = colorbars.sradcmap()
        elif param == 'WS':
            cmaq = self.cmaqws
            cmap, levels = colorbars.wscmap()
        elif param == 'TEMP':
            cmaq = self.cmaqtemp
            cmap, levels = colorbars.tempcmap()
        elif param == 'RHUM':
            cmaq = self.cmaqrh
            cmap, levels = colorbars.rhcmap()
        else:
            cmaq = None
        m = self.cmaq.map  # m = self.camx.choose_map(path, region)
        if isinstance(cmaq, type(None)):
            print 'This parameter is not in the CMAQ file: ' + param
        else:
            if date == '':
                for index, i in enumerate(self.cmaq.dates):
                    c = plots.make_spatial_contours(cmaq[index, :, :].squeeze(), self.cmaq.gridobj,
                                                    self.cmaq.dates[index],
                                                    m, levels=levels[::4], cmap=cmap, units='xy', ncolors=ncolors)
                    if not isinstance(self.cmaq.metcro2d, type(None)):
                        ws = self.cmaqws[index, :,
                             :].squeeze()  # self.camx.metcro2d.variables['WSPD10'][index, :, :, :].squeeze()
                        wdir = self.cmaqwdir[index, :,
                               :].squeeze()  # self.camx.metcro2d.variables['WDIR10'][index, :, :, :].squeeze()
                        plots.wind_barbs(ws, wdir, self.cmaq.gridobj, m, color='grey', alpha=.5)
                    try:
                        plots.spatial_scatter(df2, m, i.strftime('%Y-%m-%d %H:%M:%S'), vmin=levels[0], vmax=levels[-1],
                                              cmap=cmap, discrete=False)
                    except:
                        pass
                    c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
                    c.set_ticks(unique(levels.round(-1)))
                    if len(xlim) > 1:
                        plt.xlim([min(xlim), max(xlim)])
                        plt.ylim([min(ylim), max(ylim)])
                    plt.savefig(str(index + 10) + param + '.jpg', dpi=100)
                    plt.close()

            else:
                index = where(self.cmaq.dates == datetime.strptime(date, '%Y-%m-%d %H:%M'))[0][0]
                c = plots.make_spatial_contours(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index],
                                                m,
                                                levels=levels, cmap=cmap, units='xy', ncolors=ncolors)
                if not isinstance(self.cmaq.metcro2d, type(None)):
                    ws = self.cmaqws[index, :,
                         :].squeeze()  # self.camx.metcro2d.variables['WSPD10'][index, :, :, :].squeeze()
                    wdir = self.cmaqwdir[index, :,
                           :].squeeze()  # self.camx.metcro2d.variables['WDIR10'][index, :, :, :].squeeze()
                    plots.wind_quiver(ws, wdir, self.cmaq.gridobj, m, alpha=.3, headwidth=2, headlength=4)
                try:
                    plots.spatial_scatter(df2, m, self.cmaq.dates[index].strftime('%Y-%m-%d %H:%M:%S'), vmin=levels[0],
                                          vmax=levels[-1], cmap=cmap, discrete=False)
                except:
                    pass
                c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
                # c.set_ticks(unique(levels.round(-1)))
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

    def calc_8hr_max(self, df=None, param='OZONE'):
        temp = df.groupby('Species').get_group(param)
        temp.index = temp.datetime_local
        g = temp.groupby('SCS')['Obs', 'CMAQ', 'Latitude', 'Longitude', 'utcoffset'].rolling(8, center=True,
                                                                                             win_type='boxcar').mean()
        q = g.reset_index(level=0)
        k = q.groupby('SCS').resample('D').max().reset_index(level=1).reset_index(drop='SCS').dropna()
        columnstomerge = df.columns[~df.columns.isin(k.columns) & (df.columns != 'datetime')].append(pd.Index(['SCS']))
        dff = k.merge(df.groupby('Species').get_group(param)[columnstomerge], on='SCS', how='left').drop_duplicates(
            subset=['SCS', 'datetime_local'])
        dff['datetime'] = dff.datetime_local + pd.to_timedelta(dff.utcoffset, unit='H')
        return dff

    def calc_24hr_ave(self, df=None, param='PM2.5'):
        temp = df.groupby('Species').get_group(param)
        temp.index = temp.datetime_local
        dff = temp.groupby('SCS').resample('D').mean().reset_index()
        dff.drop('MSA_Code', inplace=True, axis=1)
        dff.drop('CMSA_Name', inplace=True, axis=1)
        columnstomerge = df.columns[~df.columns.isin(dff.columns) & (df.columns != 'datetime')].append(
            pd.Index(['SCS']))
        dff = dff.merge(df.groupby('Species').get_group(param)[columnstomerge], on='SCS', how='left').drop_duplicates(
            subset=['SCS', 'datetime_local'])
        dff['datetime'] = dff.datetime_local + pd.to_timedelta(dff.utcoffset, unit='H')
        return dff

    def write_table(self, df=None, fname='table', threasholds=[70, 1e5], site=None, city=None,
                    region=None,
                    state=None,
                    append=False,
                    label='CMAQ',
                    html=False):
        from StringIO import StringIO
        single = False
        df = df.dropna(subset=['Obs', 'CMAQ'])
        df = df[df.EPA_region != 'USEPA']
        param = df.Species.unique()[0]
        if not isinstance(site, type(None)):
            try:
                df = df.groupby('SCS').get_group(site)
                single = True
                name = site
            except KeyError:
                print 'Site Number not valid.  Enter a valid SCS'
                return
        elif not isinstance(city, type(None)):
            try:
                single = True
                names = df.get_group('MSA_Name').dropna().unique()
                name = [j for j in names if city.upper() in j.upper()]
                df = self.df.groupby('Species').get_group(param).groupby('MSA_Name').get_group(name[0])
                single = True
            except KeyError:
                print ' City either does not contain montiors for ' + param
                print '     or City Name is not valid.  Enter a valid City name: self.df.MSA_Name.unique()'
                return
        elif not isinstance(state, type(None)):
            try:
                single = True
                names = df.get_group('State_Name').dropna().unique()
                name = [j for j in names if state.upper() in j.upper()]
                df = self.df.groupby('Species').get_group(param).groupby('State_Name').get_group(name[0])
            except KeyError:
                print 'State not valid. Please enter valid 2 digit state'
                return
        elif not isinstance(region, type(None)):
            try:
                single = True
                names = df.get_group('EPA_region').dropna().unique()
                name = [j for j in names if region.upper() in j.upper()]
                print name
                df = df.groupby('EPA_region').get_group(name[0])
            except KeyError:
                print 'Region not valid.  Enter a valid Region'
                return
        if single:
            d = mystats.stats(df, threasholds[0], threasholds[1])
            d['Region'] = name
            d['Label'] = label
            dd = pd.DataFrame(d, index=[0])
        else:
            d = mystats.stats(df, threasholds[0], threasholds[1])
            d['Region'] = 'Domain'
            d['Label'] = label
            dd = pd.DataFrame(d, index=[0])
            for i in df.EPA_region.dropna().unique():
                try:
                    dff = df.groupby('EPA_region').get_group(i)
                    dt = mystats.stats(dff, threasholds[0], threasholds[1])
                    dt['Region'] = i.replace(' ', '_')
                    dt['Label'] = label
                    dft = pd.DataFrame(dt, index=[0])
                    dd = pd.concat([dd, dft])
                except KeyError:
                    pass
        pd.options.display.float_format = '{:,.3f}'.format
        stats = ['Region', 'Label', 'N', 'Obs', 'Mod', 'MB', 'NMB', 'RMSE', 'R', 'IOA', 'POD', 'FAR']
        if append:
            dff = pd.read_csv(fname + '.txt', skiprows=3, index_col=0, sep='\s+', names=stats)
            dd = pd.concat([dd, dff]).sort_values(by=['Region'])

        dd.sort_values(by=['Region', 'Label'], inplace=True)
        out = StringIO()
        dd.to_string(out, columns=stats)
        out.seek(0)
        with open(fname + '.txt', 'w') as f:
            if single:
                f.write('This is the statistics table for parameter=' + param + ' for area ' + name + '\n')
            else:
                f.write('This is the statistics table for parameter=' + param + '\n')
            f.write('\n')
            f.writelines(out.readlines())
        if html:
            #            dd.index = dd.Region
            #            dd.drop(['Region'],axis=1,inplace=True)
            dd.sort_values(by=['Region', 'Label'], inplace=True)
            dd.index = dd.Region
            dd.drop(['Region'], axis=1, inplace=True)

            dd[stats[1:]].to_html(fname + '.html')

            cssstyle = '<style>\n.GenericTable\n{\nfont-size:12px;\ncolor:white;\nborder-width: 1px;\nborder-color: rgb(160,160,160);/* This is dark*/\nborder-collapse: collapse;\n}\n.GenericTable th\n{\nfont-size:16px;\ncolor:white;\nbackground-color:rgb(100,100,100);/* This is dark*/\nborder-width: 1px;\npadding: 4px;\nborder-style: solid;\nborder-color: rgb(192, 192, 192);/* This is light*/\ntext-align:left;\n}\n.GenericTable tr\n{\ncolor:black;\nbackground-color:rgb(224, 224, 224);/* This is light*/\n}\n.GenericTable td\n{\nfont-size:14px;\nborder-width: 1px;\nborder-style: solid;\nborder-color: rgb(255, 255, 255);/* This is dark*/\n}\n.hoverTable{\nwidth:100%; \nborder-collapse:collapse; \n}\n.hoverTable td{ \npadding:7px; border:#E0E0E0 1px solid;\n}\n/* Define the default color for all the table rows */\n.hoverTable tr{\nbackground: #C0C0C0;\n}\n/* Define the hover highlight color for the table row */\n    .hoverTable tr:hover {\n          background-color: #ffff99;\n    }\n</style>'

            lines = cssstyle.split('\n')
            with open(fname + '.html', 'r') as f:
                for line in f.readlines():
                    lines.append(line.replace('class="dataframe"', 'class="GenericTable hoverTable"'))
            f.close()
            with open(fname + '.html', 'w') as f:
                for line in lines:
                    f.write(line)
            f.close()
        return dd
