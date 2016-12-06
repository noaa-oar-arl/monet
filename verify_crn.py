# verify is the main application.
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
from numpy import array, where

import mystats
import plots
from crn import crn
from cmaq import cmaq


class verify_crn:
    def __init__(self):
        self.crn = crn()
        self.cmaq = cmaq()
        self.df = None
        self.cmaqo3 = None
        self.cmaqnox = None
        self.cmaqnoy = None
        self.cmaqpm25 = None
        self.cmaqpm10 = None
        self.cmaqso2 = None
        self.cmaqco = None
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
        comparelist = self.crn.df.Species.unique()
        g = self.crn.df.groupby('Species')
        dfs = []
        for i in comparelist:
            if i == 'SUR_TEMP':
                if (self.cmaq.metcro2d is None) | ('TEMPG' not in self.cmaq.metcrokeys):
                    pass
                else:
                    print 'Interpolating Surface Skin Temperature:'
                    dfmet = g.get_group(i)
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param='TEMPG')
                    dfmet = self.interp_to_crn(cmaq, dfmet, interp=interp, r=radius, weight_func=weight_func)
                    dfmet.Obs += 273.15
                    dfs.append(dfmet)
            elif i == 'T_HR_AVG':
                if (self.cmaq.metcro2d is None) | ('TEMP2' not in self.cmaq.metcrokeys):
                    pass
                else:
                    print 'Interpolating 2 Meter Temperature:'
                    dfmet = g.get_group(i)
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param='TEMP2')
                    dfmet = self.interp_to_crn(cmaq, dfmet, interp=interp, r=radius, weight_func=weight_func)
                    dfmet.Obs += 273.15
                    dfs.append(dfmet)
            elif i == 'SOLARAD':
                if (self.cmaq.metcro2d is None) | ('RGRND' not in self.cmaq.metcrokeys):
                    pass
                else:
                    print 'Interpolating Downward Solar Radiation:'
                    dfmet = g.get_group(i)
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param='RGRND')
                    dfmet = self.interp_to_crn(cmaq, dfmet, interp=interp, r=radius, weight_func=weight_func)
                    dfs.append(dfmet)
            elif i == 'SOIL_MOISTURE_5':
                if (self.cmaq.metcro2d is None) | ('SOILW' not in self.cmaq.metcrokeys):
                    pass
                else:
                    print 'Interpolating Surface Volumetric Soil Moisture Content:'
                    dfmet = g.get_group(i)
                    cmaq = self.cmaq.get_metcro2d_cmaqvar(param='SOILW')
                    dfmet = self.interp_to_crn(cmaq, dfmet, interp=interp, r=radius, weight_func=weight_func)
                    dfs.append(dfmet)

        self.df = pd.concat(dfs)
        self.df.dropna(inplace=True, subset=['Obs', 'CMAQ'])
        self.print_available()

    def print_available(self):
        print 'Available Functions:\n'
        print '    crn_spatial(df, param=\'OZONE\', path='', region='', date=\'YYYY-MM-DD HH:MM\')'
        print '    compare_param(param=\'OZONE\', city=\'\', region=\'\', timeseries=False, scatter=False, pdfs=False,diffscatter=False, diffpdfs=False,timeseries_error=False)\n'
        print 'Species available to compare:'
        print '    ', self.df.Species.unique()

    def compare_param(self, param='OZONE', site='', city='', state='', region='', timeseries=False, scatter=False,
                      pdfs=False,
                      diffscatter=False, diffpdfs=False, timeseries_rmse=False, timeseries_mb=False, fig=None,
                      label=None, footer=True):
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
            plots.crn_timeseries_param(df2, title=title, label=label, fig=fig, footer=footer)
        if scatter:
            plots.crn_scatter_param(df2, title=title, label=label, fig=fig, footer=footer)
        if pdfs:
            plots.crn_kdeplots_param(df2, title=title, label=label, fig=fig, footer=footer)
        if diffscatter:
            plots.crn_diffscatter_param(df2, title=title)
        if diffpdfs:
            plots.crn_diffpdfs_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_rmse:
            plots.crn_timeseries_rmse_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_mb:
            plots.crn_timeseries_mb_param(df2, title=title, label=label, fig=fig, footer=footer)

    def crn_spatial(self, df, param='OZONE', path='', region='', date='', xlim=[], ylim=[], vmin=None, vmax=None):
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
        elif param == 'CO':
            cmaq = self.cmaqco
        elif param == 'NOY':
            cmaq = self.cmaqnoy
        elif param == 'SO2':
            cmaq = self.cmaqso2
        elif param == 'NOX':
            cmaq = self.cmaqnox
        m = self.cmaq.choose_map(path, region)
        if date == '':
            for index, i in enumerate(self.cmaq.dates):
                c = plots.make_spatial_plot(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index],
                                            m)
                plots.airnow_spatial_scatter(df2, m, i.strftime('%Y-%m-%d %H:%M:%S'))
                c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
                if len(xlim) > 1:
                    plt.xlim([min(xlim), max(xlim)])
                    plt.ylim([min(ylim), max(ylim)])

        else:
            index = where(self.cmaq.dates == datetime.strptime(date, '%Y-%m-%d %H:%M'))[0][0]
            c = plots.make_spatial_plot(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index], m,
                                        vmin=vmin, vmax=vmax)
            plots.airnow_spatial_scatter(df2, m, self.cmaq.dates[index].strftime('%Y-%m-%d %H:%M:%S'), vmin=vmin,
                                         vmax=vmax)
            c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
            if len(xlim) > 1:
                plt.xlim([min(xlim), max(xlim)])
                plt.ylim([min(ylim), max(ylim)])

    @staticmethod
    def calc_stats2(df):
        mb = mystats.MB(df.Obs.values, df.cmaq.values)  # mean bias
        r2 = mystats.R2(df.Obs.values, df.cmaq.values)  # pearsonr ** 2
        ioa = mystats.IOA(df.Obs.values, df.cmaq.values)  # Index of Agreement
        rmse = mystats.RMSE(df.Obs.values, df.cmaq.values)
        return mb, r2, ioa, rmse

    def interp_to_crn(self, cmaqvar, df, interp='nearest', r=12000., n=5, weight_func=lambda r: 1 / r ** 2):
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
        dfs = concat([vals, date, site], axis=1, keys=['CMAQ', 'datetime', 'SCS'])
        df = pd.merge(df, dfs, how='left', on=['SCS', 'datetime'])

        return df

    def check_cmaq_units(self, param='O3', crn_param='OZONE'):
        aunit = self.crn.df[self.crn.df.Species == crn_param].Units.unique()[0]
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

        con = ((self.crn.df.Latitude.values > lat.min()) & (self.crn.df.Latitude.values < lat.max()) & (
            self.crn.df.Longitude.values > lon.min()) & (self.crn.df.Longitude.values < lon.max()))
        self.crn.df = self.crn.df[con].copy()
