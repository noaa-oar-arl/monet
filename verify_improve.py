# verify is the main application.
from datetime import datetime

import matplotlib.pyplot as plt
from numpy import array, where, NaN, sort
from pandas import concat

import mystats
import plots
from cmaq import cmaq
from improve import improve


class verify_improve:
    def __init__(self):
        self.improve = improve()
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
        self.cmaqpm25 = None
        self.cmaqpm10 = None
        self.cmaqcl = None
        self.cmaqna = None
        self.cmaqmg = None
        self.cmaqk = None
        self.cmaqca = None
        self.cmaqso4 = None
        self.cmaqno3 = None
        self.cmaqnh4 = None
        self.cmaqfe = None
        self.cmaqmn = None
        self.cmaqsi = None
        self.cmaqal = None
        self.cmaqti = None

    def combine(self, interp='nearest', radius=12000. * 2, neighbors=10., weight_func=lambda r: 1 / r ** 2):
        print 'Acquiring Dates of CMAQ Simulation'
        print '==============================================================='
        self.cmaq.get_dates()
        print 'Simulation Start Date: ', self.cmaq.dates[0].strftime('%Y-%m-%d %H:%M')
        print 'Simulation End   Date: ', self.cmaq.dates[-1].strftime('%Y-%m-%d %H:%M')
        print '===============================================================\n'
        if self.cmaq.metcro2d is None:
            print 'METCRO2D file not loaded.  To include MET variables please load self.cmaq.open_metcro2d(\'filename\')\n'
        self.ensure_values_indomain()
        comparelist = sort(self.improve.df.Species.unique())
        g = self.improve.df.groupby('Species')
        dfs = []
        for i in comparelist:
            if i == 'CLf':
                if ('ACLI' in self.cmaq.keys) | ('ACLJ' in self.cmaq.keys) | ('PM25_CL' in self.cmaq.keys):
                    print 'Interpolating CLf:'
                    dfpm25 = g.get_group(i)
                    fac = self.check_cmaq_units(param='CLf', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='CLf') * fac
                    dfpm25 = self.interp_to_improve(cmaq, dfpm25, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm25)
                else:
                    pass
            elif i == 'PM10':
                print 'Interpolating PM10:'
                dfpm = g.get_group(i)
                fac = self.check_cmaq_units(param='PM10', improve_param=i)
                cmaqvar = self.cmaq.get_surface_cmaqvar(param='PM10') * fac
                dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                self.cmaqpm10 = cmaqvar
                dfs.append(dfpm)
            elif i == 'PM2.5':
                print 'Interpolating PM25:'
                dfpm = g.get_group(i)
                fac = self.check_cmaq_units(param='PM25', improve_param=i)
                cmaqvar = self.cmaq.get_surface_cmaqvar(param='PM25') * fac
                dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                self.cmaqpm25 = cmaqvar
                dfs.append(dfpm)
            elif i == 'NAf':
                if ('ANAI' in self.cmaq.keys) | ('ANAJ' in self.cmaq.keys) | ('PM25_NA' in self.cmaq.keys):
                    print 'Interpolating NAf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='NAf', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='NAf') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqna = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'MGf':
                if ('AMGI' in self.cmaq.keys) | ('AMGJ' in self.cmaq.keys) | ('PM25_MG' in self.cmaq.keys):
                    print 'Interpolating MGf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='MGf', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='AMGJ') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqmg= cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'TIf':
                if ('ATIJ' in self.cmaq.keys):
                    print 'Interpolating TIj:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='TIj', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='ATIJ') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqti= cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'SIf':
                if ('ASIf' in self.cmaq.keys):
                    print 'Interpolating SIf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='SIj', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='ASIJ') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqti= cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'Kf':
                if ('AKI' in self.cmaq.keys) | ('AKJ' in self.cmaq.keys) | ('PM25_K' in self.cmaq.keys):
                    print 'Interpolating Kf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='Kf', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='Kf') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqk = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'CAf':
                if ('ACAJ' in self.cmaq.keys) | ('PM25_CA' in self.cmaq.keys):
                    print 'Interpolating CAf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='CAf', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='ACAJ') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqca = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'SO4f':
                if ('ASO4I' in self.cmaq.keys) | ('ASO4J' in self.cmaq.keys) | ('PM25_SO4' in self.cmaq.keys):
                    print 'Interpolating SO4f:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='SO4f', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='SO4f') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqso4 = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'NH4f':
                if ('ANH4I' in self.cmaq.keys) | ('ANH4J' in self.cmaq.keys) | ('PM25_NH4' in self.cmaq.keys):
                    print 'Interpolating NH4f:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='NH4f', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='NH4f') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqnh4 = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'NO3f':
                if ('ANO3I' in self.cmaq.keys) | ('ANO3J' in self.cmaq.keys) | ('PM25_NO3' in self.cmaq.keys):
                    print 'Interpolating NO3f:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='NO3f', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='NO3f') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqno3 = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'FEf':
                if ('AFEJ' in self.cmaq.keys):
                    print 'Interpolating FEf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='FEf', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='AFEJ') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqfe = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'ALf':
                if ('AALF' in self.cmaq.keys):
                    print 'Interpolating ALf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='ALf', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='AALF') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqal = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'MNf':
                if ('AMNJ' in self.cmaq.keys):
                    print 'Interpolating MNf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='MNf', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='AMNJ') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqmn = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'OCf':
                if ('APOCJ' in self.cmaq.keys):
                    print 'Interpolating OCf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='OCf', improve_param=i)
                    cmaqvar = self.cmaq.get_surface_cmaqvar(param='OC') * fac
                    dfpm = self.interp_to_improve(cmaqvar, dfpm, interp=interp, r=radius, weight_func=weight_func)
                    self.cmaqmn = cmaqvar
                    dfs.append(dfpm)
                else:
                    pass

        self.df = concat(dfs)
        self.df.SCS = self.df.SCS.values.astype('int32')
        self.print_available()

    def print_available(self):
        print 'Available Functions:\n'
        print '    improve_spatial(df, param=\'OZONE\', path='', region='', date=\'YYYY-MM-DD HH:MM:SS\')'
        print '    compare_param(param=\'PM2.5\', state=\'\', region=\'\', timeseries=False, scatter=False, pdfs=False,diffscatter=False, diffpdfs=False,timeseries_error=False)\n'
        print 'Species available to compare:'
        print '    ', self.df.Species.unique()

    def compare_param(self, param='OZONE', improvesite='', epasite='', state='', region='', timeseries=False,
                      scatter=False, pdfs=False, diffscatter=False, diffpdfs=False, timeseries_rmse=False,
                      timeseries_mb=False, fig=None, label=None, footer=True):
        df = self.df.copy()
        g = df.groupby('Species')
        new = g.get_group(param)
        if epasite != '':
            if epasite in new.SCS.unique():
                df2 = new.loc[new.SCS == epasite]
        elif improvesite != '':
            if improvesite in new.Site_Code.unique():
                df2 = new.loc[new.Site_Code == improvesite]
        elif state != '':
            names = df.State_Name.dropna().unique()
            for i in names:
                if state.upper() in i.upper():
                    name = i
            df2 = new[new['State'] == name].copy().drop_duplicates()
            title = name
        elif region != '':
            df2 = new[new['Region'].str.upper() == region.upper()].copy().drop_duplicates()
            title = region
        else:
            df2 = new
            title = 'Domain'
        if timeseries:
            plots.airnow_timeseries_param(df2, title=title, label=label, fig=fig, footer=footer, sample='3D')
        if scatter:
            plots.airnow_scatter_param(df2, title=title, label=label, fig=fig, footer=footer)
        if pdfs:
            plots.airnow_kdeplots_param(df2, title=title, label=label, fig=fig, footer=footer)
        if diffscatter:
            plots.airnow_diffscatter_param(df2, title=title)
        if diffpdfs:
            plots.airnow_diffpdfs_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_rmse:
            plots.airnow_timeseries_rmse_param(df2, title=title, label=label, fig=fig, footer=footer, sample='3D')
        if timeseries_mb:
            plots.airnow_timeseries_mb_param(df2, title=title, label=label, fig=fig, footer=footer, sample='3D')

    def improve_spatial(self, df, date, param='NAf', path='', region='', xlim=[], ylim=[]):
        """
        :param param: Species Parameter: 
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
        index = where(self.cmaq.dates == datetime.strptime(date, '%Y-%m-%d %H:%M'))[0][0]
        c = plots.make_spatial_plot(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index], m)
        plots.airnow_spatial_scatter(df2, m, self.cmaq.dates[index].strftime('%Y-%m-%d %H:%M:%S'))
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

    def interp_to_improve(self, cmaqvar, df, interp='nearest', r=12000., n=5, weight_func=lambda r: 1 / r ** 2):
        import pandas as pd
        from numpy import unique
        from pyresample import geometry, kd_tree
        from numpy import vstack, NaN
        grid1 = geometry.GridDefinition(lons=self.cmaq.longitude, lats=self.cmaq.latitude)
        dates = self.cmaq.dates[self.cmaq.indexdates]
        # only interpolate to sites with latitude and longitude
        df.dropna(subset=['Latitude', 'Longitude'], inplace=True)
        vals, index = unique(df.Site_Code, return_index=True)
        lats = df.Latitude.values[index]
        lons = df.Longitude.values[index]
        grid2 = geometry.GridDefinition(lons=vstack(lons), lats=vstack(lats))
        sites = df.Site_Code.values[index]
        dfs = []
        vals = pd.Series(dtype=df.Obs.dtype)
        date = pd.Series(dtype=df.datetime.dtype)
        site = pd.Series(dtype=df.Site_Code.dtype)
        for i, j in enumerate(self.cmaq.indexdates):
            if interp.lower() == 'idw':
                val = kd_tree.resample_custom(grid1, cmaqvar[i, :, :].squeeze(), grid2, radius_of_influence=r,
                                              fill_value=NaN, neighbours=n, weight_funcs=weight_func,
                                              nprocs=2).squeeze()
            elif interp.lower() == 'gauss':
                val = kd_tree.resample_gauss(grid1, cmaqvar[i, :, :].squeeze(), grid2, radius_of_influence=r,
                                             sigmas=r / 2., fill_value=NaN, neighbours=n, nprocs=2).squeeze()
            else:
                interp = 'nearest'
                val = kd_tree.resample_nearest(grid1, cmaqvar[i, :, :].squeeze(), grid2, radius_of_influence=r,
                                               fill_value=NaN, nprocs=2).squeeze()
            vals = vals.append(pd.Series(val)).reset_index(drop=True)
            date = date.append(pd.Series([self.cmaq.dates[j] for k in lons])).reset_index(drop=True)
            site = site.append(pd.Series(sites)).reset_index(drop=True)
        dfs = pd.concat([vals, date, site], axis=1, keys=['CMAQ', 'datetime', 'Site_Code'])
        dfs.index=dfs.datetime
        r = dfs.groupby('Site_Code').get_group(dfs.Site_Code.unique()[0]).resample('D').mean().reset_index()
        g = dfs.groupby('Site_Code')
        for i in df.Site_Code.unique()[1:]:
            q = dfs.groupby('Site_Code').get_group(i)
            q = q.resample('D').mean().reset_index()
            q['Site_Code'] = i
            r = pd.concat([r,q],ignore_index=True)
        df = pd.merge(df, r, how='left', on=['Site_Code', 'datetime']).dropna(subset=['CMAQ'])
        df.Obs.loc[df.Obs < 0] = NaN
        df.dropna(subset=['Obs'],inplace=True)
        return df

    def check_cmaq_units(self, param='O3', improve_param='OZONE'):
        aunit = self.improve.df[self.improve.df.Species == improve_param].Units.unique()[0]
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
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        con = ((self.improve.df.Latitude.values > lat.min()) & (self.improve.df.Latitude.values < lat.max()) & (
            self.improve.df.Longitude.values > lon.min()) & (self.improve.df.Longitude.values < lon.max()))
        self.improve.df = self.improve.df[con].copy()
