# verify is the main application.
from datetime import datetime

import matplotlib.pyplot as plt
from pandas import DataFrame,concat
from numpy import array, where

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
        self.df8hr = None

    def combine(self):
        print 'Acquiring Dates of CMAQ Simulation'
        print '==============================================================='
        self.cmaq.get_dates()
        print 'Simulation Start Date: ', self.cmaq.dates[0].strftime('%Y-%m-%d %H:%M')
        print 'Simulation End   Date: ', self.cmaq.dates[-1].strftime('%Y-%m-%d %H:%M')
        print '===============================================================\n'
        if self.cmaq.metcro2d is None:
            print 'METCRO2D file not loaded.  To include MET variables please load self.cmaq.open_metcro2d(\'filename\')\n'
        self.ensure_values_indomain()
        comparelist = self.improve.df.Species.unique()
        g = self.improve.df.groupby('Species')
        dfs = []
        for i in comparelist:
            if i == 'CLf':
                if ('ACLI' in self.cmaq.keys) | ('ACLJ' in self.cmaq.keys) | ('PM25_CL' in self.cmaq.keys):
                    print 'Interpolating CLf:'
                    dfpm25 = g.get_group(i)
                    fac = self.check_cmaq_units(param='CLf', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='CLf') * fac
                    dfpm25 = self.interp_to_improve(cmaq, dfpm25)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm25)
                else:
                    pass
            elif i == 'PM10':
                print 'Interpolating PM10:'
                dfpm = g.get_group(i)
                fac = self.check_cmaq_units(param='PM10', improve_param=i)
                cmaq = self.cmaq.get_surface_cmaqvar(param='PM10') * fac
                dfpm = self.interp_to_improve(cmaq, dfpm)
                self.cmaqpm25 = cmaq
                dfs.append(dfpm)
            elif i == 'PM2.5':
                print 'Interpolating PM25:'
                dfpm = g.get_group(i)
                fac = self.check_cmaq_units(param='PM25', improve_param=i)
                cmaq = self.cmaq.get_surface_cmaqvar(param='PM25') * fac
                dfpm = self.interp_to_improve(cmaq, dfpm)
                self.cmaqpm25 = cmaq
                dfs.append(dfpm)
            elif i == 'NAf':
                if ('ANAI' in self.cmaq.keys) | ('ANAJ' in self.cmaq.keys) | ('PM25_NA' in self.cmaq.keys):
                    print 'Interpolating NAf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='NAf', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NAf') * fac
                    dfpm = self.interp_to_improve(cmaq, dfpm)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'MGf':
                if ('AMGI' in self.cmaq.keys) | ('AMGJ' in self.cmaq.keys) | ('PM25_MG' in self.cmaq.keys):
                    print 'Interpolating MGf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='MGf', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='MGf') * fac
                    dfpm = self.interp_to_improve(cmaq, dfpm)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'Kf':
                if ('AKI' in self.cmaq.keys) | ('AKJ' in self.cmaq.keys) | ('PM25_K' in self.cmaq.keys):
                    print 'Interpolating Kf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='Kf', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='Kf') * fac
                    dfpm = self.interp_to_improve(cmaq, dfpm)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'CAf':
                if ('ACAI' in self.cmaq.keys) | ('ACAJ' in self.cmaq.keys) | ('PM25_CA' in self.cmaq.keys):
                    print 'Interpolating CAf:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='CAf', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='CAf') * fac
                    dfpm = self.interp_to_improve(cmaq, dfpm)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'SO4f':
                if ('ASO4I' in self.cmaq.keys) | ('ASO4J' in self.cmaq.keys) | ('PM25_SO4' in self.cmaq.keys):
                    print 'Interpolating SO4f:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='SO4f', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='SO4f') * fac
                    dfpm = self.interp_to_improve(cmaq, dfpm)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'NH4f':
                if ('ANH4I' in self.cmaq.keys) | ('ANH4J' in self.cmaq.keys) | ('PM25_NH4' in self.cmaq.keys):
                    print 'Interpolating NH4f:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='NH4f', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NH4f') * fac
                    dfpm = self.interp_to_improve(cmaq, dfpm)
                    self.cmaqpm25 = cmaq
                    dfs.append(dfpm)
                else:
                    pass
            elif i == 'NO3f':
                if ('ANO3I' in self.cmaq.keys) | ('ANO3J' in self.cmaq.keys) | ('PM25_NO3' in self.cmaq.keys):
                    print 'Interpolating NO3f:'
                    dfpm = g.get_group(i)
                    fac = self.check_cmaq_units(param='NO3f', improve_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NO3f') * fac
                    dfpm = self.interp_to_improve(cmaq, dfpm)
                    self.cmaqpm25 = cmaq
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
        from numpy import NaN
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
            plots.improve_timeseries_param(df2, title=title, label=label, fig=fig, footer=footer)
        if scatter:
            plots.improve_scatter_param(df2, title=title, label=label, fig=fig, footer=footer)
        if pdfs:
            plots.improve_kdeplots_param(df2, title=title, label=label, fig=fig, footer=footer)
        if diffscatter:
            plots.improve_diffscatter_param(df2, title=title)
        if diffpdfs:
            plots.improve_diffpdfs_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_rmse:
            plots.improve_timeseries_rmse_param(df2, title=title, label=label, fig=fig, footer=footer)
        if timeseries_mb:
            plots.improve_timeseries_mb_param(df2, title=title, label=label, fig=fig, footer=footer)

    def improve_spatial(self, df, date,param='NAf', path='', region='', xlim=[], ylim=[]):
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
        plots.improve_spatial_scatter(df2, m, self.cmaq.dates[index].strftime('%Y-%m-%d %H:%M:%S'))
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

    def interp_to_improve(self, cmaqvar, df):
        import pandas as pd
        from numpy import unique,isnan
        from scipy.interpolate import griddata
        from tools import search_listinlist
        dates = self.cmaq.dates[self.cmaq.indexdates]
        #only interpolate to sites with latitude and longitude
        df.dropna(subset=['Latitude','Longitude'],inplace=True)
        vals,index = unique(df.Site_Code,return_index=True)
        lats = df.Latitude.values[index]
        lons = df.Longitude.values[index]
        sites = df.Site_Code.values[index]
        lat = self.cmaq.latitude
        lon = self.cmaq.longitude
        dfs = []
        vals = pd.Series(dtype=df.Obs.dtype)
        date = pd.Series(dtype=df.datetime.dtype)
        site = pd.Series(dtype=df.Site_Code.dtype)
        for i,j in enumerate(self.cmaq.indexdates):
            print '   Interpolating values to IMPROVE Sites. Date : ', self.cmaq.dates[j].strftime('%B %d %Y   %H utc')
            vals = vals.append(pd.Series(griddata((lon.flatten(), lat.flatten()), cmaqvar[i, :, :].flatten(),(lons, lats), method='nearest'))).reset_index(drop=True)
            date = date.append(pd.Series([self.cmaq.dates[j] for k in lons])).reset_index(drop=True)
            site = site.append(pd.Series(sites)).reset_index(drop=True)
        dfs = pd.concat([vals,date,site],axis=1,keys=['CMAQ','datetime','Site_Code'])

        g = dfs.groupby('Site_Code')
        q = pd.DataFrame()
        for i in dfs.Site_Code.unique():
            c = g.get_group(i)
            c.index = c.datetime
            c.drop('datetime',axis=1,inplace=True)
            r = c.rolling(window=72).mean() #boxcar smoother
            r.reset_index(inplace=True)
            q = q.append(r).reset_index(drop=True)
        df = pd.merge(df,q,how='left',on=['Site_Code','datetime']).dropna(subset=['CMAQ'])

        return df

    def interp_to_improve_unknown(self, cmaqvar, df, varname):
        from scipy.interpolate import griddata
        dates = self.cmaq.dates[self.cmaq.indexdates]
        lat = self.cmaq.latitude
        lon = self.cmaq.longitude

        con = df.datetime == self.cmaq.dates[self.cmaq.indexdates][0]
        new = df[con]
        print '   Interpolating values to AirNow, Date : ', self.cmaq.dates[self.cmaq.indexdates][0].strftime(
                '%B %d %Y   %H utc')
        cmaq_val = DataFrame(
                griddata((lon.flatten(), lat.flatten()), cmaqvar[self.cmaq.indexdates[0], :, :].flatten(),
                         (new.Longitude.values, new.Latitude.values), method='nearest'),
                columns=[varname], index=new.index)
        new = new.join(cmaq_val)
        for i, j in enumerate(self.cmaq.dates[self.cmaq.indexdates][1:]):
            print '   Interpolating values to AirNow, Date : ', j.strftime('%B %d %Y %H utc')
            con = df.datetime == j
            newt = df[con]
            cmaq_val = DataFrame(griddata((lon.flatten(), lat.flatten()), cmaqvar[i + 1, :, :].flatten(),
                                             (newt.Longitude.values, newt.Latitude.values), method='nearest'),
                                    columns=[varname], index=newt.index)
            newt = newt.join(cmaq_val)
            new = new.append(newt)
        return new

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
