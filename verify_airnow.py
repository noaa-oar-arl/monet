# verify is the main application.
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from numpy import array, where
from datetime import datetime

import mystats
import plots
from airnow import airnow
from cmaq import cmaq

sns.set_style('whitegrid')


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
        self.cmaqso2 = None
        self.cmaqco = None
        self.df8hr = None

    def combine(self):
        # get the lats and lons for CMAQ
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        # get the CMAQ dates
        print 'Acquiring Dates of CMAQ Simulation'
        print '==============================================================='
        self.cmaq.get_dates()
        print 'Simulation Start Date: ', self.cmaq.dates[0].strftime('%Y-%m-%d %H:%M')
        print 'Simulation End   Date: ', self.cmaq.dates[-1].strftime('%Y-%m-%d %H:%M')
        print '==============================================================='
        self.ensure_values_indomain()
        comparelist = ['OZONE', 'PM2.5', 'NOX', 'CO', 'SO2', 'NOY']
        g = self.airnow.df.groupby('Species')
        dfs = []
        for i in comparelist:
            if i == 'OZONE':
                print 'Interpolating Ozone:'
                dfo3 = g.get_group(i)
                fac = self.check_cmaq_units(param='O3', airnow_param=i)
                cmaq = self.cmaq.get_surface_cmaqvar(param='O3') * fac
                self.cmaqo3 = cmaq
                dfo3 = self.interp_to_airnow(cmaq, dfo3)
                dfs.append(dfo3)
            elif i == 'PM2.5':
                print 'Interpolating PM2.5:'
                dfpm25 = g.get_group(i)
                fac = self.check_cmaq_units(param='PM25', airnow_param=i)
                cmaq = self.cmaq.get_surface_cmaqvar(param='PM25') * fac
                dfpm25 = self.interp_to_airnow(cmaq, dfpm25)
                self.cmaqpm25 = cmaq
                dfs.append(dfpm25)
            elif i == 'CO':
                if 'CO' not in self.cmaq.cdfobj.variables.keys():
                    pass
                else:
                    print 'Interpolating CO:'
                    dfco = g.get_group(i)
                    fac = self.check_cmaq_units(param='CO', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='CO') * fac
                    dfco = self.interp_to_airnow(cmaq, dfco)
                    self.cmaqco = cmaq
                    dfs.append(dfco)
            elif i == 'NOY':
                if 'NOY' not in self.cmaq.cdfobj.variables.keys():
                    pass
                else:
                    print 'Interpolating NOY:'
                    dfnoy = g.get_group(i)
                    fac = self.check_cmaq_units(param='NOY', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NOY') * fac
                    dfnoy = self.interp_to_airnow(cmaq, dfnoy)
                    self.cmaqnoy = cmaq
                    dfs.append(dfnoy)
            elif i == 'SO2':
                if 'SO2' not in self.cmaq.cdfobj.variables.keys():
                    pass
                else:
                    print 'Interpolating SO2'
                    dfso2 = g.get_group(i)
                    fac = self.check_cmaq_units(param='SO2', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='SO2') * fac
                    dfso2 = self.interp_to_airnow(cmaq, dfso2)
                    self.cmaqso2 = cmaq
                    dfs.append(dfso2)
            elif i == 'NOX':
                if ('NO' not in self.cmaq.cdfobj.variables.keys()) | ('NO2' not in self.cmaq.cdfobj.variables.keys()):
                    pass
                else:
                    print 'Interpolating NOX:'
                    dfnox = g.get_group(i)
                    fac = self.check_cmaq_units(param='NOX', airnow_param=i)
                    cmaq = self.cmaq.get_surface_cmaqvar(param='NOX') * fac
                    dfnox = self.interp_to_airnow(cmaq, dfnox)
                    self.cmaqnox = cmaq
                    dfs.append(dfnox)

        self.df = pd.concat(dfs)
        print 'Calculating Daily 8 Hr Max Ozone....\n'
        self.df8hr = self.calc_airnow_8hr_max_calc()
        print 'Ready to Compare!!!!!!!!!!!!!\n'
        print 'Available Functions:\n'
        print '    compare_domain_all(timeseries=False, scatter=False, bargraph=False, pdfs=False, spatial=False)'
        print '    compare_region_all(timeseries=False, scatter=False, bargraph=False, pdfs=False, spatial=False)'
        print '    airnow_spatial(df, param=\'OZONE\', path='', region='', date=\'YYYY-MM-DD HH:MM:SS\')'
        print '    airnow_spatial_8hr(df, path=\'cmaq-basemap-conus.p\', region=\'northeast\', date=\'YYYY-MM-DD\')'
        print '    compare_param(param=\'OZONE\', city=\'\', region=\'\', timeseries=False, scatter=False, pdfs=False,diffscatter=False, diffpdfs=False,timeseries_error=False)\n'
        print 'Species available to compare:'
        print '    OZONE, PM2.5, PM10, CO, SO2, NOx, NOy'

    def compare_domain_all(self, timeseries=False, scatter=False, bargraph=True, pdfs=False):

        from numpy import NaN
        df = self.df.copy()[['datetime', 'datetime_local', 'Obs', 'CMAQ', 'Species', 'Region', 'SCS', 'Units']]
        df[df < -990] = NaN
        if timeseries:
            plots.airnow_timeseries(df, title='Domain Timeseries')
        if scatter:
            plots.airnow_scatter(df)
        if bargraph:
            plots.airnow_domain_bar(df)
        if pdfs:
            plots.airnow_kdeplots(df)

    def compare_region_all(self, timeseries=False, scatter=False, bargraph=True, pdfs=False, region='all'):
        from numpy import NaN
        region = region.upper()
        df = self.df.copy()[['datetime', 'datetime_local', 'Obs', 'CMAQ', 'Species', 'Region', 'SCS', 'Units']]
        df[df < -990] = NaN
        if region == 'ALL':
            regions = ['Northeast', 'Southeast', 'North Central', 'South Central', 'Rockies', 'Pacific']
            for i in regions:
                df2 = df[df['Region'] == i].copy()
                if timeseries:
                    plots.airnow_timeseries(df2, title=i + ' Timeseries')
                if scatter:
                    plots.airnow_scatter(df2)
                if bargraph:
                    plots.airnow_domain_bar(df2)
                if pdfs:
                    plots.airnow_kdeplots(df2)
        else:
            i = region
            df2 = df[df['Region'].str.upper() == region].copy()
            if timeseries:
                plots.airnow_timeseries(df2, title=i + ' Timeseries')
            if scatter:
                plots.airnow_scatter(df2)
            if bargraph:
                plots.airnow_domain_bar(df2)
            if pdfs:
                plots.airnow_kdeplots(df2)

    def compare_param(self, param='OZONE', city='', region='', timeseries=False, scatter=False, pdfs=False,
                      diffscatter=False, diffpdfs=False,timeseries_error=False):
        from numpy import NaN
        df = self.df.copy()[[
            'datetime', 'datetime_local', 'Obs', 'CMAQ', 'Species', 'MSA_Name', 'Region', 'SCS', 'Units', 'Latitude',
            'Longitude']]
        df[df < -990] = NaN
        g = df.groupby('Species')
        new = g.get_group(param)
        if city != '':
            names = df.MSA_Name.dropna().unique()
            for i in names:
                if city.upper() in i.upper():
                    name = i
                    print name
            df2 = new[new['MSA_Name'] == name].copy().drop_duplicates()
            title = name
        elif region != '':
            df2 = new[new['Region'].str.upper() == region.upper()].copy().drop_duplicates()
            title = region
        else:
            df2 = new
            name = 'Entire Domain'
            title = 'Domain'
        if timeseries:
            plots.airnow_timeseries_param(df2, title=title)
        if scatter:
            plots.airnow_scatter_param(df2, title=title)
        if pdfs:
            plots.airnow_kdeplots_param(df2, title=title)
        if diffscatter:
            plots.airnow_diffscatter_param(df2, title=title)
        if diffpdfs:
            plots.airnow_diffpdfs_param(df2, title=title)
        if timeseries_error:
            plots.airnow_timeseries_error_param(df2, title=title)

    def compare_metro_area(self, city='Philadelphia', param='OZONE', date='', timeseries=False, scatter=False,
                           bargraph=False, pdfs=False, spatial=False, path=''):
        from numpy import NaN, unique
        from tools import search_listinlist
        df = self.df.copy()[
            ['datetime', 'datetime_local', 'Obs', 'CMAQ', 'Species', 'MSA_Name', 'SCS', 'Units', 'Latitude',
             'Longitude']]
        df[df < -990] = NaN
        df = df.dropna()
        names = df.MSA_Name.dropna().unique()

        for i in names:
            if city.upper() in i.upper():
                name = i

        index1, index2 = search_listinlist(array(names), array(city))
        df2 = df[df['MSA_Name'] == name]
        if timeseries:
            plots.airnow_timeseries(df2, title=name)
        if scatter:
            plots.airnow_scatter(df2)
        if bargraph:
            plots.airnow_domain_bar(df2)
        if pdfs:
            plots.airnow_kdeplots(df2)
        if spatial:
            m = self.cmaq.load_conus_basemap(path)
            lats, index = unique(df2.Latitude.values, return_index=True)
            lons = df2.Longitude.values[index]
            print lats.mean(), lons.mean()
            xlim = array([lons.mean() - 3., lons.mean() + 3.])
            ylim = array([lats.mean() - 1., lats.mean() + 1.])
            x, y = m(xlim, ylim)
            if date == '':
                self.airnow_spatial(df2, param=param, path=path, xlim=x, ylim=y)
            else:
                self.airnow_spatial(df2, param=param, path=path, date=date, xlim=x, ylim=y)


    def compare_metro_area_8hr(self, city='Philadelphia', param='OZONE', date='', timeseries=False, scatter=False,
                               bargraph=False, pdfs=False, spatial=False, path=''):
        from numpy import NaN, unique
        from tools import search_listinlist
        df = self.df8hr.copy()[
            ['datetime', 'datetime_local', 'Obs', 'CMAQ', 'Species', 'MSA_Name', 'SCS', 'Units', 'Latitude',
             'Longitude']]
        df[df < -990] = NaN
        df = df.dropna()
        names = df.MSA_Name.dropna().unique()

        for i in names:
            if city.upper() in i.upper():
                name = i
        index1, index2 = search_listinlist(array(names), array(city))
        df2 = df[df['MSA_Name'] == name]
        if timeseries:
            plots.airnow_timeseries(df2, title=name)
        if scatter:
            plots.airnow_scatter(df2)
        if bargraph:
            plots.airnow_domain_bar(df2)
        if pdfs:
            plots.airnow_kdeplots(df2)
        if spatial:
            m = self.cmaq.load_conus_basemap(path)
            lats, index = unique(df2.Latitude.values, return_index=True)
            lons = df2.Longitude.values[index]
            print lats.mean(), lons.mean()
            xlim = array([lons.mean() - 3., lons.mean() + 3.])
            ylim = array([lats.mean() - 1., lats.mean() + 1.])
            x, y = m(xlim, ylim)
            if date == '':
                self.airnow_spatial(df2, param=param, path=path, xlim=x, ylim=y)
            else:
                self.airnow_spatial(df2, param=param, path=path, date=date, xlim=x, ylim=y)

    def airnow_spatial(self, df, param='OZONE', path='', region='', date='', xlim=[], ylim=[]):
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
            c = plots.make_spatial_plot(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index], m)
            plots.airnow_spatial_scatter(df2, m, self.cmaq.dates[index].strftime('%Y-%m-%d %H:%M:%S'))
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
                plots.airnow_8hr_spatial_scatter(self.df8hr, m, i.strftime('%Y-%m-%d'))
                c = plt.colorbar()
                c.set_label('8Hr Ozone Bias (pbbV)')
                plt.title(i.strftime('%Y-%m-%d') + ' Obs - CMAQ')
                plt.tight_layout()
                if len(xlim) > 1:
                    plt.xlim([min(xlim), max(xlim)])
                    plt.ylim([min(ylim), max(ylim)])
        else:
            plots.airnow_8hr_spatial_scatter(self.df8hr, m, date=date)
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

    def interp_to_airnow(self, cmaqvar, df):
        from scipy.interpolate import griddata
        dates = self.cmaq.dates[self.cmaq.indexdates]
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        con = df.datetime == self.cmaq.dates[self.cmaq.indexdates][0]
        new = df[con]
        print '   Interpolating values to AirNow, Date : ', self.cmaq.dates[self.cmaq.indexdates][0].strftime('%B %d %Y   %H utc')
        cmaq_val = pd.DataFrame(griddata((lon.flatten(), lat.flatten()), cmaqvar[self.cmaq.indexdates[0], :, :].flatten(),
                                         (new.Longitude.values, new.Latitude.values), method='nearest'),
                                columns=['CMAQ'], index=new.index)
        new = new.join(cmaq_val)
        for i, j in enumerate(self.cmaq.dates[self.cmaq.indexdates][1:]):
            print '   Interpolating values to AirNow, Date : ', j.strftime('%B %d %Y   %H utc')
            con = df.datetime == j
            newt = df[con]
            cmaq_val = pd.DataFrame(griddata((lon.flatten(), lat.flatten()), cmaqvar[i+1, :, :].flatten(),
                                             (newt.Longitude.values, newt.Latitude.values), method='nearest'),
                                    columns=['CMAQ'], index=newt.index)
            newt = newt.join(cmaq_val)
            new = new.append(newt)
        return new

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
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        con = ((self.airnow.df.Latitude.values > lat.min()) & (self.airnow.df.Latitude.values < lat.max()) & (
            self.airnow.df.Longitude.values > lon.min()) & (self.airnow.df.Longitude.values < lon.max()))
        self.airnow.df = self.airnow.df[con].copy()

    def calc_airnow_8hr_max_calc(self, path=''):
        r = self.df.groupby('Species').get_group('OZONE')
        r.index = r.datetime_local
        g = r.groupby('SCS')['Obs', 'CMAQ', 'Latitude', 'Longitude']
        m = g.rolling(8, center=True, win_type='boxcar').mean()
        q = m.reset_index(level=0)
        # q.index = self.df.groupby('Species').get_group('OZONE').datetime_local
        k = q.groupby('SCS').resample('1D').max()
        kk = k.reset_index(level=1)
        kkk = kk.reset_index(drop='SCS').dropna()
        if path == '':
            path = 'monitoring_site_locations.dat'
        kkk = self.airnow.get_station_locations_remerge(kkk, path=path)
        return kkk
