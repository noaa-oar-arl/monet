# verify is the main application.
from airnow import airnow
from cmaq import cmaq
from aqs import aqs
from scipy.interpolate import griddata
from numpy import empty, ceil, unique
import pandas as pd
import mystats


class verify:
    def __init__(self):
        self.airnow = airnow()
        self.cmaq = cmaq()
        self.aqs = aqs()

    def compare_aqs_24h_pm25(self, statecompare=True, spatial=False, scatter=False, time=False, firstday=True):
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        print 'Getting CMAQ values'
        cmaq = self.cmaq.get_surface_cmaqvar(param='pm25')
        self.cmaq.get_cmaq_dates()
        self.cmaq.dates, index = unique(self.cmaq.dates, return_index=True)
        self.aqs.load_aqs_daily_pm25_data(self.cmaq.dates)
        self.ensure_values_indomain()
        cmaq = cmaq[index, :, :]

        print 'Interpolating values to AQS Surface Sites for 24H Mean PM25, Date : ', self.cmaq.dates[0].strftime(
                '%B %d %Y   %H utc')
        data = self.interp_to_aqs_sites_daily_pm25(cmaq)

        if statecompare:
            # If Scatter == True then it will create scatter plots for each state and all sites within it
            # IF Time    == True then it will create average time series plots for each state
            # If stat    == true then it will create a statistical bar graph for 8 statistics for each state
            statestats = self.aqs_state_comparison_24h_pm25(data, scatter=scatter, time=time)

        if spatial:
            # this will loop over each date and create the spatial plot with comparisons to observational data overlayed
            vmin = 1.
            vmax = ceil(data['Obs_value'].median() + data['Obs_value'].std())
            for index, i in enumerate(self.cmaq.dates):
                cmaqvar = cmaq[index, :, :].squeeze()
                # m, c = cr.make_spatial_plot(self.cmaq.cdfobj, self.cmaq.gridobj, self.cmaq.dates[index], vmin=vmin,
                #                            vmax=vmax,
                #                            pick='cmaq_grids/basemap-cmaq_conus.p')
                # cr.improve_spatial_scatter(improve, m, i.strftime('%Y-%m-%d'), 'Obs_value', vmin=vmin, vmax=vmax)

        data.to_hdf(self.cmaq.dates[0].strftime('%Y') + '_interpolated_data.hdf', 'df', format='table')

    def compare_aqs_hourly(self, param='O3', statecompare=True, spatial=True, scatter=False, time=False):
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        param = param.upper()
        fac = self.check_cmaq_units(param=param)
        cmaq = self.cmaq.get_surface_cmaqvar(param=param) * fac
        self.cmaq.get_cmaq_dates()

        try:
            aqs = self.choose_aqs_data(param=param)
        except ValueError, e:
            return

        self.ensure_values_indomain()
        print 'Interpolating values to AQS Surface Sites for O3, Date : ', self.cmaq.dates[0].strftime(
                '%B %d %Y   %H utc')
        data = self.interp_to_aqs_sites(cmaq, self.aqs.aqsdf, self.cmaq.gridobj, self.cmaq.dates)

        if statecompare:
            ### If Scatter == True then it will create scatter plots for each state and all sites within it
            # IF Time    == True then it will create average time series plots for each state
            ### If stat    == true then it will create a statistical bar graph for 8 statistics for each state
            statestats = self.aqs_state_comparison(data, scatter=scatter, time=time, stat=statecompare)

        if spatial:
            # this will loop over each date and create the spatial plot with comparisons to observational data overlayed
            vmin = 1.
            vmax = ceil(aqs['Obs_value'].median() + aqs['Obs_value'].std())
            for index, i in enumerate(self.cmaq.dates):
                cmaqvar = cmaq[index, :, :].squeeze()
                # m, c = cr.make_spatial_plot(self.cmaq.cdfobj, self.self.cmaq.gridobj, self.cmaq.dates[index], vmin=vmin,vmax=vmax,pick='cmaq_grids/basemap-cmaq_conus.p')
                # cr.improve_spatial_scatter(improve, m, i.strftime('%Y-%m-%d %H:%M:%S'), 'Obs_value', vmin=vmin, vmax=vmax)

    def aqs_state_comparison_24h_pm25(self, df, statecompare=True, spatial=True, scatter=False, time=False):
        import matplotlib.pyplot as plt
        import seaborn as sns

        sns.set_style('whitegrid')
        first = True
        for k, i in enumerate(df['State_Name'].unique()):
            temp = df[df['State_Name'] == i]

            mnb, mdnb, fe, r2, d1, e1, ioa, rmse = self.calc_stats(temp)

            # put the stats in a dfframe
            if first:
                s1 = pd.Series([mnb], index=[k], name='Normalized Mean Bias')
                s2 = pd.Series([fe], index=[k], name='Fractional Error')
                s3 = pd.Series([r2], index=[k], name='R**2')
                s4 = pd.Series([d1], index=[k], name='Modified Index of Agreement; d1')
                s5 = pd.Series([e1], index=[k], name='E1')
                s6 = pd.Series([ioa], index=[k], name='Index of Agreement')
                s7 = pd.Series([i], index=[k], name='State_Name')
                s8 = pd.Series([mdnb], index=[k], name='Anomaly Correlation')
                s9 = pd.Series([rmse], index=[k], name='RMSE')
                first = False
            else:
                s1 = s1.append(pd.Series([mnb], index=[k], name='Normalized Mean Bias'))
                s2 = s2.append(pd.Series([fe], index=[k], name='Fractional Error'))
                s3 = s3.append(pd.Series([r2], index=[k], name='R**2'))
                s4 = s4.append(pd.Series([d1], index=[k], name='Modified Index of Agreement; d1'))
                s5 = s5.append(pd.Series([e1], index=[k], name='E1'))
                s6 = s6.append(pd.Series([ioa], index=[k], name='Index of Agreement'))
                s7 = s7.append(pd.Series([i], index=[k], name='State_Name'))
                s8 = s8.append(pd.Series([mdnb], index=[k], name='Anomaly Correlation'))
                s9 = s9.append(pd.Series([rmse], index=[k], name='RMSE'))

                # make scatterplot
            if scatter:
                sns.jointplot(x='Obs_value', y='cmaq', data=temp, kind='reg')
                plt.xlabel('Obs ' + temp['Units_of_Measure'].unique())
                plt.ylabel('CMAQ ' + temp['Units_of_Measure'].unique())
                plt.title(str(i) + '  ' + str(temp['cmaq'].count()) + ' Measurements ' + str(
                        unique(temp.SCS.values).shape[0]) + ' Sites')
                plt.tight_layout()
            if time:
                plt.figure(figsize=(12, 6))
                # cr.plot_timeseries(temp, siteave=True)

        stats = pd.concat([s1, s2, s3, s4, s5, s6, s7, s8, s9], axis=1).reset_index()
        f = plt.figure(figsize=(18, 10))
        ax1 = f.add_subplot(221)
        ax2 = f.add_subplot(222)
        ax3 = f.add_subplot(223, sharex=ax1)
        ax4 = f.add_subplot(224, sharex=ax2)

        sns.barplot(x='State_Name', y='Normalized Mean Bias', data=stats, ax=ax1, color='steelblue')
        ax1.set_ylabel('Normalized Mean Bias');
        ax1.set_xlabel('')
        plt.setp(ax2.get_xticklabels(), visible=False)
        plt.setp(ax1.get_xticklabels(), visible=False)
        sns.barplot(x='State_Name', y='Index of Agreement', data=stats, ax=ax2, color='steelblue')
        ax2.set_ylabel('Index of Agreement');
        ax2.set_xlabel('')
        sns.barplot(x='State_Name', y='Modified Index of Agreement; d1', data=stats, ax=ax3, color='steelblue')
        ax3.set_ylabel('Modified Index of Agreement');
        ax3.set_xlabel('')
        sns.barplot(x='State_Name', y='E1', data=stats, ax=ax4, color='steelblue')
        ax4.set_ylabel('E1');
        ax4.set_xlabel('')
        ax3.set_xticklabels(ax3.xaxis.get_majorticklabels(), rotation=87)
        ax4.set_xticklabels(ax4.xaxis.get_majorticklabels(), rotation=87)
        plt.tight_layout()

        f = plt.figure(figsize=(18, 10))
        ax5 = f.add_subplot(221)
        ax6 = f.add_subplot(222)
        ax7 = f.add_subplot(223, sharex=ax5)
        ax8 = f.add_subplot(224, sharex=ax6)
        plt.setp(ax6.get_xticklabels(), visible=False)
        plt.setp(ax5.get_xticklabels(), visible=False)
        sns.barplot(x='State_Name', y='R**2', data=stats, ax=ax5, color='steelblue')
        ax5.set_ylabel('R**2');
        ax5.set_xlabel('')
        sns.barplot(x='State_Name', y='Fractional Error', data=stats, ax=ax6, color='steelblue')
        ax6.set_ylabel('Fractional Error');
        ax6.set_xlabel('')
        sns.barplot(x='State_Name', y='RMSE', data=stats, ax=ax7, color='steelblue')
        ax7.set_ylabel('RMSE');
        ax7.set_xlabel('')
        sns.barplot(x='State_Name', y='Anomaly Correlation', data=stats, ax=ax8, color='steelblue')
        ax8.set_ylabel('Anomaly Correlation');
        ax8.set_xlabel('')
        ax7.set_xticklabels(ax7.xaxis.get_majorticklabels(), rotation=87)
        ax8.set_xticklabels(ax8.xaxis.get_majorticklabels(), rotation=87)
        plt.tight_layout()

        return stats

    def interp_to_aqs_sites(self, cmaqvar, aqs, dates):
        from scipy.interpolate import griddata
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()

        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        con = (aqs.datetime == dates[0]) & (aqs.Latitude.values > lat.min()) & (aqs.Latitude.values < lat.max()) & (
            aqs.Longitude.values > lon.min()) & (aqs.Longitude.values < lon.max())
        new = aqs[con]
        cmaq_val = pd.DataFrame(griddata((lon.flatten(), lat.flatten()), cmaqvar[0, :, :].flatten(),
                                         (new.Longitude.values, new.Latitude.values), method='nearest'),
                                columns=['cmaq'],
                                index=new.index)
        new = new.join(cmaq_val)
        for i, j in enumerate(dates[1:]):
            con = (aqs.datetime == j) & (aqs.Latitude.values > lat.min()) & (aqs.Latitude.values < lat.max()) & (
                aqs.Longitude.values > lon.min()) & (aqs.Longitude.values < lon.max())
            newt = aqs[con]
            cmaq_val = pd.DataFrame(griddata((lon.flatten(), lat.flatten()), cmaqvar[i, :, :].flatten(),
                                             (newt.Longitude.values, newt.Latitude.values), method='nearest'),
                                    columns=['cmaq'], index=newt.index)
            newt = newt.join(cmaq_val)
            new = new.append(newt)
        return new

    def interp_to_aqs_sites_daily_pm25(self, cmaqvar):
        from scipy.interpolate import griddata
        from datetime import timedelta, datetime
        from numpy import unique, array
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()
        self.aqs.load_aqs_daily_pm25_data(self.cmaq.dates)
        #    aqs = aqs[aqs['SCS'] == 720610005]
        aqsn = self.aqs.aqsdf.copy()
        dt = []
        for i in aqsn.utcoffset.values:
            dt.append(timedelta(hours=i))
        aqsn['utc timedelta'] = dt
        aqsn['datetime'] = aqsn['datetime_local'] + dt
        scs, index = unique(aqsn.SCS.values, return_index=True)
        ln = aqsn.Longitude.values[index]
        ll = aqsn.Latitude.values[index]
        utctimedelta = aqsn['utc timedelta'].values[index].astype('M8[s]').astype('O') * 1e-9
        cmaq = pd.DataFrame(
                griddata((lon.flatten(), lat.flatten()), cmaqvar[0, :, :].flatten(), (ln, ll), method='nearest'),
                columns=['cmaq'])
        cmaq['SCS'], cmaq['utc timedelta'] = scs, utctimedelta
        # cmaq['SCS'] = scs
        arr = array([self.cmaq.dates[0] + timedelta(hours=utctimedelta[k] // 3600) for k in range(ll.shape[0])])
        print 'Interpolating values to AQS Surface Sites for PM25 24H, Date : ', self.cmaq.dates[0].strftime(
                '%B %d %Y   %H utc')
        cmaq.index = arr  # + utctimedelta
        print
        for i, j in enumerate(self.cmaq.dates[1:]):
            print 'Interpolating values to AQS Surface Sites for PM25 24H, Date : ', j.strftime('%B %d %Y   %H utc')
            cmaq2 = pd.DataFrame(
                    griddata((lon.flatten(), lat.flatten()), cmaqvar[i, :, :].flatten(), (ln, ll), method='nearest'),
                    columns=['cmaq'])
            cmaq2['SCS'], cmaq2['utc timedelta'] = scs, utctimedelta
            # cmaq2['SCS'] = scs
            arr = array([j + timedelta(hours=utctimedelta[k] // 3600) for k in range(ll.shape[0])])
            cmaq2.index = arr
            cmaq = pd.concat([cmaq, cmaq2])

        scs, index = unique(aqsn.SCS.values, return_index=True)
        i = scs[0]
        cmaqr = cmaq[cmaq.SCS == i]
        cmaqr = cmaqr.resample('24H').mean()
        cmaqr['datetime_local'] = cmaqr.index
        aqsnr = aqsn[aqsn.SCS == i]
        new = pd.merge(cmaqr, aqsnr, on='datetime_local', how='right')
        for i in scs[1:]:
            cmaqr = cmaq[cmaq.SCS == i]
            cmaqr = cmaqr.resample('24H').mean()
            cmaqr['datetime_local'] = cmaqr.index
            aqsnr = aqsn[aqsn.SCS == i]
            new = new.append(pd.merge(cmaqr, aqsnr, on='datetime_local'))

        new = new.reset_index()
        new['SCS'] = new['SCS_x']
        new = new[['cmaq', 'datetime_local', 'State_Code',
                   'County_Code', 'Site_Num', 'Parameter_Code', 'POC', 'Latitude',
                   'Longitude', 'Datum', 'Parameter_Name', 'Sample_Duration',
                   'Pollutant_Standard', 'Units_of_Measure', 'Event_Type',
                   'Observation_Count', 'Observation_Percent', 'Obs_value',
                   '1st_Max_Value', '1st_Max Hour', 'AQI', 'Method_Code',
                   'Method_Name', 'Local_Site_Name', 'Address', 'State_Name',
                   'County_Name', 'City_Name', 'CBSA_Name', 'Date_of_Last_Change',
                   'datetime', 'utcoffset', 'utc timedelta', 'SCS']]
        return new

    def aqs_state_comparison(self, df, statecompare=True, spatial=True, scatter=False, time=False, tit=''):
        import matplotlib.pyplot as plt
        import seaborn as sns
        sns.set_style('whitegrid')
        first = True
        for k, i in enumerate(self.aqs.aqsdf['State_Name'].unique()):
            temp = df[self.aqs.aqsdf['State_Name'] == i]

            # calculate statistics

            mnb, mdnb, fe, r2, d1, e1, ioa, rmse = self.calc_stats(temp)

            # put the stats in a dfframe
            if first:
                s1 = pd.Series([mnb], index=[k], name='Normalized Mean Bias')
                s2 = pd.Series([fe], index=[k], name='Fractional Error')
                s3 = pd.Series([r2], index=[k], name='R**2')
                s4 = pd.Series([d1], index=[k], name='Modified Index of Agreement; d1')
                s5 = pd.Series([e1], index=[k], name='E1')
                s6 = pd.Series([ioa], index=[k], name='Index of Agreement')
                s7 = pd.Series([i], index=[k], name='State_Name')
                s8 = pd.Series([mdnb], index=[k], name='Anomaly Correlation')
                s9 = pd.Series([rmse], index=[k], name='RMSE')
                first = False
            else:
                s1 = s1.append(pd.Series([mnb], index=[k], name='Normalized Mean Bias'))
                s2 = s2.append(pd.Series([fe], index=[k], name='Fractional Error'))
                s3 = s3.append(pd.Series([r2], index=[k], name='R**2'))
                s4 = s4.append(pd.Series([d1], index=[k], name='Modified Index of Agreement; d1'))
                s5 = s5.append(pd.Series([e1], index=[k], name='E1'))
                s6 = s6.append(pd.Series([ioa], index=[k], name='Index of Agreement'))
                s7 = s7.append(pd.Series([i], index=[k], name='State_Name'))
                s8 = s8.append(pd.Series([mdnb], index=[k], name='Anomaly Correlation'))
                s9 = s9.append(pd.Series([rmse], index=[k], name='RMSE'))
            # make scatterplot
            if scatter:
                sns.jointplot(x='Obs_value', y='cmaq', data=temp, kind='reg')
                plt.xlabel('Obs ' + temp['Units_of_Measure'].unique())
                plt.ylabel('CMAQ ' + temp['Units_of_Measure'].unique())
                plt.title('State Code ' + str(i) + ' ' + tit + ': ' + str(temp.count()['index']) + 'Sites')
                plt.tight_layout()
            if time:
                plt.figure(figsize=(12, 6))
                # cr.plot_timeseries(temp, siteave=True)

        stats = pd.concat([s1, s2, s3, s4, s5, s6, s7, s8, s9], axis=1).reset_index()
        f = plt.figure(figsize=(14, 10))
        ax1 = f.add_subplot(221)
        ax2 = f.add_subplot(222)
        ax3 = f.add_subplot(223, sharex=ax1)
        ax4 = f.add_subplot(224, sharex=ax2)

        sns.barplot(x='State_Name', y='Normalized Mean Bias', data=stats, ax=ax1, color='steelblue')
        ax1.set_ylabel('Normalized Mean Bias');
        ax1.set_xlabel('')
        plt.setp(ax2.get_xticklabels(), visible=False)
        plt.setp(ax1.get_xticklabels(), visible=False)
        sns.barplot(x='State_Name', y='Index of Agreement', data=stats, ax=ax2, color='steelblue')
        ax2.set_ylabel('Index of Agreement');
        ax2.set_xlabel('')
        sns.barplot(x='State', y='Modified Index of Agreement; d1', data=stats, ax=ax3, color='steelblue')
        ax3.set_ylabel('Modified Index of Agreement');
        ax3.set_xlabel('')
        sns.barplot(x='State', y='E1', data=stats, ax=ax4, color='steelblue')
        ax4.set_ylabel('E1');
        ax4.set_xlabel('')
        plt.tight_layout()

        f = plt.figure(figsize=(14, 10))
        ax5 = f.add_subplot(221)
        ax6 = f.add_subplot(222)
        ax7 = f.add_subplot(223, sharex=ax5)
        ax8 = f.add_subplot(224, sharex=ax6)
        plt.setp(ax6.get_xticklabels(), visible=False)
        plt.setp(ax5.get_xticklabels(), visible=False)
        sns.barplot(x='State_Name', y='R**2', data=stats, ax=ax5, color='steelblue')
        ax5.set_ylabel('R**2');
        ax5.set_xlabel('')
        sns.barplot(x='State_Name', y='Fractional Error', data=stats, ax=ax6, color='steelblue')
        ax6.set_ylabel('Fractional Error');
        ax6.set_xlabel('')
        sns.barplot(x='State_Name', y='RMSE', data=stats, ax=ax7, color='steelblue')
        ax7.set_ylabel('RMSE');
        ax7.set_xlabel('')
        sns.barplot(x='State_Name', y='Anomaly Correlation', data=stats, ax=ax8, color='steelblue')
        ax8.set_ylabel('Anomaly Correlation');
        ax8.set_xlabel('')
        plt.tight_layout()
        return stats

    def check_cmaq_units(self, param='O3'):
        from numpy import concatenate, array
        if (param == 'PM25') | (param == 'PM10'):
            fac = 1.
        else:
            unit = self.cmaq.cdfobj.variables[param].units
            if unit == 'ppmV':
                fac = 1000.  # convert to ppbV
            else:
                fac = 1.

        return fac

    def calc_stats(self, pandasobj):
        mb = mystats.MB(pandasobj['Obs_value'].values, pandasobj['cmaq'].values)  # mean bias
        mdnb = mystats.AC(pandasobj['Obs_value'].values, pandasobj['cmaq'].values)  # median bias
        fe = mystats.FE(pandasobj['Obs_value'].values, pandasobj['cmaq'].values)  # fractional error
        r2 = mystats.R2(pandasobj['Obs_value'].values, pandasobj['cmaq'].values)  # pearsonr ** 2
        d1 = mystats.d1(pandasobj['Obs_value'].values, pandasobj['cmaq'].values)  # modifed index of agreement
        e1 = mystats.E1(pandasobj['Obs_value'].values,
                        pandasobj['cmaq'].values)  # Modified Coefficient of Efficiency, E1
        ioa = mystats.IOA(pandasobj['Obs_value'].values, pandasobj['cmaq'].values)  # Index of Agreement
        rmse = mystats.RMSE(pandasobj['Obs_value'].values, pandasobj['cmaq'].values)
        return mb, mdnb, fe, r2, d1, e1, ioa, rmse

    def ensure_values_indomain(self):
        lat = self.cmaq.gridobj.variables['LAT'][0, 0, :, :].squeeze()
        lon = self.cmaq.gridobj.variables['LON'][0, 0, :, :].squeeze()

        con = ((self.aqs.aqsdf.Latitude.values > lat.min()) & (self.aqs.aqsdf.Latitude.values < lat.max()) & (
            self.aqs.aqsdf.Longitude.values > lon.min()) & (self.aqs.aqsdf.Longitude.values < lon.max()))
        self.aqs.aqsdf = self.aqs.aqsdf[con].copy()

    def choose_aqs_data(self, param='O3'):
        param = param.upper()
        if param == 'O3':
            aqs = self.aqs.load_aqs_ozone_data(self.cmaq.dates)
        elif param == 'SO2':
            aqs = self.aqs.load_aqs_so2_data(self.cmaq.dates)
        elif param == 'NO2':
            aqs = self.aqs.load_aqs_no2_data(self.cmaq.dates)
        elif param == 'PM25':
            aqs = self.aqs.load_aqs_pm25_data(self.cmaq.dates)
        elif param == 'PM10':
            aqs = self.aqs.load_aqs_pm10_data(self.cmaq.dates)
        elif param == 'CO':
            aqs = self.aqs.load_aqs_co_data(self.cmaq.dates)
        else:
            raise ValueError('Sorry but there is no data available for that in the AQS Data Mart')
        return aqs
