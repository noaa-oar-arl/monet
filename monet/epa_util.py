
def compare_param(df, param='OZONE', site='', city='', region='', state='', timeseries=False, scatter=False,
                  pdfs=False, diffscatter=False, diffpdfs=False, timeseries_rmse=False, timeseries_mb=False,
                  taylordiagram=False, fig=None, label=None, footer=False, dia=None,marker=None):
    from numpy import NaN
    cityname = True
    if 'MSA_Name' in df.columns:
        df = df.copy()[[
                'datetime', 'datetime_local', 'Obs', 'CMAQ', 'Species', 'MSA_Name', 'Region', 'SCS', 'Units',
                'Latitude',
                'Longitude', 'State_Name','EPA_region']]
    else:
        df = df.copy()[[
                'datetime', 'datetime_local', 'Obs', 'CMAQ', 'Species', 'Region', 'SCS', 'Units', 'Latitude',
                'Longitude', 'State_Name','EPA_region']]
        cityname = False
        df[df < -990] = NaN
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
    elif epa_region != '':
        df2 = new[new['EPA_region'].str.upper() == region.upper()].copy().drop_duplicates()
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
        if marker is None: marker = 'o'
            if fig is None:
                dia = plots.taylordiagram(df2, label=label, dia=dia, addon=False,marker=marker)
                return dia
            else:
                dia = plots.taylordiagram(df2, label=label, dia=dia, addon=True,marker=marker)
                plt.legend()
                return dia

def spatial( df, cmaqvar,m,param='OZONE', path='', region='', date='', xlim=[], ylim=[], vmin=0, vmax=150, ncolors=16,
            cmap='YlGnBu'):
    """
    :param param: Species Parameter: Acceptable Species: 'OZONE' 'PM2.5' 'CO' 'NOY' 'SO2' 'SO2' 'NOX'
    :param region: EPA Region: 'Northeast', 'Southeast', 'North Central', 'South Central', 'Rockies', 'Pacific'
    :param date: If not supplied will plot all time.  Put in 'YYYY-MM-DD HH:MM' for single time
    :return:
    """
    from numpy.ma import masked_less
    
    g = df.groupby('Species')
    df2 = g.get_group(param)
        
    cmaq = masked_less(cmaqvar, .001)
    if date == '':
        for index, i in enumerate(self.cmaq.dates):
            c = plots.make_spatial_plot(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index],
                                        m, vmin=vmin, vmax=vmax)
            plots.spatial_scatter(df2, m, i.strftime('%Y-%m-%d %H:%M:%S'), vmin=vmin, vmax=vmax)
            c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
            if len(xlim) > 1:
                plt.xlim([min(xlim), max(xlim)])
                plt.ylim([min(ylim), max(ylim)])
                
    else:
        index = where(self.cmaq.dates == datetime.strptime(date, '%Y-%m-%d %H:%M'))[0][0]
        c = plots.make_spatial_plot(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index], m,
                                    vmin=vmin, vmax=vmax, ncolors=ncolors, cmap=cmap)
        plots.spatial_scatter(df2, m, self.cmaq.dates[index].strftime('%Y-%m-%d %H:%M:%S'), vmin=vmin, vmax=vmax,
                              ncolors=ncolors, cmap=cmap)
        c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
        if len(xlim) > 1:
            plt.xlim([min(xlim), max(xlim)])
            plt.ylim([min(ylim), max(ylim)])
            
def spatial_contours(df, cmaqvar,m,cmap=None,levels=None,param='OZONE', path='', region='', date='', xlim=[], ylim=[]):
        """                                                                                                                                                                                             
        :param param: Species Parameter: Acceptable Species: 'OZONE' 'PM2.5' 'CO' 'NOY' 'SO2' 'SO2' 'NOX'                                                                                               
        :param region: EPA Region: 'Northeast', 'Southeast', 'North Central', 'South Central', 'Rockies', 'Pacific'                                                                                     
        :param date: If not supplied will plot all time.  Put in 'YYYY-MM-DD HH:MM' for single time                                                                                                     
        :return:                                                                                                                                                                                        
        """
        import colorbars
        from numpy import unique
        
        g = df.groupby('Species')
        df2 = g.get_group(param)
        param = param.upper()
        cmaq = cmaqvar
        if isinstance(cmaq, type(None)):
            print 'This parameter is not in the CMAQ file: ' + param
        else:
            if date == '':
                for index, i in enumerate(self.cmaq.dates):
                    c = plots.make_spatial_contours(cmaq[index, :, :].squeeze(), self.cmaq.gridobj,
                                                    self.cmaq.dates[index],
                                                    m, levels=levels, cmap=cmap, units='xy')
                    if not isinstance(self.cmaq.metcro2d, type(None)):
                        ws = self.cmaq.metcro2d.variables['WSPD10'][index, :, :, :].squeeze()
                        wdir = self.cmaq.metcro2d.variables['WDIR10'][index, :, :, :].squeeze()
                        plots.wind_barbs(ws, wdir, self.cmaq.gridobj, color='grey', alpha=.5)
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
                c = plots.make_spatial_contours(cmaq[index, :, :].squeeze(), self.cmaq.gridobj, self.cmaq.dates[index], m,
                                                levels=levels, cmap=cmap, units='xy')
                if not isinstance(self.cmaq.metcro2d, type(None)):
                    ws = self.cmaq.metcro2d.variables['WSPD10'][index, :, :, :].squeeze()
                    wdir = self.cmaq.metcro2d.variables['WDIR10'][index, :, :, :].squeeze()
                    plots.wind_barbs(ws, wdir, self.cmaq.gridobj, m, color='black', alpha=.3)
                try:
                    plots.spatial_scatter(df2, m, self.cmaq.dates[index].strftime('%Y-%m-%d %H:%M:%S'), vmin=levels[0],
                                      vmax=levels[-1], cmap=cmap, discrete=False)
                except:
                    pass
                c.set_label(param + ' (' + g.get_group(param).Units.unique()[0] + ')')
                c.set_ticks(unique(levels.round(-1)))
                if len(xlim) > 1:
                    plt.xlim([min(xlim), max(xlim)])
                    plt.ylim([min(ylim), max(ylim)])
            cmap, levels = colorbars.noxcmap()
        elif param == 'NOY':
            cmaq = self.cmaqnoy
            cmap, levels = colorbars.noxcmap()
        elif param == 'SO2':
            cmaq = self.cmaqso2
        
def ensure_values_indomain(df,lon,lat):
     con = ((df.Latitude.values > lat.min()) & (df.Latitude.values < lat.max()) & (
             df.Longitude.values > lon.min()) & (df.Longitude.values < lon.max()))
     df = df[con].copy()
     return df

def write_table(self, df=None, param='OZONE', fname='table', threasholds=[70, 1e5], site=None, city=None,
                region=None,
                state=None,
                append=False,
                label='CMAQ',
                html=False):
    from StringIO import StringIO
    single = False
    if df is None:
        print 'Please provide a DataFrame'
    else:
        df = df.groupby('Species').get_group(param)
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
                df = df.groupby('Species').get_group(param).groupby('MSA_Name').get_group(name[0])
                single = True
            except KeyError:
                print ' City either does not contain montiors for ' + param
                print '     or City Name is not valid.  Enter a valid City name: df.MSA_Name.unique()'
                return
        elif not isinstance(state, type(None)):
            try:
                single = True
                names = df.get_group('State_Name').dropna().unique()
                name = [j for j in names if state.upper() in j.upper()]
                df = df.groupby('Species').get_group(param).groupby('State_Name').get_group(name[0])
            except KeyError:
                print 'State not valid. Please enter valid 2 digit state'
                return
        elif not isinstance(region, type(None)):
            try:
                single = True
                names = df.get_group('Region').dropna().unique()
                name = [j for j in names if region.upper() in j.upper()]
                df = df.groupby('Region').get_group(name[0])
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
            for i in df.Region.dropna().unique():
                try:
                    dff = df.groupby('Region').get_group(i)
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
