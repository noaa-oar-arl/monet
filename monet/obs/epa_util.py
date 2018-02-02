from __future__ import print_function

from future import standard_library

standard_library.install_aliases()


def check_cmaq_units(df, param='O3', aqs_param='OZONE'):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    param : type
        Description of parameter `param` (the default is 'O3').
    aqs_param : type
        Description of parameter `aqs_param` (the default is 'OZONE').

    Returns
    -------
    type
        Description of returned object.

    """
    aunit = df[df.Species == aqs_param].Units.unique()[0]
    if aunit == 'UG/M3':
        fac = 1.
    elif aunit == 'PPB':
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


def ensure_values_indomain(df, lon, lat):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    lon : type
        Description of parameter `lon`.
    lat : type
        Description of parameter `lat`.

    Returns
    -------
    type
        Description of returned object.

    """
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
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df` (the default is None).
    param : type
        Description of parameter `param` (the default is 'OZONE').
    fname : type
        Description of parameter `fname` (the default is 'table').
    threasholds : type
        Description of parameter `threasholds` (the default is [70).
    1e5] : type
        Description of parameter `1e5]`.
    site : type
        Description of parameter `site` (the default is None).
    city : type
        Description of parameter `city` (the default is None).
    region : type
        Description of parameter `region` (the default is None).
    state : type
        Description of parameter `state` (the default is None).
    append : type
        Description of parameter `append` (the default is False).
    label : type
        Description of parameter `label` (the default is 'CMAQ').
    html : type
        Description of parameter `html` (the default is False).

    Returns
    -------
    type
        Description of returned object.

    """
    from io import StringIO
    single = False
    if df is None:
        print('Please provide a DataFrame')
    else:
        df = df.groupby('Species').get_group(param)
        if not isinstance(site, type(None)):
            try:
                df = df.groupby('SCS').get_group(site)
                single = True
                name = site
            except KeyError:
                print('Site Number not valid.  Enter a valid SCS')
                return
        elif not isinstance(city, type(None)):
            try:
                single = True
                names = df.get_group('MSA_Name').dropna().unique()
                name = [j for j in names if city.upper() in j.upper()]
                df = df.groupby('Species').get_group(param).groupby('MSA_Name').get_group(name[0])
                single = True
            except KeyError:
                print(' City either does not contain montiors for ' + param)
                print('     or City Name is not valid.  Enter a valid City name: df.MSA_Name.unique()')
                return
        elif not isinstance(state, type(None)):
            try:
                single = True
                names = df.get_group('State_Name').dropna().unique()
                name = [j for j in names if state.upper() in j.upper()]
                df = df.groupby('Species').get_group(param).groupby('State_Name').get_group(name[0])
            except KeyError:
                print('State not valid. Please enter valid 2 digit state')
                return
        elif not isinstance(region, type(None)):
            try:
                single = True
                names = df.get_group('Region').dropna().unique()
                name = [j for j in names if region.upper() in j.upper()]
                df = df.groupby('Region').get_group(name[0])
            except KeyError:
                print('Region not valid.  Enter a valid Region')
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


def get_region(df):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import array, concatenate
    from pandas import DataFrame, merge
    se = array(['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV'])
    ne = array(['CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'])
    nc = array(['IL', 'IN', 'IA', 'KY', 'MI', 'MN', 'MO', 'OH', 'WI'])
    sc = array(['AR', 'LA', 'OK', 'TX'])
    r = array(['AZ', 'CO', 'ID', 'KS', 'MT', 'NE', 'NV', 'NM', 'ND', 'SD', 'UT', 'WY'])
    p = array(['CA', 'OR', 'WA'])
    ner = array(['Northeast' for i in ne])
    ser = array(['Southeast' for i in se])
    ncr = array(['North_Central' for i in nc])
    scr = array(['South_Central' for i in sc])
    rr = array(['Rockies' for i in r])
    pr = array(['Pacific' for i in p])
    states = concatenate([se, ne, nc, sc, r, p])
    region = concatenate([ser, ner, ncr, scr, rr, pr])
    dd = DataFrame({'State_Name': states, 'Region': region})
    return merge(df, dd, how='left', on='State_Name')


def get_epa_location_df(df, param, site='', city='', region='', epa_region='', state=''):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    param : type
        Description of parameter `param`.
    site : type
        Description of parameter `site` (the default is '').
    city : type
        Description of parameter `city` (the default is '').
    region : type
        Description of parameter `region` (the default is '').
    epa_region : type
        Description of parameter `epa_region` (the default is '').
    state : type
        Description of parameter `state` (the default is '').

    Returns
    -------
    type
        Description of returned object.

    """
    cityname = True
    if 'MSA_Name' in df.columns:
        cityname = True
    else:
        cityname = False
    new = df.groupby('Species').get_group(param)
    if site != '':
        if site in new.SCS.unique():
            df2 = new.loc[new.SCS == site]
            title = df2.SCS.unique().astype('str')[0].zfill(9)
    elif city != '':
        names = df.MSA_Name.dropna().unique()
        for i in names:
            if i.upper().find(city.upper()) != -1:
                name = i
                print(name)
        df2 = new[new['MSA_Name'] == name].copy().drop_duplicates()
        title = name
    elif state != '':
        df2 = new[new['State_Name'].str.upper() == state.upper()].copy().drop_duplicates()
        title = 'STATE: ' + state.upper()
    elif region != '':
        df2 = new[new['Region'].str.upper() == region.upper()].copy().drop_duplicates()
        title = 'REGION: ' + region.upper()
    elif epa_region != '':
        df2 = new[new['EPA_region'].str.upper() == epa_region.upper()].copy().drop_duplicates()
        title = 'EPA_REGION: ' + epa_region.upper()
    else:
        df2 = new
        title = 'Domain'
    return df2, title
