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


def convert_statenames_to_abv(df):
    d = {'Alabama': 'AL',
         'Alaska': 'AK',
         'Arizona': 'AZ',
         'Arkansas': 'AR',
         'California': 'CA',
         'Colorado': 'CO',
         'Connecticut': 'CT',
         'Delaware': 'DE',
         'Florida': 'FL',
         'Georgia': 'GA',
         'Hawaii': 'HI',
         'Idaho': 'ID',
         'Illinois': 'IL',
         'Indiana': 'IN',
         'Iowa': 'IA',
         'Kansas': 'KS',
         'Kentucky': 'KY',
         'Louisiana': 'LA',
         'Maine': 'ME',
         'Maryland': 'MD',
         'Massachusetts': 'MA',
         'Michigan': 'MI',
         'Minnesota': 'MN',
         'Mississippi': 'MS',
         'Missouri': 'MO',
         'Montana': 'MT',
         'Nebraska': 'NE',
         'Nevada': 'NV',
         'New Hampshire': 'NH',
         'New Jersey': 'NJ',
         'New Mexico': 'NM',
         'New York': 'NY',
         'North Carolina': 'NC',
         'North Dakota': 'ND',
         'Ohio': 'OH',
         'Oklahoma': 'OK',
         'Oregon': 'OR',
         'Pennsylvania': 'PA',
         'Rhode Island': 'RI',
         'South Carolina': 'SC',
         'South Dakota': 'SD',
         'State': 'Postal',
         'Tennessee': 'TN',
         'Texas': 'TX',
         'Utah': 'UT',
         'Vermont': 'VT',
         'Virginia': 'VA',
         'Washington': 'WA',
         'West Virginia': 'WV',
         'Wisconsin': 'WI',
         'Wyoming': 'WY'}
    for i in d:
        df['State_Name'].loc[df.State_Name.isin([i])] = d[i]
    df['State_Name'].loc[df.State_Name.isin(['Canada'])] = 'CC'
    df['State_Name'].loc[df.State_Name.isin(['Mexico'])] = 'MM'
    return df


def read_monitor_file(network=None):
    from numpy import NaN
    import pandas as pd
    import os
    try:
        basedir = os.path.abspath(os.path.dirname(__file__))[:-10]
        s = pd.read_csv(os.path.join(basedir, 'data', 'monitoring_site_locations.dat'))
    except:
        print('Monitor File Not Found... Reprocessing')
        baseurl = 'https://aqs.epa.gov/aqsweb/airdata/'
        site_url = baseurl + 'aqs_sites.zip'
        # has network info (CSN IMPROVE etc....)
        monitor_url = baseurl + 'aqs_monitors.zip'
        # Airnow monitor file
        monitor_airnow_url = 'https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/today/monitoring_site_locations.dat'
        colsinuse = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                     11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
        airnow = pd.read_csv(monitor_airnow_url, delimiter='|', header=None, usecols=colsinuse)
        airnow.columns = ['SCS', 'Site_Code', 'Site_Name', 'Status', 'Agency', 'Agency_Name', 'EPA_region', 'Latitude',
                          'Longitude', 'Elevation', 'GMT_Offset', 'Country_Code', 'CMSA_Code', 'CMSA_Name', 'MSA_Code',
                          'MSA_Name', 'State_Code', 'State_Name', 'County_Code', 'County_Name', 'City_Code']
        airnow['SCS'] = pd.to_numeric(airnow.SCS, errors='coerce')
        # Read EPA Site file
        site = pd.read_csv(site_url)
        # read epa monitor file
        monitor = pd.read_csv(monitor_url)
        # make SCS column
        site['SCS'] = site['State Code'].astype(str).str.zfill(2) + site['County Code'].astype(str).str.zfill(3) + site[
            'Site Number'].astype(str).str.zfill(4)
        monitor['SCS'] = monitor['State Code'].astype(str).str.zfill(2) + monitor['County Code'].astype(str).str.zfill(
            3) + monitor['Site Number'].astype(str).str.zfill(4)
        site.columns = [i.replace(' ', '_') for i in site.columns]
        s = monitor.merge(site[['SCS', 'Land_Use', 'Location_Setting', 'GMT_Offset']], on=['SCS'], how='left')
        s.columns = [i.replace(' ', '_') for i in s.columns]
        s['SCS'] = pd.to_numeric(s.SCS, errors='coerce')
        monitor_drop = ['State_Code', u'County_Code', u'Site_Number', 'Extraction_Date', 'Parameter_Code', 'Parameter_Name', 'POC', 'Last_Sample_Date', 'PQAO',
                        'Reporting_Agency', 'Exclusions', u'Monitoring_Objective', 'Last_Method_Code', 'Last_Method', u'NAAQS_Primary_Monitor', u'QA_Primary_Monitor']
        s.drop(monitor_drop, axis=1, inplace=True)
        # drop airnow keys for merge
        airnow_drop = [u'Site_Code', u'Site_Name', u'Status', u'Agency', 'Agency_Name', 'Country_Code', u'CMSA_Code',
                       'State_Code', u'County_Code', u'City_Code', u'Latitude', u'Longitude', 'GMT_Offset', 'State_Name', 'County_Name']
        airnow.drop(airnow_drop, axis=1, inplace=True)
        s = s.merge(airnow, how='left', on='SCS')
        s = convert_statenames_to_abv(s).dropna(subset=['Latitude', 'Longitude'])
    if network is not None:
        s = s.loc[s.Networks.isin([network])].drop_duplicates(subset=['SCS'])
    return s
