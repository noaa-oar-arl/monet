#common utilities for various programs

def write_table(df=None, fname='table',append=False,label='CMAQ',location='location'):
    from StringIO import StringIO
    from numpy import sqrt
    from mystats import MB,R2,IOA,RMSE,NMB,MNPB
    import pandas as pd
    dd = {}
    dd['Location'] = location
    dd['Simulation'] = label
    dd['Parameter'] = df.Species.unique()[0]
    dd['Units'] = df.Units.unique()[0]
    dd['N'] = df.Obs.dropna().count()
    dd['Obs'] = df.Obs.mean()
    dd['Mod'] = df.CMAQ.mean()
    dd['MB'] = MB(df.Obs.values, df.CMAQ.values)  # mean bias     
    dd['R'] = sqrt(R2(df.Obs.values, df.CMAQ.values))  # pearsonr ** 2                                                 
    dd['IOA'] = IOA(df.Obs.values, df.CMAQ.values)  # Index of Agreement       
    dd['RMSE'] = RMSE(df.Obs.values, df.CMAQ.values)
    dd['NMB'] = NMB(df.Obs.values, df.CMAQ.values)
    d = pd.DataFrame(dd,index=[0])
    columns=['Location', 'Simulation', 'Parameter','Units','N', 'Obs', 'Mod', 'MB', 'NMB', 'RMSE', 'R', 'IOA']
    if append:
        dff = pd.read_csv(fname + '.txt',  sep=',')
        d = pd.concat([d, dff]).sort_values(by=['Location','Simulation','Parameter'])
    d.to_csv(fname+'.txt',sep=',',columns=columns,float_format='%.3f',index=False,header=True)
    return d

def get_epa_location_df(df,param,site='',city='',region='',epa_region='',state=''):
    cityname = True
    if 'MSA_Name' in df.columns:
        cityname=True
    else:
        cityname = False
#    df[df < -990] = NaN
    new = df.groupby('Species').get_group(param)
    if site != '':
        if site in new.SCS.unique():
            df2 = new.loc[new.SCS == site]
    elif city != '':
        names = df.MSA_Name.dropna().unique()
        for i in names:
            if i.upper().find(city.upper()) != -1:
                name = i
                print name
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
    return df2,title


def get_region(df):
    from numpy import array,concatenate
    from pandas import DataFrame, merge
    se = array(['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN','VA', 'WV'])
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
    states = concatenate([se,ne,nc,sc,r,p])
    region = concatenate([ser,ner,ncr,scr,rr,pr])
    dd = DataFrame({'State_Name': states, 'Region': region})
    return merge(df,dd,how='left',on='State_Name')
