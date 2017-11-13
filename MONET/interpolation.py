def interp_to_pt_obs(cmaqvar,dates, df, lon,lat,interp='nearest', r=12000., n=7, weight_func=lambda r: 1 / r ** 2,site_label='SCS',daily=False,md8hr=False):
        """
        This function interpolates variables (2d surface) in time to measurement sites

        :param cmaqvar: this is the CMAQ 3D variable
        :param df: The aqs
        :param interp: inteprolation method 'nearest',idw,guass
        :param r: radius of influence
        :param n: number of nearest neighbors to include
        :param weight_func: the user can set a defined method of interpolation
                            example:
                                lambda r: 1 / r ** 2
        :return: df
        """
        from pyresample import geometry, kd_tree
        from pandas import concat, Series, merge,to_numeric
        from numpy import append, empty, vstack, NaN,array
        from gc import collect
        #dates = self.camx.dates[self.camx.indexdates]
        grid1 = geometry.GridDefinition(lons=lon, lats=lat)
        vals = array([], dtype=cmaqvar.dtype)
        date = array([], dtype='O')
        site = array([], dtype=df[site_label].dtype)
        print '    Interpolating using ' + interp + ' method'
        for i, j in enumerate(dates):
            con = df.datetime == j
            print j
            try:
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
                collect()
            except:
                pass

        vals = Series(vals)
        date = Series(date)
        site = Series(site)
        dfs = concat([vals, date, site], axis=1, keys=['CMAQ', 'datetime', 'SCS'])
       # if daily:
       #         a = dfs.merge(dfo3,on=
       #         dfs.index = dfs.datetime
       #         dfs['SCS'] = to_numeric(dfs.SCS)
       #         dfs = dfs.groupby('SCS').resample('D').mean()
       #         dfs = dfs.drop('SCS',axis=1).reset_index(level=0).reset_index()
       #         print dfs
        df = merge(df, dfs, how='left', on=['SCS', 'datetime'])

        return dfs

def interp_to_obs_new(var,df,lat,lon,radius=12000.):
    from numpy import NaN,vstack
    from pyresample import geometry,image
    from pandas import to_timedelta,DataFrame
    #define CMAQ pyresample grid (source)
    grid1 = geometry.GridDefinition(lons=lon,lats=lat)
    #get unique sites from df
    dfn = df.drop_duplicates(subset=['Latitude','Longitude'])
    #define site grid (target)
    lats = dfn.Latitude.values 
    lons = dfn.Longitude.values
    grid2 = geometry.GridDefinition(lons=vstack(lons), lats=vstack(lats))
    #Create image container
    i = image.ImageContainerNearest(var.transpose('ROW','COL','TSTEP').values,grid1,radius_of_influence=radius,fill_value=NaN)
    #resample
    ii = i.resample(grid2).image_data.squeeze()
    #recombine data
    e = DataFrame(ii,index=dfn.SCS,columns=var.TSTEP.values)
    w = e.stack().reset_index().rename(columns={'level_1':'datetime',0:'CMAQ'})
    w = w.merge(dfn.drop(['datetime','datetime_local','Obs'],axis=1),on='SCS',how='left')
    w = w.merge(df[['datetime','SCS','Obs']],on=['SCS','datetime'],how='left')
    #calculate datetime local
    
    w['datetime_local'] = w.datetime + to_timedelta(w.utcoffset,'H')

    return w

#this function is used to create and define various grids
#SMOPS grid first (Equidistant Cylindrical or Plate Carree)
def get_smops_area_def(nx=1440,ny=720):
    p = Proj(proj='eqc',lat_ts=0.,lat_0=0.,lon_0=0.,x_0=0.,y_0=0.,a=6378137,b=6378137,units='m')
    proj4_args = p.srs
    area_name = 'Global .25 degree SMOPS Grid'
    area_id = 'smops'
    proj_id = area_id
    aa = p([-180,180],[-90,90])
    area_extent = (aa[0][0],aa[1][0],aa[0][1],aa[1][1])
    area_def = utils.get_area_def(area_id, area_name,proj_id,proj4_args,nx,ny,area_extent)
    return area_def

def get_gfs_area_def(nx=1440,ny=721):
#    proj4_args = '+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m'
    p = Proj(proj='eqc',lat_ts=0.,lat_0=0.,lon_0=0.,x_0=0.,y_0=0.,a=6378137,b=6378137,units='m')
    proj4_args = p.srs
    area_name ='Global .25 degree SMOPS Grid'
    area_id = 'smops'
    proj_id = area_id
    aa = p([0,360-.25],[-90,90.])
    area_extent= (aa[0][0],aa[1][0],aa[0][1],aa[1][1])
    area_def = utils.get_area_def(area_id, area_name,proj_id,proj4_args,nx,ny,area_extent)
    return area_def

def geotiff_meta_to_areadef(meta):
     """
     Transform (Rasterio) geotiff meta dictionary to pyresample area definition
     Arguments:
      meta (dictionary) : dictionary containing projection and image geometry
                          information (formed by Rasterio)
     Returns:
          area_def (pyresample.geometry.AreaDefinition) : Area definition object
     """
     import pyresample
     area_id = ""
     name = ""
     proj_id = "Generated from GeoTIFF"
     proj_dict = meta['crs']
     proj_dict_with_string_values = dict(zip([str(key) for key in proj_dict.keys()], [str(value) for value in proj_dict.values()]))
     x_size = meta['width']
     x_res = meta['transform'][0]
     y_res = meta['transform'][4] * -1
     y_size = meta['height']
     x_ll = meta['transform'][2]
     y_ur = meta['transform'][5]
     y_ll = y_ur - y_size * y_res
     x_ur = x_ll + x_size * x_res
     area_extent = [x_ll, y_ll, x_ur, y_ur]
     print area_extent,x_size,y_size,x_res,y_res

     area_def = pyresample.geometry.AreaDefinition(area_id,
                                                     name,
                                                     proj_id,
                                                     proj_dict_with_string_values,
                                                     x_size,
                                                     y_size,
                                                     area_extent)
     print area_extent,xsize,ysize
     return area_def

def geotiff_meta_to_areadef2(meta):
     """
     Transform (Rasterio) geotiff meta dictionary to pyresample area definition
     Arguments:
      meta (dictionary) : dictionary containing projection and image geometry
                          information (formed by Rasterio)
     Returns:
          area_def (pyresample.geometry.AreaDefinition) : Area definition object
     """
     import pyresample
     area_id = ""
     name = ""
     proj_id = "Generated from GeoTIFF"
     proj_dict = meta['crs']
     proj_dict_with_string_values = dict(zip([str(key) for key in proj_dict.keys()], [str(value) for value in proj_dict.values()]))
     x_size = meta['width']
     x_res = 50000.
     y_res = 50000.
     y_size = meta['height']
     x_ll = meta['transform'][2]
     y_ur = meta['transform'][5]
     y_ll = y_ur - y_size * y_res
     x_ur = x_ll + x_size * x_res
     area_extent = [x_ll, y_ll, x_ur, y_ur]
     print area_extent,x_size,y_size,x_res,y_res

     area_def = pyresample.geometry.AreaDefinition(area_id,
                                                     name,
                                                     proj_id,
                                                     proj_dict_with_string_values,
                                                     x_size,
                                                     y_size,
                                                     area_extent)
     return area_def


def get_grid_def(lon,lat):
    return geometry.GridDefinition(lons=lon,lats=lat)
