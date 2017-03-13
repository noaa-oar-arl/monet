def interp_to_pt_obs(cmaqvar, df, lon,lat,interp='nearest', r=12000., n=7, weight_func=lambda r: 1 / r ** 2,site_label='SCS"):
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
        from pandas import concat, Series, merge
        from numpy import append, empty, vstack, NaN
        from gc import collect
        dates = self.cmaq.dates[self.cmaq.indexdates]
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
        df = merge(df, dfs, how='left', on=['SCS', 'datetime'])

        return df

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


def get_grid_def(lon,lat):
    return geometry.GridDefinition(lons=lon,lats=lat)
