from numpy import array, concatenate
from pandas import Series, to_datetime
#from monet.models.basemodel import *
from inspect import getmembers, isfunction
from ..grids import _hysplit_latlon_grid_from_dataset
from ..grids import get_hysplit_latlon_pyreample_area_def


def open_files(fname):
    """Short summary.

    Parameters
    ----------
    fname : type
        Description of parameter `fname`.
    earth_radius : type
        Description of parameter `earth_radius`.

    Returns
    -------
    type
        Description of returned object.

    """
    # open the dataset using xarray
    binfile = ModelBin(cdump, verbose=False, readwrite='r')
    dset = binfile.dset

    # get the grid information
    p4 = _hysplit_latlon_grid_from_dataset(dset)
    swath = get_hysplit_latlon_pyreample_area_def(dset, p4)

    # now assign this to the dataset and each dataarray
    dset = dset.assign_attrs({'proj4_srs': p4})
    for i in dset.variables:
        dset[i] = dset[i].assign_attrs({'proj4_srs': p4})
        for j in dset[i].attrs:
            dset[i].attrs[j] = dset[i].attrs[j].strip()
        dset[i] = dset[i].assign_attrs({'area': swath})
    dset = dset.assign_attrs(area=swath)

    # return the dataset
    return dset


class ModelBin(object):
    """represents a binary cdump (concentration) output file from HYSPLIT
       methods:
       readfile - opens and reads contents of cdump file into an xarray
       self.dset

    """

    def __init__(self,
                 filename,
                 drange=None,
                 century=None,
                 verbose=False,
                 readwrite='r',
                 fillra=True):
        """
          drange should be a list of two datetime objects.
           The read method will store data from the cdump file for which the
           sample start is greater thand drange[0] and less than drange[1]
           for which the sample stop is less than drange[1].

        """
        self.drange = drange
        self.filename = filename
        self.century = century
        self.verbose = verbose
        self.zeroconcdates = [
        ]  # list of tuples (date1, date2)  of averaging periods with zero concentrations
        self.nonzeroconcdates = [
        ]  # list of tuples  of averaging periods with nonzero concentrtations]
        if readwrite == 'r':
            self.dataflag = self.readfile(
                filename, drange, verbose, century, fillra=fillra)

    @staticmethod
    def define_struct():
        """Each record in the fortran binary begins and ends with 4 bytes which
        specify the length of the record. These bytes are called pad below.
        They are not used here, but are thrown out. The following block defines
        a numpy dtype object for each record in the binary file. """
        from numpy import dtype
        real4 = '>f'
        int4 = '>i'
        int2 = '>i2'
        char4 = '>a4'

        rec1 = dtype([
            ('pad1', int4),
            ('model_id', char4),  # meteorological model id
            ('met_year', int4),  # meteorological model starting time
            ('met_month', int4),
            ('met_day', int4),
            ('met_hr', int4),
            ('met_fhr', int4),  # forecast hour
            ('start_loc', int4),  # number of starting locations
            ('conc_pack', int4),  # concentration packing flag (0=no, 1=yes)
            ('pad2', int4),
        ])

        # start_loc in rec1 tell how many rec there are.
        rec2 = dtype([
            ('pad1', int4),
            ('r_year', int4),  # release starting time
            ('r_month', int4),
            ('r_day', int4),
            ('r_hr', int4),
            ('s_lat', real4),  # Release location
            ('s_lon', real4),
            ('s_ht', real4),
            ('r_min', int4),  # release startime time (minutes)
            ('pad2', int4),
        ])

        rec3 = dtype([
            ('pad1', int4),
            ('nlat', int4),
            ('nlon', int4),
            ('dlat', real4),
            ('dlon', real4),
            ('llcrnr_lat', real4),
            ('llcrnr_lon', real4),
            ('pad2', int4),
        ])

        rec4a = dtype([
            ('pad1', int4),
            ('nlev', int4),  # number of vertical levels in concentration grid
        ])

        rec4b = dtype([
            ('levht', int4),  # height of each level (meters above ground)
        ])

        rec5a = dtype([
            ('pad1', int4),
            ('pad2', int4),
            ('pollnum', int4),  # number of different pollutants
        ])

        rec5b = dtype([
            ('pname', char4),  # identification string for each pollutant
        ])

        rec5c = dtype([
            ('pad2', int4),
        ])

        rec6 = dtype([
            ('pad1', int4),
            ('oyear', int4),  # sample start time.
            ('omonth', int4),
            ('oday', int4),
            ('ohr', int4),
            ('omin', int4),
            ('oforecast', int4),
            ('pad3', int4),
        ])

        # rec7 has same form as rec6.            #sample stop time.

        # record 8 is pollutant type identification string, output level.

        rec8a = dtype([
            ('pad1', int4),
            ('poll', char4),  # pollutant identification string
            ('lev', int4),
            ('ne', int4),  # number of elements
        ])

        rec8b = dtype([
            ('indx', int2),  # longitude index
            ('jndx', int2),  # latitude index
            ('conc', real4),
        ])

        rec8c = dtype([
            ('pad2', int4),
        ])

        return rec1, rec2, rec3, rec4a, rec4b, rec5a, rec5b, rec5c, rec6, rec8a, rec8b, rec8c

    def readfile(self, filename, drange, verbose, century, fillra=True):
        """Data from the file is stored in an xarray, self.dset
           returns False if all concentrations are zero else returns True.
           INPUTS
           filename - name of cdump file to open
           drange - [date1, date2] - range of dates to load data for. if [] then loads all data.
                    date1 and date2  should be datetime ojbects.
           verbose - turns on print statements
           century - if None will try to guess the century by looking at the last two digits of the year.
           For python 3 the numpy char4 are read in as a numpy.bytes_ class and need to be converted to a python
           string by using decode('UTF-8').
           fillra : if True will return complete concentration grid  array with zero cocenctrations filled in

        """
        from numpy import fromfile, arange
        # 8/16/2016 moved species=[]  to before while loop. Added print statements when verbose.
        self.dset = None
        atthash = {
        }  # dictionary which will be turned into the dset attributes.
        ahash = {}
        fp = open(filename, 'rb')

        # each record in the fortran binary begins and ends with 4 bytes which specify the length of the record.
        # These bytes are called pad1 and pad2 below. They are not used here, but are thrown out.
        # The following block defines a numpy dtype object for each record in the binary file.
        rec1, rec2, rec3, rec4a, rec4b, rec5a, rec5b, rec5c, rec6, rec8a, rec8b, rec8c = self.define_struct(
        )
        rec7 = rec6
        # start_loc in rec1 tell how many rec there are.
        tempzeroconcdates = []
        # Reads header data. This consists of records 1-5.
        hdata1 = fromfile(fp, dtype=rec1, count=1)
        nstartloc = hdata1['start_loc'][0]
        # in python 3 np.fromfile reads the record into a list even if it is just one number.
        # so if the length of this record is greater than one something is wrong.
        if len(hdata1['start_loc']) != 1:
            print(
                'WARNING in ModelBin _readfile - number of starting locations incorrect'
            )
            print(hdata1['start_loc'])
        hdata2 = fromfile(fp, dtype=rec2, count=nstartloc)
        hdata3 = fromfile(fp, dtype=rec3, count=1)
        atthash['Number Start Locations'] = nstartloc

        # Description of concentration grid
        ahash['Number Lat Points'] = hdata3['nlat'][0]
        ahash['Number Lon Points'] = hdata3['nlon'][0]
        ahash['Latitude Spacing'] = hdata3['dlat'][0]
        ahash['Longitude Spacing'] = hdata3['dlon'][0]
        ahash['llcrnr longitude'] = hdata3['llcrnr_lon'][0]
        ahash['llrcrnr latitude'] = hdata3['llcrnr_lat'][0]

        self.llcrnr_lon = hdata3['llcrnr_lon'][0]
        self.llcrnr_lat = hdata3['llcrnr_lat'][0]
        self.nlat = hdata3['nlat'][0]
        self.nlon = hdata3['nlon'][0]
        self.dlat = hdata3['dlat'][0]
        self.dlon = hdata3['dlon'][0]

        atthash['Meteorological Model ID'] = hdata1['model_id'][0].decode(
            'UTF-8')
        self.sourcedate = []
        self.slat = []
        self.slon = []
        self.sht = []
        atthash['Starting Locations'] = []
        atthash['Source Date'] = []
        # Loop through starting locations

        for n in range(0, nstartloc):
            # create list of starting latitudes, longitudes and heights.
            self.slat.append(hdata2['s_lat'][n])
            self.slon.append(hdata2['s_lon'][n])
            self.sht.append(hdata2['s_ht'][n])
            atthash['Starting Locations'].append((hdata2['s_lat'][n],
                                                  hdata2['s_lon'][n]))

            # try to guess century if century not given
            if century is None:
                if hdata2['r_year'][0] < 50:
                    century = 2000
                else:
                    century = 1900
                print(
                    'WARNING: Guessing Century for HYSPLIT concentration file',
                    century)
            # add sourcedate which is datetime.datetime object
            sourcedate = (datetime.datetime(
                century + hdata2['r_year'][n], hdata2['r_month'][n],
                hdata2['r_day'][n], hdata2['r_hr'][n], hdata2['r_min'][n]))
            self.sourcedate.append(sourcedate)
            atthash['Source Date'].append(sourcedate)

        # read record 4 which gives information about vertical levels.
        hdata4a = fromfile(fp, dtype=rec4a, count=1)  # gets nmber of levels
        hdata4b = fromfile(
            fp, dtype=rec4b, count=hdata4a['nlev'][
                0])  # reads levels, count is number of levels.
        self.levels = hdata4b['levht']
        atthash['Number of Levels'] = hdata4a['nlev'][0]
        atthash['Level top heights (m)'] = hdata4b['levht']

        # read record 5 which gives information about pollutants / species.
        hdata5a = fromfile(fp, dtype=rec5a, count=1)
        hdata5b = fromfile(fp, dtype=rec5b, count=hdata5a['pollnum'][0])
        hdata5c = fromfile(fp, dtype=rec5c, count=1)
        atthash['Number of Species'] = hdata5a['pollnum'][0]

        # Loop to reads records 6-8. Number of loops is equal to number of output times.
        # Only save data for output times within drange. if drange=[] then save all.
        # Loop to go through each sampling time
        ii = 0  # check to make sure don't go above max number of iterations
        iii = 0  # checks to see if some nonzero data was saved in xarray
        imax = 1e3  # Safety valve - will not allow more than 1000 loops to be executed.
        testf = True
        while testf:
            hdata6 = fromfile(fp, dtype=rec6, count=1)
            hdata7 = fromfile(fp, dtype=rec6, count=1)
            if len(hdata6
                   ) == 0:  # if no data read then break out of the while loop.
                break
            if verbose:
                print('REC 6 & 7 ***************')
                print(hdata6)
                print(hdata7)
            # pdate1 is the sample start
            # pdate2 is the sample stop
            pdate1 = datetime.datetime(century + hdata6['oyear'],
                                       hdata6['omonth'], hdata6['oday'],
                                       hdata6['ohr'])
            pdate2 = datetime.datetime(century + hdata7['oyear'],
                                       hdata7['omonth'], hdata7['oday'],
                                       hdata7['ohr'])
            atthash['Sampling Time'] = pdate2 - pdate1
            savedata = True

            # if pdate1 is within drange then save the data.
            # AND if pdate2 is within drange then save the data.
            # if drange[0] > pdate1 then stop looping to look for more data
            # this block sets savedata to true if data within specified time range or time range not specified
            if drange is None:
                savedata = True
            elif pdate1 >= drange[0] and pdate1 <= drange[1] and pdate2 <= drange[1]:
                savedata = True
            elif pdate1 > drange[1] or pdate2 > drange[1]:
                testf = False
                savedata = False
            else:
                savedata = False
            # END block

            if verbose:
                print(savedata, 'DATES :', pdate1, pdate2)

            datelist = []
            atthash['Species ID'] = []
            inc_iii = False
            # LOOP to go through each level
            for lev in range(hdata4a['nlev'][0]):
                # LOOP to go through each pollutant
                for pollutant in range(hdata5a['pollnum'][0]):

                    # record 8a has the number of elements (ne). If number of elements greater than 0 than there are concentrations.
                    hdata8a = fromfile(fp, dtype=rec8a, count=1)
                    atthash['Species ID'].append(
                        hdata8a['poll'][0].decode('UTF-8'))
                    if hdata8a['ne'] >= 1:  # if number of elements is nonzero then
                        hdata8b = fromfile(
                            fp, dtype=rec8b,
                            count=hdata8a['ne'][0])  # get rec8 - indx and jndx
                        self.nonzeroconcdates.append(
                            pdate1
                        )  # add sample start time to list of start times with non zero conc
                    else:
                        tempzeroconcdates.append(
                            pdate1
                        )  # or add sample start time to list of start times with zero conc.

                    hdata8c = fromfile(
                        fp, dtype=rec8c, count=1)  # This is just padding.
                    # if savedata is set and nonzero concentrations then save the data in a pandas dataframe
                    if savedata and hdata8a['ne'] >= 1:
                        self.nonzeroconcdates.append(pdate1)
                        inc_iii = True  # set to True to indicate that there is data to be saved.

                        lev_name = hdata8a['lev'][0]
                        col_name = hdata8a['poll'][0].decode('UTF-8')
                        ndata = hdata8b.byteswap().newbyteorder(
                        )  # otherwise get endian error.
                        concframe = pd.DataFrame.from_records(ndata)
                        # add latitude longitude columns
                        lat = arange(
                            self.llcrnr_lat,
                            self.llcrnr_lat + self.nlat * self.dlat, self.dlat)
                        lon = arange(
                            self.llcrnr_lon,
                            self.llcrnr_lon + self.nlon * self.dlon, self.dlon)

                        def flat(x):
                            return lat[x - 1]

                        def flon(x):
                            return lon[x - 1]

                        # This block will fill in zero values in the concentration grid.
                        if fillra:
                            n1 = arange(1, self.nlat + 1)
                            n2 = arange(1, self.nlon + 1)
                            concframe['ji'] = zip(concframe['jndx'],
                                                  concframe['indx'])
                            concframe.set_index(['ji'], inplace=True)
                            newi = [(x, y) for x in n1 for y in n2]
                            concframe = concframe.reindex(newi)
                            concframe.reset_index(inplace=True)
                            concframe[['jndx',
                                       'indx']] = concframe['ji'].tolist()
                            concframe.fillna(0, inplace=True)
                            concframe.drop('ji', axis=1, inplace=True)
                            # print(len(lat))
                            # print(len(lon))
                            #print(len(n1), len(n2), n1[-1], n2[-1])
                            #print(self.nlat, self.nlon)
                            #print(self.llcrnr_lat, self.llcrnr_lon)
                            # print(concframe[-50:-1])

                        concframe['latitude'] = concframe['jndx'].apply(flat)
                        concframe['longitude'] = concframe['indx'].apply(flon)
                        concframe.drop(['jndx', 'indx'], axis=1, inplace=True)
                        concframe['levels'] = lev_name
                        concframe['time'] = pdate1
                        if verbose:
                            print('pdate1')
                        concframe.set_index(
                            ['time', 'levels', 'longitude', 'latitude'],
                            inplace=True)
                        concframe.rename(
                            columns={'conc': col_name}, inplace=True)
                        dset = xr.Dataset.from_dataframe(concframe)
                        if self.dset is None:  # if this is the first time through. create dataframe for first level and pollutant.
                            self.dset = dset
                        else:  # create dataframe for level and pollutant and then merge with main dataframe.
                            #self.dset = xr.concat([self.dset, dset],'levels')
                            self.dset = xr.merge([self.dset, dset])
                        ii += 1

                # END LOOP to go through each pollutant
            # END LOOP to go through each level
            # safety check - will stop sampling time while loop if goes over imax iterations.
            if ii > imax:
                testf = False
            if inc_iii:
                iii += 1

        atthash['Concentration Grid'] = ahash
        atthash['Species ID'] = list(set(atthash['Species ID']))
        atthash['Coordinate time description'] = 'Beginning of sampling time'
        # END OF Loop to go through each sampling time
        self.dset.attrs = atthash
        if verbose:
            print(self.dset)

        if iii == 0:
            print(
                'Warning: ModelBin class _readfile method: no data in the date range found'
            )
            return False
        return True
