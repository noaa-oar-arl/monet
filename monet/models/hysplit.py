""" HYPSLIT MODEL READER """
import datetime
import pandas as pd
import xarray as xr
import numpy as np
from numpy import fromfile, arange


def _hysplit_latlon_grid_from_dataset(ds):
    pargs = dict()
    pargs["lat_0"] = ds.latitude.mean()
    pargs["lon_0"] = ds.longitude.mean()

    p4 = (
        "+proj=eqc +lat_ts={lat_0} +lat_0={lat_0} +lon_0={lon_0} "
        "+ellps=WGS84 +datum=WGS84 +units=m +no_defs".format(**pargs)
    )
    return p4


def get_hysplit_latlon_pyresample_area_def(ds, proj4_srs):
    from pyresample import geometry

    return geometry.SwathDefinition(lons=ds.longitude.values, lats=ds.latitude.values)


def check_drange(drange, pdate1, pdate2, verbose):
    """
    drange : list of two datetimes
    pdate1 : datetime
    pdate2 : datetime

    Returns
    savedata : boolean


    returns True if drange is between pdate1 and pdate2
    """
    savedata = True
    testf = True
    # if pdate1 is within drange then save the data.
    # AND if pdate2 is within drange then save the data.
    # if drange[0] > pdate1 then stop looping to look for more data
    # this block sets savedata to true if data within specified time
    # range or time range not specified
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
    # if verbose:
    #    print(savedata, 'DATES :', pdate1, pdate2)
    return testf, savedata


def open_dataset(fname, drange=None, verbose=False):
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

   CHANGES for PYTHON 3
   For python 3 the numpy char4 are read in as a numpy.bytes_ class and need to
    be converted to a python
   string by using decode('UTF-8').


    """
    # open the dataset using xarray
    binfile = ModelBin(fname, drange=drange, verbose=verbose, readwrite="r")
    dset = binfile.dset
    # return dset
    # get the grid information
    # May not need the proj4 definitions now that lat lon defined properly.
    p4 = _hysplit_latlon_grid_from_dataset(dset)
    swath = get_hysplit_latlon_pyresample_area_def(dset, p4)

    # now assign this to the dataset and each dataarray
    dset = dset.assign_attrs({"proj4_srs": p4})
    # return dset
    for iii in dset.variables:
        dset[iii] = dset[iii].assign_attrs({"proj4_srs": p4})
        for jjj in dset[iii].attrs:
            dset[iii].attrs[jjj] = dset[iii].attrs[jjj].strip()
        dset[iii] = dset[iii].assign_attrs({"area": swath})
    dset = dset.assign_attrs(area=swath)

    # dset.drop('x')
    # dset.drop('y')

    return dset


class ModelBin(object):
    """
       represents a binary cdump (concentration) output file from HYSPLIT
       methods:
       readfile - opens and reads contents of cdump file into an xarray
       self.dset
    """

    def __init__(
        self, filename, drange=None, century=None, verbose=True, readwrite="r"
    ):
        """
        drange :  list of two datetime objects.
        The read method will store data from the cdump file for which the
        sample start is greater thand drange[0] and less than drange[1]
        for which the sample stop is less than drange[1].

        century : integer
        verbose : boolean
        read

        """
        self.drange = drange
        self.filename = filename
        self.century = century
        self.verbose = verbose
        # list of tuples (date1, date2)  of averaging periods with zero
        # concentrations
        self.zeroconcdates = []
        # list of tuples  of averaging periods with nonzero concentrtations]
        self.nonzeroconcdates = []
        self.sourcedate = []
        self.slat = []
        self.slon = []
        self.sht = []
        self.atthash = {}
        self.atthash["Starting Locations"] = []
        self.atthash["Source Date"] = []
        self.llcrnr_lon = None
        self.llcrnr_lat = None
        self.nlat = None
        self.nlon = None
        self.dlat = None
        self.dlon = None
        self.levels = None

        if readwrite == "r":
            self.dataflag = self.readfile(
                filename, drange, verbose=verbose, century=century
            )

    @staticmethod
    def define_struct():
        """Each record in the fortran binary begins and ends with 4 bytes which
        specify the length of the record. These bytes are called pad below.
        They are not used here, but are thrown out. The following block defines
        a numpy dtype object for each record in the binary file. """
        from numpy import dtype

        real4 = ">f"
        int4 = ">i"
        int2 = ">i2"
        char4 = ">a4"

        rec1 = dtype(
            [
                ("pad1", int4),
                ("model_id", char4),  # meteorological model id
                ("met_year", int4),  # meteorological model starting time
                ("met_month", int4),
                ("met_day", int4),
                ("met_hr", int4),
                ("met_fhr", int4),  # forecast hour
                ("start_loc", int4),  # number of starting locations
                ("conc_pack", int4),  # concentration packing flag (0=no, 1=yes)
                ("pad2", int4),
            ]
        )

        # start_loc in rec1 tell how many rec there are.
        rec2 = dtype(
            [
                ("pad1", int4),
                ("r_year", int4),  # release starting time
                ("r_month", int4),
                ("r_day", int4),
                ("r_hr", int4),
                ("s_lat", real4),  # Release location
                ("s_lon", real4),
                ("s_ht", real4),
                ("r_min", int4),  # release startime time (minutes)
                ("pad2", int4),
            ]
        )

        rec3 = dtype(
            [
                ("pad1", int4),
                ("nlat", int4),
                ("nlon", int4),
                ("dlat", real4),
                ("dlon", real4),
                ("llcrnr_lat", real4),
                ("llcrnr_lon", real4),
                ("pad2", int4),
            ]
        )

        rec4a = dtype(
            [
                ("pad1", int4),
                ("nlev", int4),  # number of vertical levels in concentration grid
            ]
        )

        rec4b = dtype([("levht", int4)])  # height of each level (meters above ground)

        rec5a = dtype(
            [
                ("pad1", int4),
                ("pad2", int4),
                ("pollnum", int4),  # number of different pollutants
            ]
        )

        rec5b = dtype([("pname", char4)])  # identification string for each pollutant

        rec5c = dtype([("pad2", int4)])

        rec6 = dtype(
            [
                ("pad1", int4),
                ("oyear", int4),  # sample start time.
                ("omonth", int4),
                ("oday", int4),
                ("ohr", int4),
                ("omin", int4),
                ("oforecast", int4),
                ("pad3", int4),
            ]
        )

        # rec7 has same form as rec6.            #sample stop time.

        # record 8 is pollutant type identification string, output level.

        rec8a = dtype(
            [
                ("pad1", int4),
                ("poll", char4),  # pollutant identification string
                ("lev", int4),
                ("ne", int4),  # number of elements
            ]
        )

        rec8b = dtype(
            [
                ("indx", int2),  # longitude index
                ("jndx", int2),  # latitude index
                ("conc", real4),
            ]
        )

        rec8c = dtype([("pad2", int4)])
        recs = (
            rec1,
            rec2,
            rec3,
            rec4a,
            rec4b,
            rec5a,
            rec5b,
            rec5c,
            rec6,
            rec8a,
            rec8b,
            rec8c,
        )
        return recs

    def parse_header(self, hdata1):
        """
        hdata1 : dtype
        Returns
        nstartloc : int
           number of starting locations in file.
        """
        if len(hdata1["start_loc"]) != 1:
            print(
                "WARNING in ModelBin _readfile - number of starting locations "
                "incorrect"
            )
            print(hdata1["start_loc"])
        # in python 3 np.fromfile reads the record into a list even if it is
        # just one number.
        # so if the length of this record is greater than one something is
        # wrong.
        nstartloc = hdata1["start_loc"][0]
        self.atthash["Meteorological Model ID"] = hdata1["model_id"][0].decode("UTF-8")
        self.atthash["Number Start Locations"] = nstartloc
        return nstartloc

    def parse_hdata2(self, hdata2, nstartloc, century):

        # Loop through starting locations
        for nnn in range(0, nstartloc):
            # create list of starting latitudes, longitudes and heights.
            self.slat.append(hdata2["s_lat"][nnn])
            self.slon.append(hdata2["s_lon"][nnn])
            self.sht.append(hdata2["s_ht"][nnn])
            self.atthash["Starting Locations"].append(
                (hdata2["s_lat"][nnn], hdata2["s_lon"][nnn])
            )

            # try to guess century if century not given
            if century is None:
                if hdata2["r_year"][0] < 50:
                    century = 2000
                else:
                    century = 1900
                print(
                    "WARNING: Guessing Century for HYSPLIT concentration file", century
                )
            # add sourcedate which is datetime.datetime object
            sourcedate = datetime.datetime(
                century + hdata2["r_year"][nnn],
                hdata2["r_month"][nnn],
                hdata2["r_day"][nnn],
                hdata2["r_hr"][nnn],
                hdata2["r_min"][nnn],
            )
            self.sourcedate.append(sourcedate)
            self.atthash["Source Date"].append(sourcedate)
            return century

    def parse_hdata3(self, hdata3, ahash):
        # Description of concentration grid
        ahash["Number Lat Points"] = hdata3["nlat"][0]
        ahash["Number Lon Points"] = hdata3["nlon"][0]
        ahash["Latitude Spacing"] = hdata3["dlat"][0]
        ahash["Longitude Spacing"] = hdata3["dlon"][0]
        ahash["llcrnr longitude"] = hdata3["llcrnr_lon"][0]
        ahash["llrcrnr latitude"] = hdata3["llcrnr_lat"][0]

        self.llcrnr_lon = hdata3["llcrnr_lon"][0]
        self.llcrnr_lat = hdata3["llcrnr_lat"][0]
        self.nlat = hdata3["nlat"][0]
        self.nlon = hdata3["nlon"][0]
        self.dlat = hdata3["dlat"][0]
        self.dlon = hdata3["dlon"][0]
        return ahash

    def parse_hdata4(self, hdata4a, hdata4b):
        self.levels = hdata4b["levht"]
        self.atthash["Number of Levels"] = hdata4a["nlev"][0]
        self.atthash["Level top heights (m)"] = hdata4b["levht"]

    def parse_hdata6and7(self, hdata6, hdata7, century):

        # if no data read then break out of the while loop.
        if not hdata6:
            return False, None, None
        # if verbose:
        #    print('REC 6 & 7 ***************')
        #    print(hdata6)
        #    print(hdata7)
        # pdate1 is the sample start
        # pdate2 is the sample stop
        pdate1 = datetime.datetime(
            century + hdata6["oyear"], hdata6["omonth"], hdata6["oday"], hdata6["ohr"]
        )
        pdate2 = datetime.datetime(
            century + hdata7["oyear"], hdata7["omonth"], hdata7["oday"], hdata7["ohr"]
        )
        self.atthash["Sampling Time"] = pdate2 - pdate1
        return True, pdate1, pdate2

    def parse_hdata8(self, hdata8a, hdata8b, pdate1):
        """
        hdata8a : dtype
        hdata8b : dtype
        pdate1  : datetime

        Returns:
        concframe : DataFrame
        """
        lev_name = hdata8a["lev"][0]
        col_name = hdata8a["poll"][0].decode("UTF-8")
        ndata = hdata8b.byteswap().newbyteorder()  # otherwise get endian error.
        concframe = pd.DataFrame.from_records(ndata)
        # add latitude longitude columns
        # lat = arange(self.llcrnr_lat,
        #             self.llcrnr_lat + self.nlat * self.dlat,
        #             self.dlat)
        # lon = arange(self.llcrnr_lon,
        #             self.llcrnr_lon + self.nlon * self.dlon,
        #             self.dlon)

        # def flat(x):
        #    return lat[x - 1]
        #
        # def flon(x):
        #    return lon[x - 1]

        # concframe['latitude'] = concframe['jndx'].apply(flat)
        # concframe['longitude'] = concframe['indx'].apply(flon)
        # concframe.drop(['jndx', 'indx'], axis=1, inplace=True)
        concframe["levels"] = lev_name
        concframe["time"] = pdate1

        # rename jndx x
        # rename indx y
        names = concframe.columns.values
        names = ["y" if x == "jndx" else x for x in names]
        names = ["x" if x == "indx" else x for x in names]
        names = ["z" if x == "levels" else x for x in names]
        concframe.columns = names
        concframe.set_index(
            # ['time', 'levels', 'longitude', 'latitude'],
            # ['time', 'levels', 'longitude', 'latitude','x','y'],
            ["time", "z", "y", "x"],
            inplace=True,
        )
        concframe.rename(columns={"conc": col_name}, inplace=True)
        # mgrid = np.meshgrid(lat, lon)
        return concframe

    def makegrid(self, xindx, yindx):
        lat = arange(
            self.llcrnr_lat, self.llcrnr_lat + self.nlat * self.dlat, self.dlat
        )
        lon = arange(
            self.llcrnr_lon, self.llcrnr_lon + self.nlon * self.dlon, self.dlon
        )
        lonlist = [lon[x - 1] for x in xindx]
        latlist = [lat[x - 1] for x in yindx]
        mgrid = np.meshgrid(lonlist, latlist)
        return mgrid

    def readfile(self, filename, drange, verbose, century):
        """Data from the file is stored in an xarray, self.dset
           returns False if all concentrations are zero else returns True.
           INPUTS
           filename - name of cdump file to open
           drange - [date1, date2] - range of dates to load data for. if []
                    then loads all data.
                    date1 and date2  should be datetime ojbects.
           verbose - turns on print statements
           century - if None will try to guess the century by looking
                    at the last two digits of the year.
           For python 3 the numpy char4 are read in as a numpy.bytes_
            class and need to be converted to a python
           string by using decode('UTF-8').

        """
        # 8/16/2016 moved species=[]  to before while loop. Added print
        # statements when verbose.
        self.dset = None
        # dictionaries which will be turned into the dset attributes.
        ahash = {}
        fp = open(filename, "rb")

        # each record in the fortran binary begins and ends with 4 bytes which
        # specify the length of the record.
        # These bytes are called pad1 and pad2 below. They are not used here,
        # but are thrown out.
        # The following block defines a numpy dtype object for each record in
        # the binary file.
        recs = self.define_struct()
        rec1, rec2, rec3, rec4a = recs[0], recs[1], recs[2], recs[3]
        rec4b, rec5a, rec5b, rec5c = recs[4], recs[5], recs[6], recs[7]
        rec6, rec8a, rec8b, rec8c = recs[8], recs[9], recs[10], recs[11]
        # rec7 = rec6
        # start_loc in rec1 tell how many rec there are.
        tempzeroconcdates = []
        # Reads header data. This consists of records 1-5.
        hdata1 = fromfile(fp, dtype=rec1, count=1)
        nstartloc = self.parse_header(hdata1)

        hdata2 = fromfile(fp, dtype=rec2, count=nstartloc)
        century = self.parse_hdata2(hdata2, nstartloc, century)

        hdata3 = fromfile(fp, dtype=rec3, count=1)
        ahash = self.parse_hdata3(hdata3, ahash)

        # read record 4 which gives information about vertical levels.
        hdata4a = fromfile(fp, dtype=rec4a, count=1)  # gets nmber of levels
        hdata4b = fromfile(
            fp, dtype=rec4b, count=hdata4a["nlev"][0]
        )  # reads levels, count is number of levels.
        self.parse_hdata4(hdata4a, hdata4b)

        # read record 5 which gives information about pollutants / species.
        hdata5a = fromfile(fp, dtype=rec5a, count=1)
        fromfile(fp, dtype=rec5b, count=hdata5a["pollnum"][0])
        fromfile(fp, dtype=rec5c, count=1)
        # hdata5b = fromfile(fp, dtype=rec5b, count=hdata5a['pollnum'][0])
        # hdata5c = fromfile(fp, dtype=rec5c, count=1)
        self.atthash["Number of Species"] = hdata5a["pollnum"][0]

        # Loop to reads records 6-8. Number of loops is equal to number of
        # output times.
        # Only save data for output times within drange. if drange=[] then
        # save all.
        # Loop to go through each sampling time
        ii = 0  # check to make sure don't go above max number of iterations
        iii = 0  # checks to see if some nonzero data was saved in xarray
        # Safety valve - will not allow more than 1000 loops to be executed.
        imax = 1e3
        testf = True
        while testf:
            hdata6 = fromfile(fp, dtype=rec6, count=1)
            hdata7 = fromfile(fp, dtype=rec6, count=1)
            check, pdate1, pdate2 = self.parse_hdata6and7(hdata6, hdata7, century)
            if not check:
                break
            testf, savedata = check_drange(drange, pdate1, pdate2, verbose)
            print("Sample time", pdate1, " to ", pdate2)
            # datelist = []
            self.atthash["Species ID"] = []
            inc_iii = False
            # LOOP to go through each level
            for lev in range(self.atthash["Number of Levels"]):
                # LOOP to go through each pollutant
                for pollutant in range(self.atthash["Number of Species"]):
                    # record 8a has the number of elements (ne). If number of
                    # elements greater than 0 than there are concentrations.
                    hdata8a = fromfile(fp, dtype=rec8a, count=1)
                    self.atthash["Species ID"].append(
                        hdata8a["poll"][0].decode("UTF-8")
                    )
                    # if number of elements is nonzero then
                    if hdata8a["ne"] >= 1:
                        # get rec8 - indx and jndx
                        hdata8b = fromfile(fp, dtype=rec8b, count=hdata8a["ne"][0])
                        # add sample start time to list of start times with
                        # non zero conc
                        self.nonzeroconcdates.append(pdate1)
                    else:
                        tempzeroconcdates.append(
                            pdate1
                        )  # or add sample start time to list of start times
                        # with zero conc.
                    # This is just padding.
                    fromfile(fp, dtype=rec8c, count=1)
                    # if savedata is set and nonzero concentrations then save
                    # the data in a pandas dataframe
                    if savedata and hdata8a["ne"] >= 1:
                        self.nonzeroconcdates.append(pdate1)
                        inc_iii = True
                        concframe = self.parse_hdata8(hdata8a, hdata8b, pdate1)
                        dset = xr.Dataset.from_dataframe(concframe)
                        if verbose:
                            print("Adding ", "Pollutant", pollutant, "Level", lev)
                        # if this is the first time through. create dataframe
                        # for first level and pollutant.
                        if self.dset is None:
                            self.dset = dset
                        else:  # create dataframe for level and pollutant and
                            # then merge with main dataframe.
                            # self.dset = xr.concat([self.dset, dset],'levels')
                            self.dset = xr.merge([self.dset, dset])
                        ii += 1
                # END LOOP to go through each pollutant
            # END LOOP to go through each level
            # safety check - will stop sampling time while loop if goes over
            #  imax iterations.
            if ii > imax:
                testf = False
            if inc_iii:
                iii += 1
        self.atthash["Concentration Grid"] = ahash
        self.atthash["Species ID"] = list(set(self.atthash["Species ID"]))
        self.atthash["Coordinate time description"] = "Beginning of sampling time"
        # END OF Loop to go through each sampling time
        if self.dset.variables:
            self.dset.attrs = self.atthash
            mgrid = self.makegrid(self.dset.coords["x"], self.dset.coords["y"])
            self.dset = self.dset.assign_coords(longitude=(("y", "x"), mgrid[0]))
            self.dset = self.dset.assign_coords(latitude=(("y", "x"), mgrid[1]))

            self.dset = self.dset.reset_coords()
            self.dset = self.dset.set_coords(["time", "latitude", "longitude"])
        if verbose:
            print(self.dset)
        if iii == 0:
            print(
                "Warning: ModelBin class _readfile method: no data in the date range found"
            )
            return False
        return True
