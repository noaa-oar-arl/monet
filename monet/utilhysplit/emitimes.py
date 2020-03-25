# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import datetime
from operator import itemgetter

import numpy as np


"""
Classes to read and write input file for HYSPLIT.
Emissions file see https://ready.arl.noaa.gov/hysplitusersguide/S417.htm
   class EmiTimes : EMITTIMES file which can be used as input to HYSPLIT.
                    to specify emissions.
   class EmitCycle : one cycle in an EMITTIMES file
   class EmitLine  : one line in an EMITTIMES file
"""


class EmiTimes(object):
    """
    Class to represent and EMITTIMES file which can be used as input to HYSPLIT.
    Helper classes are EmitCycle which represents one emissions Cycle in the
    file and EmitLine which represents one line in the file.


    General usage is to initialize the class
    efile = EmiTimes(filename)

    Then either read an existing file.
    efile.read_file()
    When reading a file, number of species must be specified if more than one.
    efile.read_file(num_species=3)

    or create a new file by first adding cycles.
    efile.add_cycle(start_date, duration)
    and then adding records
    efile.add_record(date, duraction, lat, lon, height, rate, area, heat, spnum)
    The spnum indicates which species the record is for. Default is 1.
    The spnum indicates the position in the CONTROL file of the species.
    1 is the first species type defined. 2 is the second species type defined
    etc.

    The class will automatically put the record into the correct emission
    cycle based on the date.
    efile.write_new(filename)

    The class will also automatically write dummy lines with 0 emissions to
    make sure that each emission cycle has the same number of lines (which is
    required by HYSPLIT).

    The class will also make sure that there is one record for each species.
    Dummy lines with 0 emissions will be written if a record for a species is
    missing.  Each record is associated with a species. The program sorts the
    records in each cycle in order of date and then species number. It makes
    sure that records are always written with species number 1,2,3...N,
    1,2,3..N.

    other capabilities include

    efile.filter_records:remove records which lie outside a defined
                         rectangular area.

    """
    def __init__(self, filename="EMITIMES.txt", nanvalue=None, species=[1]):
        self.filename = filename
        self.cycle_list = []  # list of EmitCycle objects.
        self.ncycles = 0
        self.chash = {}
        self.sdatelist = []
        self.nanvalue = nanvalue  # if NaN shows up, what value to use.
        # if None, will throw and error if there a Nan
        self.splist = [1]  # list of species in the file
        # val - name of species. key - position (1 to..)
        self.sphash = {1: "P001"}
        self.header = self.header_str()

    def header_str(self):
        """
        default header string for EMITTIMES file
        RETURNS

        returnval : str
        """
        returnval = "YYYY MM DD HH    DURATION(hhhh) #RECORDS #spnum "
        for val in self.splist:
            returnval += str(self.sphash[val]) + " "
        returnval += "\n"
        returnval += "YYYY MM DD HH MM DURATION(hhmm) "
        returnval += "LAT LON HGT(m) RATE(/h) AREA(m2) HEAT(w)  \n"
        return returnval

    def modify_header(self, hstring):
        self.header = hstring

    def set_species(self, sphash):
        for val in self.splist:
            # if there is a species not
            # in the hash then make a new name.
            if val not in sphash.keys():
                # print('setting species name')
                sphash[val] = "P" + str(val)
        self.sphash = sphash
        # re-write the header string whenever updating the species.
        self.header = self.header_str()

    def get_species(self):
        """
        This determines number of species from the cycles.
        """
        return list(np.arange(1, self.findmaxsp() + 1))

    def findmaxsp(self):
        """
        find cycle with the most species.
        """
        maxsp = 1
        for ec in self.cycle_list:
            spnum = ec.splist[-1]
            if spnum > maxsp:
                maxsp = spnum
        return maxsp

    def findmaxrec(self):
        """
        Find cycle with the most records and return number of records in that
        cycle.
        This is used when writing an EmitTimes file since HYSPLIT
        requires each cycle to have the same number of records.
        Cycles with less records will have dummy records added.
        Returns
        maxrec : int
           maximum number of records.
        """
        maxrec = 0
        for ec in self.cycle_list:
            if ec.nrecs > maxrec:
                maxrec = ec.nrecs
        return maxrec

    def write_new(self, filename):
        """
        write a new EmitTimes file to filename.
        filename : str
        """
        # make sure all cycles have same number of species.
        self.splist = list(range(1, self.findmaxsp() + 1))
        # make sure that there is a name for each species.
        self.set_species(self.sphash)
        # print(splist)
        for ecycle in self.cycle_list:
            ecycle.splist = self.splist
            ecycle.fill_species()
        # make sure all cycles have same number of records
        maxrec = self.findmaxrec()
        with open(filename, "w") as fid:
            fid.write(self.header)
        for ecycle in self.cycle_list:
            # if the recordra is empty then there are no emissions
            # for that cycle and it can be skipped.
            if ecycle.recordra:
                for iii in range(0, maxrec - ecycle.nrecs):
                    ecycle.add_dummy_record()
                ecycle.write_new(filename)
        # print('end file', filename)

    def header2sp(self):
        """
        check if information on species is stored in the first line.
        """
        rval = False
        if "spnum" in self.header:
            rval = True
            temp = self.header.split("\n")[0]
            temp = temp.split("spnum")[1]
            temp = temp.split()
            iii = 1
            sphash = {}
            for val in temp:
                sphash[iii] = val
                iii += 1
            self.splist = np.arange(1, iii)
            self.sphash = sphash
        return rval

    def read_file(self, verbose=False, num_species=1):
        """
        Reads an EmitTimes file.
        verbose: boolean

        num_species is used to determine how many species are represented.
        Default is 1.
        if this information is in the header, that will be used instead.

        Returns False if EmitTimes file is empty

        """
        with open(self.filename, "r") as fid:
            lines = fid.readlines()
            self.header = lines[0] + "/n" + lines[1]

            # check to see if header contains info on particle species
            # if it does not then get the splist from num_species input.
            if not self.header2sp():
                self.splist = np.arange(1, num_species + 1)
            iii = 2
            while iii < len(lines):
                if verbose:
                    print("NEW CYCLE")
                # pass on particle species info to the EmitCycle object.
                ec = EmitCycle(splist=self.splist)
                # the third line (0,1,2) is the cycle header line.
                nrecs = ec.parse_header(lines[iii])
                check = ec.read_cycle(lines[iii + 1:iii + nrecs + 1])
                if not check:
                    break
                else:
                    self.cycle_list.append(ec)
                    self.ncycles += 1
                iii += nrecs + 1
        if self.ncycles == 0:
            return False
        else:
            return True

    def add_cycle(self, sdate, duration):
        """
        Adds information on a cycle to an EmiTimes object.
        sdate: datetime object
               start time of cycle.
        duration : integer
               duratio in hours of cycle.
        """
        self.ncycles += 1
        ec = EmitCycle(sdate, duration)
        self.cycle_list.append(ec)
        d1 = sdate
        dt = datetime.timedelta(hours=int(duration))
        d2 = sdate + dt
        self.chash[self.ncycles - 1] = (d1, d2)
        self.sdatelist.append(sdate)

    def filter_records(self, llcrnr, urcrnr):
        """ removes records which are outside the box
            described by llcrnr = (lat, lon) lower left corner
                         urcrnr = (lat, lon) upper right corner
        """
        for ec in self.cycle_list:
            ec.filter_records(llcrnr, urcrnr)

    def add_record(self,
                   date,
                   duration,
                   lat,
                   lon,
                   height,
                   rate,
                   area,
                   heat,
                   spnum=1,
                   nanvalue=0):
        """
        adds a record to a cycle based on the date of the record.
        Returns:
           rvalue : boolean
           False if no cycle could be found to add the record to.
        """
        # This block determines which cycle the record goes into
        # based on the date.
        cycle_number = -1
        for ccc in self.chash:
            if date >= self.chash[ccc][0] and date < self.chash[ccc][1]:
                cycle_number = ccc
        if cycle_number == -1:
            rvalue = False
        else:
            self.cycle_list[cycle_number].add_record(date, duration, lat, lon,
                                                     height, rate, area, heat,
                                                     spnum, nanvalue)
            rvalue = True
        return rvalue


class EmitCycle(object):
    """Helper class for EmitTimes
    This represents a cycle in an EmitTimes file.
    Each cycle begins with a line which has the start date, duration
    and number of records. Then the records follow.
    """

    # def __init__(self, filename='EMITIMES.txt'):

    def __init__(self, sdate=None, duration=None, splist=[1]):
        self.sdate = sdate
        self.duration = duration  # duration of the cycle.
        self.recordra = []
        # number of records in a cycle.
        self.nrecs = 0
        # number of locations in a cycle.
        # this will be nrecs / len(splist)
        self.nlocs = 0
        # all cycles in a file must have same number of records.
        # so some cycles may need to have dummy records
        # with zero emissions.
        self.dummy_recordra = []
        self.drecs = 0
        self.splist = splist  # list of ints starting with 1.

    def sort(self):
        # sort records according to date and then spnum.
        # then lat, lon and height.
        # this sort order will allow line sources to be specified properly.
        #
        self.recordra.sort(
            key=lambda x: (x.date, x.spnum, x.lat, x.lon, x.height))
        return -1

    def fill_species(self):
        """
        make sure one record written for each species.
        """
        rstr = ""
        if self.splist[0] == 0:
            print("WARNING EmitCycle: species list should start with 1")
        self.sort()
        nlist = []
        slist = []
        for rc in self.recordra:
            nlist.append((rc.date, rc.lat, rc.lon, rc.height))
            slist.append(rc.spnum)
        # list of all date, lat, lon height locations
        nlist = sorted(set(nlist))
        # nlist = sorted(nlist, key=itemgetter(0))
        # list of all species numbers.
        slist = list(set(slist))
        alist = []
        # list with one species for each date, lat, lon, height location.
        alist = []
        for nnn in nlist:
            for sss in self.splist:
                alist.append((nnn, sss))
        # now need to make sure records match the alist. and fill in ones that
        # don't.
        jjj = 0
        new_records = []
        for rc in self.recordra:
            nnn = (rc.date, rc.lat, rc.lon, rc.height)
            sss = rc.spnum
            while (nnn, sss) != alist[jjj]:
                # print('JJJ', jjj)
                date = alist[jjj][0][0]
                lat = alist[jjj][0][1]
                lon = alist[jjj][0][2]
                ht = alist[jjj][0][3]
                spnum = alist[jjj][1]
                new_records.append(
                    EmitLine(date, "0100", lat, lon, 0, 0, 0, 0, spnum))
                jjj += 1
            jjj += 1

        for iii in range(jjj, len(alist)):
            date = alist[iii][0][0]
            lat = alist[iii][0][1]
            lon = alist[iii][0][2]
            ht = alist[iii][0][3]
            spnum = alist[iii][1]
            new_records.append(
                EmitLine(date, "0100", lat, lon, 0, 0, 0, 0, spnum))

        for rec in new_records:
            self.add_emitline(rec)
        self.sort()
        return 1

    def parse_header(self, header):
        """
        read header in the file.
        """
        temp = header.split()
        year = int(temp[0])
        month = int(temp[1])
        day = int(temp[2])
        hour = int(temp[3])
        # minute = int(temp[4])
        dhour = int(temp[4])
        nrecs = int(temp[5])
        self.sdate = datetime.datetime(year, month, day, hour)
        self.duration = datetime.timedelta(hours=dhour)
        self.nrecs = nrecs
        return nrecs

    def write_new(self, filename):
        """
        write new emittimes file.
        """
        # if len(self.splist)>1: self.fill_species()
        maxrec = self.nrecs + self.drecs
        datestr = self.sdate.strftime("%Y %m %d %H ")
        # print('FILENAME EMIT', filename)
        with open(filename, "a") as fid:
            # fid.write(self.header_str())
            fid.write(datestr + " " + self.duration + " " + str(maxrec) + "\n")
            for record in self.recordra:
                fid.write(str(record))
            for record in self.dummy_recordra:
                fid.write(str(record))

    def parse_record(self, record, spnum=1):
        """
        Takes a string which is a line in an EMITTIMES file
        specifying an emission and turn it into an EmitLine object.
        record : string
        spnum : int
               indicates species number
               (position in CONTROL file starting with 1)
        RETURNS
        EmitLine object.
        """
        temp = record.split()
        year = int(temp[0])
        month = int(temp[1])
        day = int(temp[2])
        hour = int(temp[3])
        # dhour = int(temp[5][0:2])
        # dmin = int(temp[5][-2:])
        duration = temp[5]
        sdate = datetime.datetime(year, month, day, hour)
        lat = float(temp[6])
        lon = float(temp[7])
        ht = float(temp[8])
        rate = float(temp[9])
        spnum = int(spnum)
        try:
            area = float(temp[10])
        except BaseException:
            area = 0
        try:
            heat = float(temp[11])
        except BaseException:
            heat = 0
        return EmitLine(sdate, duration, lat, lon, ht, rate, area, heat, spnum)

    def add_dummy_record(self):
        """uses last record in the recordra to get date and position"""
        rc = self.recordra[-1]

        # need to make lat lon slightly different or HYSPLIT
        # will think these are line sources and not calculate number
        # of particles to emit correctly in emstmp.f
        lat = rc.lat + np.random.rand(1)[0] * 10
        lon = rc.lon + np.random.rand(1)[0] * 10
        eline = EmitLine(rc.date, rc.duration, lat, lon, 0, 0, 0, rc.spnum, 0)
        self.dummy_recordra.append(eline)
        self.drecs += 1

    def add_emitline(self, eline, nanvalue=0):
        self.recordra.append(eline)
        if eline.spnum not in self.splist:
            self.splist.append(spnum)
            self.splist.sort()
        self.nrecs += 1

    def add_record(self,
                   sdate,
                   duration,
                   lat,
                   lon,
                   ht,
                   rate,
                   area,
                   heat,
                   spnum=1,
                   nanvalue=0):
        """Inputs
        sdate
        duration
        lat
        lon
        height
        rate
        area
        heat
        """
        if spnum == 0:
            import sys

            print(
                "ERROR in add_record",
                sdate,
                duration,
                lat,
                lon,
                ht,
                rate,
                area,
                heat,
                spnum,
            )
            sys.exit()
        eline = EmitLine(sdate, duration, lat, lon, ht, rate, area, heat,
                         spnum, nanvalue)
        self.recordra.append(eline)
        if spnum not in self.splist:
            self.splist.append(spnum)
            self.splist.sort()
        self.nrecs += 1

    def read_cycle_header(self, header, verbose=False):
        """
        Read line containing header information for the emisson cycle.
        header : str
        verbose : boolean
        """
        nrecs = self.parse_header(header)
        if verbose:
            print("HEADER", header)
        return nrecs

    def read_cycle(self, lines, num_species=1, verbose=False):
        """
        Take lines from an emittimes file and turn them into
        instances of the EmitLine  class. Add them to the list of
        EmitLine objects.
        lines : list of str
        verbose: boolean
        TO DO: take into account number of species.
        """
        check = True
        recordra = []
        jjj = 1
        for temp in lines:
            if verbose:
                print("Line", temp)
            # parse record returns EmitLine object.
            recordra.append(self.parse_record(temp, spnum=jjj))
            jjj += 1
            if jjj > len(self.splist):
                jjj = 1
        self.recordra.extend(recordra)
        return check

    def filter_records(self, llcrnr, urcrnr):
        """ removes records which are outside the box
            described by llcrnr = (lat, lon) lower left corner
                         urcrnr = (lat, lon) upper right corner
        """
        iii = 0
        rrr = []
        for record in self.recordra:
            if record.lat < llcrnr[1] or record.lat > urcrnr[1]:
                rrr.append(iii)
            elif record.lon < llcrnr[0] or record.lon > urcrnr[0]:
                rrr.append(iii)
            iii += 1
        for iii in sorted(rrr, reverse=True):
            self.recordra.pop(iii)
            self.nrecs -= 1


class EmitLine(object):
    """
    Helper class for EmiTimes and EmitCycle.
    Represents one line in ane EMITTIMES file.

    methods:
    __init__
    checknan
    __str__

    """

    def __init__(
            self,
            date,
            duration,
            lat,
            lon,
            height,
            rate,
            area=0,
            heat=0,
            spnum=1,
            nanvalue=0,
            verbose=False,
    ):
        self.date = date
        self.duration = duration
        self.lat = lat
        self.lon = lon
        self.height = height
        self.rate = rate
        self.area = area
        self.heat = heat
        self.message = ""
        self.spnum = spnum
        nanpresent = self.checknan(nanvalue)
        if nanpresent and verbose:
            print("WARNING: EmitFile NaNs present. \
          Being changed to " + str(nanvalue) + self.message)

    def checknan(self, nanvalue):
        """
        check to see if a nan is in the area or rate field.
        change the nan to self.nanvalue.
        """
        nanpresent = False
        if np.isnan(self.area):
            self.area = nanvalue
            nanpresent = True
            self.message += "area is Nan \n"
        if np.isnan(self.rate):
            self.rate = nanvalue
            nanpresent = True
            self.message += "rate is Nan \n"
        # if np.isnan(self.heat):
        #   self.heat = nanvalue
        #   nanpresent=True
        #   self.message += 'heat is Nan \n'
        return nanpresent

    def __str__(self):
        """
        output in correct format for EMITTIMES file.
        """
        returnstr = self.date.strftime("%Y %m %d %H %M ")
        returnstr += self.duration + " "
        returnstr += "{:1.4f}".format(self.lat) + " "
        returnstr += "{:1.4f}".format(self.lon) + " "
        returnstr += str(self.height) + " "
        returnstr += "{:1.2e}".format(self.rate) + " "
        returnstr += "{:1.2e}".format(self.area) + " "
        try:
            returnstr += "{:1.2e}".format(self.heat)
            returnstr += " \n"
        except BaseException:
            returnstr += str(self.heat)
            returnstr += '\n'
        #returnstr += " " + str(self.spnum) + "\n"
        return returnstr
