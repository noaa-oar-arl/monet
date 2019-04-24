# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import datetime
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

    or create a new file by first adding cycles.
    efile.add_cycle(start_date, duration)
    and then adding records
    efile.add_record(date, duraction, lat, lon, height, rate, area, heat)

    The class will automatically put the record into the correct emission
    cycle based on the date.
    efile.write_new(filename)

    The class will also automatically write dummy lines with 0 emissions to
    make sure that each emission cycle has the same number of lines (which is
    required by HYSPLIT).


    other capabilities include

    efile.filter_records:remove records which lie outside a defined
                         rectangular area.

    """

    def __init__(self, filename='EMITIMES.txt', nanvalue=None):
        self.filename = filename
        self.cycle_list = []  # list of EmitCycle objects.
        self.ncycles = 0
        self.chash = {}
        self.sdatelist = []
        self.nanvalue = nanvalue  # if NaN shows up, what value to use.
        # if None, will throw and error if there a Nan
        self.header = self.header_str()

    def header_str(self):
        """
        default header string for EMITTIMES file
        RETURNS

        returnval : str
        """
        returnval = 'YYYY MM DD HH    DURATION(hhhh) #RECORDS \n'
        returnval += 'YYYY MM DD HH MM DURATION(hhmm) '
        returnval += 'LAT LON HGT(m) RATE(/h) AREA(m2) HEAT(w)  \n'
        return returnval

    def modify_header(self, hstring):
        self.header = hstring

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
        maxrec = self.findmaxrec()
        with open(filename, 'w') as fid:
            fid.write(self.header)
        for ecycle in self.cycle_list:
            for iii in range(0, maxrec - ecycle.nrecs):
                ecycle.add_dummy_record()
            ecycle.write_new(filename)

    def read_file(self, verbose=False):
        """
        Reads an EmitTimes file.
        verbose: boolean
        """
        with open(self.filename, 'r') as fid:
            lines = fid.readlines()
            iii = 2
            while iii < len(lines):
                if verbose:
                    print('NEW CYCLE')
                ec = EmitCycle()
                nrecs = ec.parse_header(lines[iii])
                check = ec.read_cycle(lines[iii + 1: iii + nrecs + 1])
                if not check:
                    break
                else:
                    self.cycle_list.append(ec)
                    self.ncycles += 1
                iii += nrecs + 1

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
            self.cycle_list[cycle_number].add_record(
                date, duration, lat, lon, height, rate, area, heat, nanvalue)
            rvalue = True
        return rvalue


class EmitCycle(object):
    """Helper class for EmitTimes
    This represents a cycle in an EmitTimes file.
    Each cycle begins with a line which has the start date, duration
    and number of records. Then the records follow.
    """

    # def __init__(self, filename='EMITIMES.txt'):

    def __init__(self, sdate=None, duration=None):
        self.sdate = sdate
        self.duration = duration  # duration of the cycle.
        self.recordra = []
        self.nrecs = 0
        # all cycles in a file must have same number of records.
        # so some cycles may need to have dummy records
        # with zero emissions.
        self.dummy_recordra = []
        self.drecs = 0

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
        maxrec = self.nrecs + self.drecs
        datestr = self.sdate.strftime('%Y %m %d %H ')
        # print('FILENAME EMIT', filename)
        with open(filename, 'a') as fid:
            # fid.write(self.header_str())
            fid.write(datestr + ' ' + self.duration + ' ' + str(maxrec) + '\n')
            for record in self.recordra:
                fid.write(str(record))
            for record in self.dummy_recordra:
                fid.write(str(record))

    def parse_record(self, record):
        """
        Takes a string which is a line in an EMITTIMES file
        specifying an emission and turn it into an EmitLine object.
        record : string
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
        try:
            area = float(temp[10])
        except BaseException:
            area = 0
        try:
            heat = float(temp[11])
        except BaseException:
            heat = 0
        return EmitLine(sdate, duration, lat, lon, ht, rate, area, heat)

    def add_dummy_record(self):
        """uses last record in the recordra to get date and position"""
        rc = self.recordra[-1]
        # need to make lat lon slightly different or HYSPLIT
        # will think these are line sources and not calculate number
        # of particles to emit correctly in emstmp.f
        lat = rc.lat + np.random.rand(1)[0] * 10
        lon = rc.lon + np.random.rand(1)[0] * 10
        eline = EmitLine(rc.date, "0100", lat, lon, 0, 0, 0, 0)
        self.dummy_recordra.append(eline)
        self.drecs += 1

    def add_record(self,
                   sdate,
                   duration,
                   lat,
                   lon,
                   ht,
                   rate,
                   area,
                   heat,
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
        eline = EmitLine(sdate, duration, lat, lon, ht, rate, area, heat,
                         nanvalue)
        self.recordra.append(eline)
        self.nrecs += 1

    def read_cycle_header(self, header, verbose=False):
        """
        Read line containing header information for the emisson cycle.
        header : str
        verbose : boolean
        """
        nrecs = self.parse_header(header)
        if verbose:
            print('HEADER', header)
        return nrecs

    def read_cycle(self, lines, verbose=False):
        """
        Take lines from an emittimes file and turn them into
        instances of the EmitLine  class. Add them to the list of
        EmitLine objects.
        lines : list of str
        verbose: boolean
        """
        check = True
        recordra = []
        for temp in lines:
            if verbose:
                print('Line', temp)
            # parse record returns EmitLine object.
            recordra.append(self.parse_record(temp))
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

    def __init__(self,
                 date,
                 duration,
                 lat,
                 lon,
                 height,
                 rate,
                 area=0,
                 heat=0,
                 nanvalue=0):
        self.date = date
        self.duration = duration
        self.lat = lat
        self.lon = lon
        self.height = height
        self.rate = rate
        self.area = area
        self.heat = heat
        self.message = ''
        nanpresent = self.checknan(nanvalue)
        if nanpresent:
            print('WARNING: EmitFile NaNs present. \
          Being changed to ' + str(nanvalue) + self.message)

    def checknan(self, nanvalue):
        """
        check to see if a nan is in the area or rate field.
        change the nan to self.nanvalue.
        """
        nanpresent = False
        if np.isnan(self.area):
            self.area = nanvalue
            nanpresent = True
            self.message += 'area is Nan \n'
        if np.isnan(self.rate):
            self.rate = nanvalue
            nanpresent = True
            self.message += 'rate is Nan \n'
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
        returnstr += self.duration + ' '
        returnstr += '{:1.4f}'.format(self.lat) + ' '
        returnstr += '{:1.4f}'.format(self.lon) + ' '
        returnstr += str(self.height) + ' '
        returnstr += '{:1.2e}'.format(self.rate) + ' '
        returnstr += '{:1.2e}'.format(self.area) + ' '
        try:
            returnstr += '{:1.2e}'.format(self.heat) + ' \n'
        except BaseException:
            returnstr += str(self.heat) + ' \n'
        return returnstr
