# forecast_data.py
# Functions designed to find the correct directories and meteorological files for
# a HYSPLIT run
import os

# Find met files for HYSPLIT forecast runs


def findcycles_forecast(dstart, metdata):
    """dstart : datetime object. start date
       metdata : str
       GFS, GFS0p5, GFS0p25, NAM 12 km, NAMAK, NAMHI
    """
    FCTDIR = '/pub/forecast/'
    cycles = ["t00z", "t06z", "t12z", "t18z"]
    ctimes = [0, 6, 12, 18]
    metdirlist = []
    metfilelist = []
    days = [""]
    if metdata == "GFS":
        meta = "gfsa"
        met = "gfsf"
    elif metdata == "GFS0p5":
        met = "gfs0p5f"
    elif metdata == "GFS0p25":
        met = "gfs0p25"
    elif metdata == "NAM 12 km":
        met = "namf"
    elif metdata == "NAMAK":
        met = "namsf.AK"
        meta = "namsa.AK"
    elif metdata == "NAMHI":
        met = "namsf.HI"
        meta = "namsa.HI"
    metdir1 = FCTDIR + dstart.strftime("%Y%m%d") + '/'
    #print('<br>' + metdir1)
    cyclename = 'None'
    metnamefinal = 'No data found'
    for cyc, tms in zip(cycles, ctimes):
        metfilename = 'hysplit.' + cyc + '.' + met + 'f'
        metname = metdir1 + metfilename
        if os.path.isfile(metname):
            if tms <= dstart.hour:
                metnamefinal = metfilename
                cyclename = str(tms) + ' UTC'
    for fs in days:
        metfilelist.append(metnamefinal + fs)
        metdirlist.append(metdir1)
    return metdirlist, metfilelist


# Find met files for HYSPLIT archive runs


def findcycles_archive(dstart, dend, metdata, direction):
    """dstart : datetime object start date
        dend : datetime object end date
        metdata : string : GDAS0p5, GDAS1
        direction: string : Forward, Back
   """
    from datetime import timedelta as td
    #from datetime import date
    DIR = '/pub/archives/'
    metdirlist = []
    metfilelist = []
    datelist = []
    if metdata == "GFS0p25":
        met = "gfs0p25"
        metdir1 = DIR + met + '/'
        if (direction == 'Forward'):
            datelist.append(dstart)
            while dstart <= dend:
                datelist.append(dstart + td(days=1))
                dstart += td(days=1)
        elif (direction == 'Back'):
            while dstart >= dend:
                datelist.append(dstart - td(days=1))
                dstart -= td(days=1)
        y = 0
        while y < len(datelist):
            metfilelist.append(datelist[y].strftime("%Y%m%d") + '_' + met)
            metdirlist.append(metdir1)
            y += 1
    elif metdata == "GDAS1":
        met = "gdas1"
        metdir1 = DIR + met + '/'
        smonyr = dstart.strftime('%b%y').lower()
        emonyr = dend.strftime('%b%y').lower()
        if (smonyr == emonyr):
            sweek = (dstart.day - 1) // 7 + 1
            eweek = (dend.day - 1) // 7 + 1
            if (sweek == eweek):
                metdirlist.append(metdir1)
                metfilelist.append(met + '.' + smonyr + '.w' + str(sweek))
            if (sweek != eweek):
                if (direction == 'Forward'):
                    nweeks = list(range(sweek, eweek + 1, 1))
                if (direction == 'Back'):
                    nweeks = list(range(sweek, eweek - 1, -1))
                    y = 0
                    while y < len(nweeks):
                        metdirlist.append(metdir1)
                        metfilelist.append(met + '.' + smonyr + '.w' +
                                           str(nweeks[y]))
                        y += 1
        if smonyr != emonyr:
            sweek = (dstart.day - 1) // 7 + 1
            eweek = (dend.day - 1) // 7 + 1
            ndays = abs((dend - dstart).days)
            if ndays < 14:
                metdirlist.append(metdir1)
                metfilelist.append(met + '.' + smonyr + '.w' + str(sweek))
                metdirlist.append(metdir1)
                metfilelist.append(met + '.' + emonyr + '.w' + str(eweek))
            if ndays > 14:
                dstep = ndays // 7 + 1
                tmpdate = dstart
                for x in range(0, dstep + 1):
                    if (direction == 'Forward'):
                        tmpdate = tmpdate + td(days=7 * x)
                    if (direction == 'Back'):
                        tmpdate = tmpdate - td(days=7 * x)
                    tmpweek = (tmpdate.day - 1) // 7 + 1
                    tmpmonyr = tmpdate.strftime('%b%y').lower()
                    metdirlist.append(metdir1)
                    metfilelist.append(met + '.' + tmpmonyr + '.w' +
                                       str(tmpweek))
    return metdirlist, metfilelist
