import monet.utilhysplit.metfiles as mf
import datetime

d1 = datetime.datetime(2017, 12, 5, 2)
runtime = 50
mdir = '/pub/archives/'
m2dir = mdir + 'wrf27km/%Y/wrfout_d01_%Y%m%d.ARL'
files = mf.getmetfiles(m2dir, d1, runtime)
for fl in files:
    print(fl)

mdir = '/pub/archives/'
m2dir = mdir + 'gdas0p5/%Y%m%d_gdas0p5'
files = mf.getmetfiles(m2dir, d1, runtime)
for fl in files:
    print(fl)

mdir = '/pub/archives/'
m2dir = mdir + 'gdas1/gdas1.%b%d.week'
files = mf.getmetfiles(m2dir, d1, runtime)
for fl in files:
    print(fl)
