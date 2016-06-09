#this is written to retrive airnow data concatenate and add to pandas array for usage
import pandas as pd
def search_listinlist(array1, array2):
    #find intersections
    from numpy import array,concatenate,sort,where,int32
    s1 = set(array1.flatten())
    s2 = set(array2.flatten())
    inter = s1.intersection(s2)
    index1 = array([])
    index2 = array([])
    #find the indexes in array1
    for i in inter:
        index11 = where(array1 == i)
        index22 = where(array2 == i)
        index1 = concatenate([index1[:], index11[0]])
        index2 = concatenate([index2[:], index22[0]])

    return sort(int32(index1)), sort(int32(index2))

class airnow:
    def __init__(self):
        from datetime import datetime
        self.username='Barry.Baker'
        self.password='p00pST!ck123'
        self.url ='ftp.airnowgateway.org'
        self.dates = [datetime.strptime('2016-06-06 12:00:00','%Y-%m-%d %H:%M:%S'),datetime.strptime('2016-06-06 13:00:00','%Y-%m-%d %H:%M:%S')]
        self.datestr = []
        self.ftp = None
        self.output = None

    def retrieve_hourly_filelist(self):
        self.ftp.cwd('HourlyData')
        nlst = self.ftp.nlst('*')[1:]
        return nlst

    def openftp(self):
        from ftplib import FTP
        self.ftp = FTP(self.url)
        self.ftp.login(self.username,self.password)

    def convert_dates_tofnames(self):
        for i in self.dates:
            self.datestr.append(i.strftime('%Y%m%d%H.dat'))

    def retrieve_hourly_files(self):
        from numpy import array
        self.openftp()
        self.convert_dates_tofnames()
        nlst = self.retrieve_hourly_filelist()
        print nlst
        print self.datestr
        index1,index2 = search_listinlist(array(nlst),array(self.datestr))
        
        if index1.shape< 1:
            self.ftp.cwd('Archive')
            year = self.dates[0].strftime('%Y')
            self.ftp.cwd(year)
            nlst = self.ftp.nlst('*')
            index1,index2 = search_listinlist(array(nlst),array(self.datestr))
            if index1.shape[0] <1:
                print 'AirNow does not have hourly data at this time.  Please try again'
            else:
                self.ftp_to_pandas(nlst[index1])
        else:
            print index1
            self.ftp_to_pandas(array(nlst)[index1])


    def ftp_to_pandas(self,flist):
        first =True
        for i in flist:
            fname = 'ftp://'+self.username+':'+self.password+'@'+self.url+self.ftp.pwd()+i
            dft = pd.read_csv(fname,delimiter='|',header=None,infer_datetime_format=True)
            cols = ['date','time','SCS','Site','utcoffset','Species','Units','Obs','Source']
            dft.columns=cols
            if first:
                self.output = dft.copy()
            else:
                self.output = self.output.append(dft)

    def write_to_hdf(self,filename='testoutput.hdf'):
        self.output.to_hdf(fname,'df',format='table')
