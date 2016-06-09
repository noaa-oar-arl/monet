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
        self.datestr = []
        for i in self.dates:
            self.datestr.append(i.strftime('%Y%m%d%H.dat'))

    def retrieve_hourly_files(self,cleanup=True):
        from numpy import array
        self.openftp()
        self.convert_dates_tofnames()
        nlst = self.retrieve_hourly_filelist()
        index1,index2 = search_listinlist(array(nlst),array(self.datestr))
        if index1.shape[0]< 1:
            self.ftp.cwd('Archive')
            year = self.dates[0].strftime('%Y')
            self.ftp.cwd(year)
            nlst = self.ftp.nlst('*')
            index1,index2 = search_listinlist(array(nlst),array(self.datestr))
            if index1.shape[0] <1:
                print 'The dates you have entered are not found in the AirNow FTP Server'
                print 'Please enter valid dates'
            else:
                self.download_rawfiles(array(nlst)[index1])
                self.ftp_to_pandas(array(nlst)[index1])
        else:
            self.download_rawfiles(array(nlst)[index1])
            self.ftp_to_pandas(array(nlst)[index1])
        self.ftp.close()

        if cleanup:
            self.cleanup_directory(array(nlst)[index1])


    def download_single_rawfile(self,fname):
        localfile = open(fname,'wb')
        print 'Retriving file: ' + self.url +self.ftp.pwd() + '/' + fname
        self.ftp.retrbinary('RETR ' + fname,localfile.write,1024)
        localfile.close()


    def download_rawfiles(self,flist):
        if flist.shape[0] <2:
            self.download_single_rawfile(flist[0])
        else:
            for i in flist:
                self.download_single_rawfile(i)
    
    def cleanup_directory(self,flist):
        import os
        for i in flist:
            path = os.path.join(os.getcwd(),i)
            os.remove(path)

    def ftp_to_pandas(self,flist):
        first =True
        for i in flist:
            dft = pd.read_csv(i,delimiter='|',header=None,infer_datetime_format=True)
            cols = ['date','time','SCS','Site','utcoffset','Species','Units','Obs','Source']
            dft.columns=cols
            if first:
                self.output = dft.copy()
                first = False
            else:
                self.output = pd.concat([self.output,dft])

    def write_to_hdf(self,filename='testoutput.hdf'):
        self.output.to_hdf(fname,'df',format='table')

    def write_to_csv(self,filename='testoutput.csv'):
        self.output.to_csv(fname)
