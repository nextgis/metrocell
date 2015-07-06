
###this code was creating with python 3.4###

import os,sys
from pandas import concat,DataFrame,read_csv,merge
from argparse import ArgumentParser
from numpy import arange,float64
from datetime import datetime
"""
-----------
Before starting make sure that you have set
the "pandas" and "numpy" libraryies
-----------
This code works with nextgis_logger datasets.
Each dataset presents folder with the next filling:
    -cell_time_marks.csv
    -cell_time_log.csv
    -sensor_time_log.csv
    -sensor_time_marks.csv

----------- 
"""
class BreakOnSections():
    def __init__(self,input_dir,output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        # let's go!
        self.break_on_sections()
        return

    def extract_sections(self,Mkey):
        fname = Mkey + "_time_marks.csv"
        marks=read_csv(self.currentDir + "\\" + fname,sep=";",dtype='object')
        secs=[]
        i=0
        stop_move=None
        start_move=None
        while i<len(marks):

            if (self.get_mark(marks['ID'][i])==3):
                localtime=datetime.fromtimestamp(int(int(marks['TimeStamp'][i])/1000)).strftime("%Y%m%d%H%M")
                move_section=marks.iloc[i:i+1]
                id_from=self.get_id(marks['ID'][i])
                stop_section=DataFrame()
                while i<(len(marks)-1):
                    i+=1
                    if (self.get_mark(marks['ID'][i])==2):
                        if Mkey=='cell':
                            if (stop_move==None):
                            #if marks['Active'][i]=='1':
                                stop_move=i
                        if Mkey=='sensor':
                            stop_move=i
                    if (self.get_mark(marks['ID'][i])==3) and (id_from!=self.get_id(marks['ID'][i])):
                        if self.get_id(marks['ID'][i])==self.get_id(marks['ID'][i-1]):
                            j=0
                            while i+j!=(len(marks)-1):
                                j+=1
                                if self.get_mark(marks['ID'][i+j])==4:
                                    if Mkey=='cell':
                                        if (start_move==None):
                                        #if marks['Active'][i+j]=='1':
                                            start_move=i+j
                                    if Mkey=='sensor':
                                        start_move=i+j
                                if self.get_mark(marks['ID'][i+j])==1:
                                    if self.get_id(marks['ID'][i+j])==id_from:
                                        break
                                    else:
                                        stop_section=concat([stop_section,marks.iloc[stop_move:start_move]])
                                        i=int(stop_move)
                                        stop_move=None
                                        start_move=None
                                        break
                        else:
                            i-=1
                        break
                    else:
                        move_section=concat([move_section,marks.iloc[i:i+1]])
                section=dict(move=move_section,stop=stop_section,time=localtime,username = marks['User'][0])
                secs.append(section)
            i+=1
        return secs
    def break_on_sections(self):
        """
        -----------
        file with cell- datasets will be pushed to the "cell" folder,
        while sensor-datasets  to the "sensor" folder.
        -----------
        """
        lOfRawDirs = os.listdir(self.input_dir)
        length = len(lOfRawDirs)
        iter = 0
        for dir in lOfRawDirs:
            sys.stdout.write("\r" + str(dir) + " : " + str(iter) + "/" + str(length))
            sys.stdout.flush()
            self.currentDir =self.input_dir + "\\" + dir
            if not self.output_dir:
                self.output_dir = self.currentDir
            lofSubDirs = os.listdir(self.output_dir)
            if "cell" not in lofSubDirs:
                os.mkdir(self.output_dir+'\\cell')
            if "sensor" not in lofSubDirs:
                os.mkdir(self.output_dir+'\\sensor')
            if "external" not in lofSubDirs:
                os.mkdir(self.output_dir+'\\external')
            if "sensor_time_marks.csv" in self.currentDir:
                sensor_sec=self.extract_sections("sensor")
                self.create_clean_log(sensor_sec,"sensor")
            if "external_time_marks.csv" in self.currentDir:
                sensor_sec=self.extract_sections("external")
                self.create_clean_log(sensor_sec,"external")
            cell_sec=self.extract_sections("cell")
            self.create_clean_log(cell_sec,"cell")

            iter+=1

    def create_clean_log(self,sections,Mkey):
        """
        extracting the log file info by the current section
        parameters:
            -Mkey
                "sensor", "cell" or "external"
            -sections
                extracted sections
        return
            -logs
                dictionary,contained 2 dataframes: the
                first contained "move" part, the second -
                "stop" part
        """

        for sec in sections:
            logs=dict(move=None,stop=None)
            logs['move']=self.arrange_marks(sec['move'],Mkey)
            if sec['stop'].empty==False:
                if sec['stop'].columns[0]=='':
                    logs['stop']=sec['stop']
                else:
                    logs['stop']=self.arrange_marks(sec['stop'],Mkey)

            self.put_in_tidy(logs,Mkey,sec['time'],sec['username'])


    def comma_2_dot(self,x):
        return x.replace(',','.')
    def change_type(self,frame,columns,type):
        for col in columns:
            frame[col]=frame[col].astype(type)
    def arrange_marks(self,section,Mkey):
        if Mkey=='sensor':
            log=read_csv(self.currentDir + "\\" +"sensor_time_log.csv",sep=';',dtype=object)
            section.Accel_X=section.Accel_X.apply(self.comma_2_dot).astype(float64)
            section.Accel_Y=section.Accel_Y.apply(self.comma_2_dot).astype(float64)
            section.Accel_Z=section.Accel_Z.apply(self.comma_2_dot).astype(float64)
        if Mkey=='cell':
            log=read_csv(self.currentDir + "\\" +"cell_time_log.csv",sep=';',dtype=object)

        section.index=arange(0,len(section))
        clean_log=merge(section,log,how='outer')
        if Mkey=='sensor':
            self.change_type(clean_log,['TimeStamp'],'int64')
        if Mkey=='cell':
            self.change_type(clean_log,['TimeStamp','MCC','MNC','LAC','CID','PSC','Power'],'int64')
        self.change_type(section,['TimeStamp'],'int64')
        clean_log=clean_log.sort(columns='TimeStamp')
        clean_log=clean_log[(clean_log.TimeStamp>=section.TimeStamp[0])&(clean_log.TimeStamp<=section.TimeStamp[len(section)-1])]
        clean_log.index=arange(0,len(clean_log))
        if (str(float(clean_log.ID[-1:]))=='nan'):
            clean_log=clean_log[0:-1]
        if (str(float(clean_log.ID[0:1]))=='nan'):
            clean_log=clean_log[1:]
        return clean_log

    def put_in_tidy(self,data_time,Mkey,ltime,username):
        id_from=str(self.get_id(data_time['move']['ID'][0])).zfill(3)
        id_to=str(self.get_id(data_time['move']['ID'][len(data_time['move'])-1])).zfill(3)
        movename=id_from + "-" + id_to +"-"+ ltime+"-"+username+".csv"
        stopname=id_from + "-" + id_to +"-"+ ltime + "-" + username + "-stop"+".csv"
        path=self.output_dir + '\\'+str(Mkey)
        path2move=path+"\\"+movename
        path2stop=path+"\\"+stopname
        data_time['move'].to_csv(path2move,index=False,encoding='utf-8')
        if type(data_time['stop'])==type(data_time['move']):
            data_time['stop'].to_csv(path2stop,index=False,encoding='utf-8')
    def get_mark(self,x):
        return int(str(x)[-1:])
    def get_id(self,x):
        return int(str(x)[0:-1])


def main():
    parser = ArgumentParser(description= "Splitter of the NextGis Logger raw logs")
    parser.add_argument('-o-', '--output_dir', type = str,default = False, help = 'Output splitted log files where one file represents one segment')
    parser.add_argument('input_dir', type = str, help = 'Input unzipped directory containing one or more directories of marks and logs')
    args = parser.parse_args()
    return args

if __name__=="__main__":

    args = main()
    args = BreakOnSections(input_dir = args.input_dir,
                           output_dir = args.output_dir)