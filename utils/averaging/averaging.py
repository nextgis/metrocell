__author__ = 'Alex'

import paths
import pandas as pd
import numpy as np
import sys
from utils import Utils
from geoFilesConn import GeoFilesConn
from pointInterpolator import PointInterpolator
from preproc import Preproc
from smooth import Smooth
from dbase import Dbase
from argparse import ArgumentParser
from posAlgorithm import SignalCorrelator
from filters import Filters
class Averaging():
    def __init__(self,preproc,average,georef,pushToDb,subwayInfoDf,mkey,_input,_output):
        self.preproc = preproc
        if not self.preproc:
            self.subwayInfoDf = subwayInfoDf
        self.average = average
        self.georef = georef
        self.push = pushToDb
        self.mkey = mkey
        self.input = _input
        self.output = _output
        if self.output:
            self.preLogPointsPath = self.output + "\\pre_log_points.csv"
            self.saveCellSmoothed = self.output + "\\Cells_smoothed_ref-200-sm2.csv"
            self.testDfPath = self.output + "\\testSets.csv"
            self.jpath = self.output + "\\journals\\"
            self.segInfoPath = self.output + "\\segmentsStepsDf.csv"
        if self.push:
            self.db_output = self.output + "\\Cells_smoothed_ref-200"
            self.subwayInfoPath = self.output + "\\subwayInfo.csv"

        self.filters = Filters()
        # save after filtration
        self.filters.unique_names = ['segment','NetworkType','NetworkGen','LAC','CID','laccid','segment_start_id','segment_end_id']
        # the number of neighbours (for kNeighbours method of averaging)
        self.filters.nNeighbours = 50
        # the minimum number of rows for collected LACCID as input parameter (for kNeighbours method of averaging)
        self.minData = 30
        # percent of testing data for kNeighbours classifieer
        self.filters.testsize = 0.4
        # the minimum passed time needed to move from one station to the another.
        # This value is set to except blunders effect appeared at the data collection.
        # By default it is 30 seconds.
        self.minTime = 30
        self.aver_df = pd.DataFrame()
        # drop this names from database
        self.dropnames = ['NumRaces','NumUsers','laccid','segment','quality','weight']
        # the minimum number of rows for each collected unique cell(for preprocessing)
        self.Lcdelta = 15 # MUST BE IDENTICAL IN SMOOTH MODULE!
        #instances
        self.ut = Utils()
        self.geojsonConn = GeoFilesConn(paths.segments_geojson_path,'CODE')

        self.interpolator = PointInterpolator(self.geojsonConn)
        self.correlator = SignalCorrelator()
        self.powerAveraging = Smooth()
        self.powerAveraging.correlator =self.correlator
        self.powerAveraging.filters = self.filters
        return
    def postGeoref(self):
        # 4. post georeferencing
        self.aver_df['x'] = self.aver_df.apply(lambda x:self.interpolator.interpolate_by_ratio(x['segment'],x['ratio'],0).x,axis = 1)
        self.aver_df['y'] = self.aver_df.apply(lambda x:self.interpolator.interpolate_by_ratio(x['segment'],x['ratio'],0).y,axis = 1)
        self.aver_df = self.aver_df.sort(['segment','ratio','laccid'])
        self.aver_df['index'] = range(0,len(self.aver_df))
        self.aver_df = self.aver_df.set_index(['index'])
        if self.output:
            self.aver_df.to_csv(self.saveCellSmoothed,index_label = 'index')
            for jname in self.powerAveraging.journal.keys():
                fpath = self.jpath  + Utils.fromCurrentTime(jname,'.csv')
                self.powerAveraging.journal[jname].to_csv(fpath)
            self.segmentsStepsDf.to_csv(self.segInfoPath,index_label='index')

    def pushToDb(self):
        """
        Pushing data to database
        :return:
        """
        self.aver_df.drop(self.dropnames,inplace = True, axis = 1)
        for id in ['segment_start_id','segment_end_id']:
            self.aver_df[id] = self.aver_df[id].apply(int)
        db = Dbase(paths.output_db,key = "")
        db.connection.text_factory = str
        self.aver_df.to_sql(paths.tabname,con = db.connection)
        db.connection.close()
    def preprocData(self,Df,badUsers):
        """
        Preprocessing of input dataFrame
        :param Df:
        :param badUsers:
        :return:
        """
        if self.mkey!= '':
            self.mkey = "-" + self.mkey
        self.preprocDf = Preproc(df = Df,
                                   users = badUsers)
        # process move-data
        self.preprocDf.proc_cell_df()
        self.subwayInfoDf = self.preprocDf.computeAverTime(minTimeSegment = self.minTime,
                                                           _output = self.subwayInfoPath,
                                                            push = self.push)
        self.preprocDf.filterLackofData(self.Lcdelta)
        # split data by Operators
        self.preprocDf.splitByNetworkAndSave(self.output,mkey = self.mkey)
        # save results
        if self.push:
            self.preprocDf.df.to_csv(self.output+ "\\"  + "pre_log_points "+ self.mkey + ".csv")
        return self.preprocDf.df,self.subwayInfoDf
    def iterateBySegment(self,MoveDf,subwayInfoDf):
        """
        Iterate by segments and bind the result to the one dataframe.
        :param MoveDf: input dataframe
        :return:
        """
        self.segmentsStepsDf = pd.DataFrame()
        networkErrors = pd.DataFrame()
        step = 0
        segments = MoveDf['segment'].unique()
        seg_len = len(segments)
        print("Segments in processing..")
        #loop through the segments
        for seg in segments:
            step+=1
            sys.stdout.write("\r"+(str(step)+"/"+str(seg_len)))
            sys.stdout.flush()
            print " : " + seg
            seg_df = MoveDf[MoveDf['segment'] == seg]
            pathTime = subwayInfoDf.loc[seg]['pathTime']
            # split data to equal time step(cut on one second steps)
            time_Df,interpStep = self.splitByTimeStep(pathTime)
            # write ratio length to the DataFrame
            segRow = pd.DataFrame({'segment':[seg],'interpStep':interpStep,'index':[step]})
            self.segmentsStepsDf = pd.concat([self.segmentsStepsDf,segRow])

            self.filters.time_Df = time_Df
            self.powerAveraging.segmentTime = pathTime

            for laccid in seg_df['laccid'].unique():
                # loop through laccids
                laccid_df = seg_df[seg_df['laccid']==laccid]
                # check if frame contains error-rows(for ex. points which contains lac and cid from the next cell, but MNC from the last cell)
                errorRows,laccid_df = self.powerAveraging.splitFrameByMinLen(laccid_df,by = 'NetworkType')
                if laccid_df.shape[0]>self.minData:
                    # filter all parts of data with a few datasets.
                    laccid_df_time = self.distToTimeRatio(laccid_df,time_Df)
                    self.powerAveraging.interpStep = interpStep
                    # initialization of smoothing algorithm
                    smoothed,fewData = self.powerAveraging.initCombinations(laccid_df_time,combinations='RU')
                    self.aver_df = pd.concat([self.aver_df,smoothed],ignore_index = True)
                networkErrors = pd.concat([networkErrors,errorRows])
        print networkErrors
    def processTestDf(self,df,subwayInfoDf):
        """
        Processing of input test frame.loop through the segments and interpolate each of them by equal steps.
        :param df: testing dataframe {pd.DataFrame}
        :param subwayInfoDf: dataframe contains info about passed time for each segment
        :return:
        """
        Interpolated = pd.DataFrame()
        segments = df.segment.unique()
        for seg in segments:
            segDf = df[df.segment == seg]
            pathTime = subwayInfoDf.loc[seg]['pathTime']
            time_Df,interpStep = self.splitByTimeStep(pathTime)
            grouped = segDf.groupby('laccid')
            for ix,gr in grouped:
                _gr = self.distToTimeRatio(gr,time_Df,toTime=True)
                Interpolated = pd.concat([Interpolated,_gr])
        return Interpolated

    def distToTimeRatio(self,distDf,timeDf,toTime = False):
        """
        Translate distance ratios to the time ratios
        :param distDf: origin df {pd.DataFrame}
        :param timeDf: end df splitted by ratios {pd.DataFrame}
        :param toTime: if true - append "TimeStamp" column to imitate real situation. {boolean,default False}
        :return: translated Df {pd.DataFrame}
        """
        LCDF = pd.DataFrame()
        grouped = distDf.groupby(['race_id','User'])
        for ix,gr in grouped:
            newDf = pd.DataFrame()
            boundaries = np.searchsorted(timeDf['ratio'],gr.loc[[gr.index[0],gr.index[-1]],'ratio'])
            _grDf = timeDf[boundaries[0]:boundaries[-1]]
            if not _grDf.empty:
                ixs = np.searchsorted(gr['ratio'],_grDf['ratio'])
                ixs = gr.iloc[ixs].index
                for i in range(0,len(ixs)):
                    row = gr.loc[ixs[i]:ixs[i]]
                    _row = row.copy()
                    _row.loc[row.index[0],'ratio'] = _grDf.loc[_grDf.index[i],'ratio']
                    if toTime:
                        _row.loc[row.index[0],'TimeStamp'] = _grDf.loc[_grDf.index[i],'time']
                    newDf = pd.concat([newDf,_row])
                newDf['Power'] = np.interp(newDf['ratio'],gr['ratio'],gr['Power'])
                LCDF = pd.concat([LCDF,newDf])
        return LCDF
    def splitByTimeStep(self,pathTime,step = 1):
        """
        Split dataframe by equal time steps
        :param pathTime: mean time passed on a segment {float}
        :param step: length of the step (default 1 second) {int}
        :return: _segDf: dataFrame splitted on equal segments
             interpStep: interpolation step
        """
        nSteps = int(round(pathTime/step))+1
        timeRatio = np.linspace(0,1,nSteps)
        _segDf = pd.DataFrame({'ratio':timeRatio})
        interpStep = _segDf.loc[_segDf.index[1],'ratio'] - _segDf.loc[_segDf.index[0],'ratio']
        times = np.linspace(0,len(_segDf)-1,len(_segDf))
        _segDf.loc[:,'time'] = times
        return _segDf,interpStep
    def dbTestSplitter(self,df):
        """
        random choose and split data on main and test DF.
        :param df: origin dataframe
        :return: MainDf: dataframe will be bushed to smoothing algorithm
                 TestDf: testing parts
        """
        MainDf = pd.DataFrame()
        TestDf = pd.DataFrame()
        grouped = df.groupby(['segment'])
        for seg,gr in grouped:
            _gr = gr.copy()
            races = _gr.race_id.unique()
            testRace = np.random.choice(races,1)[0]
            testGroup = _gr[_gr.race_id == testRace]
            users = testGroup.User.unique()
            if len(list(users) + list(races)) >2:
                testUser = np.random.choice(users,1)[0]
                testFrame = testGroup[testGroup.User == testUser]
                mainFrame = _gr.drop(testFrame.index)
                MainDf = pd.concat([MainDf,mainFrame])
                TestDf = pd.concat([TestDf,testFrame])
        return MainDf,TestDf
def main():
    parser = ArgumentParser()
    parser.add_argument('-p', '--preproc',action = 'store_true',help = 'Preprocess referenced data')
    parser.add_argument('-a', '--average',action = 'store_true',help = 'Average referenced data')
    parser.add_argument('-g', '--georef',action = 'store_true',help = 'Post-georeferencing of input data')
    parser.add_argument('-d', '--pushToDb',action = 'store_true',help = 'push result to the database')
    parser.add_argument('-m', '--mkey', type = str,default="",help = "Mark key. For example key for stops-data is 'stop',whereas for"
                                                        "interchanges-data is 'inter'. That keyword will be passed to the output name")
    parser.add_argument('-s', '--subwayInfoDf', type = str,help = 'DataFrame containing subway information')
    parser.add_argument('-o','--output',type = str,help = 'Path to the DIR to write out processed data')
    parser.add_argument('input',type = str, help = 'Path to the referenced data')

    args = parser.parse_args()
    dbAveraging = Averaging(preproc = args.preproc,
          average = args.average,
          georef = args.georef,
          pushToDb = args.pushToDb,
          subwayInfoDf = args.subwayInfoDf,
          mkey = args.mkey,
          _input = args.input,
          _output = args.output)

    test = False
    dbTest = True

    test_segments = ['098-099','099-098','100-099','099-100','100-101','101-100','074-075','075-074','074-073','073-074','151-219','219-151','068-069','069-068','143-144']
    #test_segments = ['151-219','219-151']
    #test_segments = ['099-100']
        ###must get interactively!###
    Df = pd.io.parsers.read_csv(dbAveraging.input,index_col = 'index')
    # 2. Pre-processing of input referenced dataframe,according to the phone parameters
    if dbAveraging.preproc:
        Df,subwayInfoDf = dbAveraging.preprocData(Df,badUsers = ['sasfeat'])
    else:
        subwayInfoDf = pd.io.parsers.read_csv(dbAveraging.subwayInfoDf,index_col = 'segment')
    if test:
        Df = Df[Df['segment'].isin(test_segments)]
    # 3. Average pre-processed data
    if dbAveraging.average:
        if dbTest:
            Df,testDf = dbAveraging.dbTestSplitter(Df)
            testDf = dbAveraging.processTestDf(testDf,subwayInfoDf)
            testDf.to_csv(dbAveraging.testDfPath)
        dbAveraging.iterateBySegment(Df,subwayInfoDf)
        if dbAveraging.georef:
            dbAveraging.postGeoref()
        # 4. push smoothed dataframe into the sqlite database
        if dbAveraging.push:
            dbAveraging.pushToDb()
if __name__ == '__main__':
    main()


