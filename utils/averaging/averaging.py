import paths
import pandas as pd
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
            self.saveCellSmoothed = self.output + "\\Cells_smoothed_ref-200.csv"
            self.moveAlongAxisJnl = self.output + "\\journals\\" + Utils.fromCurrentTime('.csv')


        if self.push:
            self.db_output = self.output + "\\Cells_smoothed_ref-200"
            self.subwayInfoPath = self.output + "\\subwayInfo.csv"

        #self.moveAlongAxisJnl = self.output + "\\" + "moveAlongAxisJnl.csv"
        # meters
        self.base_step = 200
        # seconds
        self.base_step_time = 1
        # the number of neighbours (for kNeighbours method of averaging)
        self.nNeighbours = 50
        # the minimum number of rows for collected LACCID as input parameter (for kNeighbours method of averaging)
        self.minData = 30
        # percent of testing data for kNeighbours classifieer
        self.testsize = 0.4
        # the minimum passed time needed to move from one station to the another.
        # This value is set to except blunders effect appeared at the data collection.
        # By default it is 30 seconds.
        self.minTime = 30
        self.aver_df = pd.DataFrame()
        self.qualities = {}
        # drop from database
        self.dropnames = ['NumRaces','NumUsers','laccid','segment','quality','weight']
        # save after filtration
        self.unique_names = ['segment','NetworkType','NetworkGen','LAC','CID','laccid','segment_start_id','segment_end_id']
        # the minimum number of rows for each collected unique cell(for preprocessing)
        self.Lcdelta = 15 # MUST BE IDENTICAL IN SMOOTH MODULE!
        #instances
        self.ut = Utils()
        self.geojsonConn = GeoFilesConn(paths.segments_geojson_path,'CODE')
        self.filters = Filters(nNeighbours = self.nNeighbours,
                                     unique_names = self.unique_names,
                                     minData = self.minData,
                                      test_size= self.testsize)

        self.interpolator = PointInterpolator(self.geojsonConn)
        self.correlator = SignalCorrelator()
        self.powerAveraging = Smooth(correlator = self.correlator,
                                             filters = self.filters)
        return


    def postGeoref(self):
        # 4. post georeferencing
        self.aver_df['x'] = self.aver_df.apply(lambda x:self.interpolator.interpolate_by_ratio(x['segment'],x['ratio'],0).x,axis = 1)
        self.aver_df['y'] = self.aver_df.apply(lambda x:self.interpolator.interpolate_by_ratio(x['segment'],x['ratio'],0).y,axis = 1)
        if self.output:
            self.aver_df.to_csv(self.saveCellSmoothed,index_label = 'index')

    def pushToDb(self):
        self.aver_df.drop(self.dropnames,inplace = True, axis = 1)
        for id in ['segment_start_id','segment_end_id']:
            self.aver_df[id] = self.aver_df[id].apply(int)
        db = Dbase(paths.output_db,key = "")
        db.connection.text_factory = str
        self.aver_df.to_sql(paths.tabname,con = db.connection)
        db.connection.close()
    def preprocData(self,Df,badUsers):
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
        step = 0
        segments = MoveDf['segment'].unique()
        seg_len = len(segments)
        print("Segments in processing..")
        for seg in segments:
            step+=1
            sys.stdout.write("\r"+(str(step)+"/"+str(seg_len)))
            sys.stdout.flush()
            print " : " + seg
            self.qualities[seg] = {}
            seg_df = MoveDf[MoveDf['segment'] == seg]
            # compute the number of points for each segment
            # full time on the segment / base time step
            pathTime = subwayInfoDf.loc[seg]['pathTime']
            numOfPts = int(pathTime/self.base_step_time)
            self.powerAveraging.segmentTime = pathTime

            for laccid in seg_df['laccid'].unique():
                print laccid
                laccid_df = seg_df[seg_df['laccid']==laccid]
                # get left and right boundaries(min and max) where this signal was detected
                self.filters.min_lc_ratio,self.filters.max_lc_ratio,self.filters.levels = self.ut.getBoundaries(laccid_df,numOfPts)
                #try:


                smoothed_laccid_df = self.powerAveraging.initCombinations(laccid_df)

                #except:
                 #   print  ("Oops! Cell with type-gen-lac-cid == %s at segment %s"%(laccid,seg))
                 #   continue

                self.aver_df = pd.concat([self.aver_df,smoothed_laccid_df],ignore_index = True)
                #self.qualities[seg][laccid] = quality
        self.aver_df.to_csv("C:\\temp\\test\\movedAlongRatio.csv")
        self.powerAveraging.journal.to_csv(self.moveAlongAxisJnl)
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
    test = True
    #test_segments = ['098-099','099-098','100-099','099-100','100-101','101-100','074-075','075-074','074-073','073-074','151-219','219-151','068-069','069-068']
    test_segments = ['151-219','219-151']
        ###must get interactively!###
    Df = pd.io.parsers.read_csv(dbAveraging.input)
    # 2. Pre-processing of input referenced dataframe,according to the phone parameters
    if dbAveraging.preproc:
        Df,subwayInfoDf = dbAveraging.preprocData(Df,badUsers = ['sasfeat'])
    else:
        subwayInfoDf = pd.io.parsers.read_csv(dbAveraging.subwayInfoDf,index_col = 'segment')
    if test:
        Df = Df[Df['segment'].isin(test_segments)]
    # 3. Average pre-processed data
    if dbAveraging.average:
        dbAveraging.iterateBySegment(Df,subwayInfoDf)
        if dbAveraging.georef:
            dbAveraging.postGeoref()
        # 4. push smoothed dataframe into the sqlite database
        if dbAveraging.push:
            dbAveraging.pushToDb()


if __name__ == '__main__':
    main()


