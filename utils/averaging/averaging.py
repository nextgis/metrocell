import paths
import pandas as pd
import sys
from utils import Utils
from geojsonC import GeojsonConn
from pointInterpolator import PointInterpolator
from preproc import Preproc
from smooth import Smooth
from dbase import Dbase

class Averaging():
    def __init__(self):
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
        self.dropnames = ['NumRaces','NumUsers','laccid','segment','quality','weight']
        self.unique_names = ['segment','NetworkType','NetworkGen','LAC','CID','laccid','segment_start_id','segment_end_id']
        # the minimum number of rows for each collected unique cell(for preprocessing)
        self.Lcdelta = 15
        #instances
        self.ut = Utils()
        self.geojsonConn = GeojsonConn(paths.segments_geojson_path,'CODE')
        self.power = Smooth(nNeighbours = self.nNeighbours,
                            unique_names = self.unique_names,
                            minData = self.minData)

        self.interpolator = PointInterpolator(self.geojsonConn)
        self.push = True
        return

    def main(self):
        ###must get interactively!###
        MoveDf = pd.io.parsers.read_csv(paths.inputMovesPath)
        StopDf = pd.io.parsers.read_csv(paths.inputStopsPath)
        """
        #1. check phone parameters
        bad_phones = {'Power':[],'Accel':[],'Gyro':[],'Magn':[]}
        users = list(StopDf['User'].drop_duplicates())
        for user in users:
            userFrame = StopDf[StopDf['User']==user]
            phoneStats = PhonePars.phonePars(userFrame)
            if phoneStats['pulledPars']['Power'] == 0:
                bad_phones['Power'].append(user)
        """
        # 2. Pre-processing of input referenced dataframe,according to the phone parameters
        self.preprocMove = Preproc(df = MoveDf,
                                   users = ['sasfeat'])
        self.preprocStop = Preproc(df = StopDf,
                                   users = ['sasfeat'])
        # process stop-data
        self.preprocStop.proc_cell_df()
        # process move-data
        self.preprocMove.proc_cell_df()
        self.SubwayInfoDf = self.preprocMove.computeAverTime(minTimeSegment = self.minTime,
                                                             push = True)
        self.preprocMove.filterLackofData(self.Lcdelta)
        # save results
        self.preprocMove.df.to_csv(paths.preLogPointsPath)
        self.preprocStop.df.to_csv(paths.preLogPointsPathStop)

        # 3. Mean pre-processed data
        self.iterateBySegment(self.preprocMove.df)

        #print (self.qualities)
        #print (self.aver_df)
        # 4. post georeferencing
        self.aver_df['x'] = self.aver_df.apply(lambda x:self.interpolator.interpolate_by_ratio(x['segment'],x['ratio'],0).x,axis = 1)
        self.aver_df['y'] = self.aver_df.apply(lambda x:self.interpolator.interpolate_by_ratio(x['segment'],x['ratio'],0).y,axis = 1)
        self.aver_df.to_csv(paths.saveCellSmoothed)

        # push smoothed dataframe into the sqlite database
        if self.push ==True :

            self.aver_df.drop(self.dropnames,inplace = True, axis = 1)
            for id in ['segment_start_id','segment_end_id']:
                self.aver_df[id] = self.aver_df[id].apply(int)
            db = Dbase(paths.output_db,key = "")
            db.connection.text_factory = str
            self.aver_df.to_sql(paths.tabname,con = db.connection)
            db.connection.close()

    def iterateBySegment(self,MoveDf):
        """
        Iterate by segments and bind the result to the one dataframe.
        :param MoveDf: input dataframe
        :return:
        """
        segments = self.ut.unique(MoveDf,'segment')
        seg_len = len(segments)
        print("Segments in processing..")
        for seg in segments:
            sys.stdout.write("\r"+(str(segments.index(seg)+1)+"/"+str(seg_len)))
            sys.stdout.flush()
            self.qualities[seg] = {}
            seg_df = MoveDf[MoveDf['segment'] == seg]
            # compute the number of points for each segment

            # the number of points = full length of the segment / by the base step
            # segment_length = self.geojsonConn.get_segment_length(seg)
            # numOfPts = segment_length/self.base_step + 1

            # full time on the segment / base time step
            pathTime = self.SubwayInfoDf.loc[seg]['pathTime']
            numOfPts = int(pathTime/self.base_step_time)

            seg_laccid_list = self.ut.unique(seg_df,'laccid')

            for laccid in seg_laccid_list:

                laccid_df = seg_df[seg_df['laccid']==laccid]
                # get left and right boundaries(min and max) where this signal was detected
                levels = self.ut.getBoundaries(laccid_df,numOfPts)

                try:
                    smoothed_laccid_df,quality = self.power.smooth(laccid_df,
                                                                      func = 'Power',
                                                                      min_lc_ratio = levels['min_lc_ratio'],
                                                                      max_lc_ratio = levels['max_lc_ratio'],
                                                                      levels_num = levels['levels_num'],
                                                                      test_size = self.testsize
                                                                      )
                except:
                    print  ("Oops! Cell with type-gen-lac-cid == %s at segment %s"%(laccid,seg))
                    continue

                self.aver_df = pd.concat([self.aver_df,smoothed_laccid_df],ignore_index = True)
                self.qualities[seg][laccid] = quality


if __name__ == '__main__':
    averaging = Averaging()
    averaging.main()
