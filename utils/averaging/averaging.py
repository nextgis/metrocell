import paths
import pandas as pd
from utils import Utils
from geojsonC import GeojsonConn
from pointInterpolator import PointInterpolator
from preproc import Preproc
from smooth import Smooth
from dbase import Dbase

class Averaging():
    def __init__(self):
        self.base_step = 200
        self.aver_df = pd.DataFrame()
        self.qualities = {}
        self.dropnames = ['NumRaces','NumUsers','laccid','segment','quality','weight']
        self.unique_names = ['segment','NetworkType','NetworkGen','LAC','CID','laccid','segment_start_id','segment_end_id']
        #instances
        self.ut = Utils()
        self.preproc_ = Preproc()
        self.geojsonConn = GeojsonConn(paths.segments_geojson_path,'CODE')
        self.power = Smooth(unique_names = self.unique_names)
        self.interpolator = PointInterpolator(self.geojsonConn)

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

        MoveDf = self.preproc_.proc_cell_df(MoveDf,users = ['sasfeat'])
        MoveDf.to_csv(paths.preLogPointsPath)
        # 3. Mean pre-processed data
        self.iterateBySegment(MoveDf)
        self.aver_df.to_csv(paths.saveCellSmoothed + "Cells_smoothed_unref-" + str(self.base_step) + ".csv")
        #print (self.qualities)
        #print (self.aver_df)
        # 4. post georeferencing
        self.aver_df['x'] = self.aver_df.apply(lambda x:self.interpolator.interpolate_by_ratio(x['segment'],x['ratio'],0).x,axis = 1)
        self.aver_df['y'] = self.aver_df.apply(lambda x:self.interpolator.interpolate_by_ratio(x['segment'],x['ratio'],0).y,axis = 1)
        self.aver_df.drop(self.dropnames,inplace = True, axis = 1)
        # push smoothed dataframe into the sqlite database
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
        for seg in segments:
            self.qualities[seg] = {}
            seg_df = MoveDf[MoveDf['segment'] == seg]
            segment_length = self.geojsonConn.get_segment_length(seg)
            numOfPts = segment_length/self.base_step + 1
            seg_laccid_list = self.ut.unique(seg_df,'laccid')
            for laccid in seg_laccid_list:
                laccid_df = seg_df[seg_df['laccid']==laccid]
                min_lc_ratio,max_lc_ratio,levels_num = self.ut.getBoundaries(laccid_df,numOfPts)
                if levels_num>5:
                    try:
                        smoothed_laccid_df,quality = self.power.smooth(laccid_df,
                                                                          func = 'Power',
                                                                          min_lc_ratio = min_lc_ratio,
                                                                          max_lc_ratio = max_lc_ratio,
                                                                          levels_num = levels_num,
                                                                          test_size = 0.4
                                                                          )
                    except:
                        print  ("Oops! Cell with type-gen-lac-cid == %s at segment %s"%(laccid,seg))
                        continue

                    self.aver_df = pd.concat([self.aver_df,smoothed_laccid_df],ignore_index = True)
                    self.qualities[seg][laccid] = quality


if __name__ == '__main__':
    averaging = Averaging()
    averaging.main()
