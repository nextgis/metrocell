__author__ = 'Alex'

import pandas as pd
import numpy as np
import paths
from utils import Utils
class Preproc():
    def __init__(self,df,users = []):
        self.users = users
        self.df = df
        self.netOperators = {'25099':'Beeline','25001':'MTS','25002':'MegaPhone'}
        self.gens = ['4G','3G','2G']
        return
    def proc_cell_df(self):
        """
        :param dfPath: 'path to georeferenced -csv file'
        :param users: list of users with 'bad phones'
        :return: the dataframe from which was removed rows with:
            1)"NA" LAC or CID
            2) Users who have 'bad phones'
            ...and was added columns:
            1) 'LAC-CID'
            2) 'segment'
        """
        #create new column 'laccid' to identify cell-station referencing
        f = lambda x,y : x[y].astype(str)
        self.df['laccid'] = f(self.df,'LAC') + "-" + f(self.df,'CID')
        #self.df['laccid'] =    self.df['NetworkType'].astype(str) + "-" +\
        #                       self.df['NetworkGen'].astype(str)  + "-" +\
        #                       self.df['LAC'].astype(str)         + "-" +\
        #                       self.df['CID'].astype(str)
        #create new column 'segment' to identify subway-station referencing
        self.df['segment_start_id'] = self.df['segment_start_id'].str.replace("$","")
        self.df['segment_end_id'] = self.df['segment_end_id'].str.replace("$","")
        self.df['segment'] =  self.df['segment_start_id'] + '-' + self.df['segment_end_id']
        #exclude phones with bad parameters
        if len(self.users)!=0:
            for user in self.users:
                self.df = self.df[self.df['User']!= user]
        #exclude cells that have no lac,cid (fields contain "-1") and Power > 0(this means NoSignal)
        self.df = self.df[(self.df['LAC']>0)
                    &(self.df['CID']>0)
                    &(self.df['Power']<0)
                    #&(self.df['Power']>-113)
                    #&(self.df['NetworkType']!='unknown')
                    #&(self.df['NetworkGen']!='unknown')
                    #&(self.df['MCC']!=-1)
                    #&(self.df['MNC']!=-1)
                    #&(self.df['PSC']!=-1)
                      ]
        self.df = self.excludeActiveBag(self.df)

        self.df.drop(['Unnamed: 0'],axis=1,inplace=True)
        self.df['index'] = range(0,self.df.shape[0])
        self.df = self.df.set_index(['index'])

        #lcLens = self.df.groupby(['segment','laccid'])['ratio'].apply(len)
        #lcKeys = lcLens[lcLens>LCdelta].keys()
        #self.df = self.df[self.self.df['laccid'].isin(lcKeys)]


    def excludeActiveBag(self,df):

        f = lambda x,y : x[y].astype(str)
        _df = df.copy()
        _df['ACTIVE'] = f(_df,'MCC') + "-" + f(_df,'MNC') + "-" + f(_df,'LAC')  + "-" +f(_df,'CID')
        activeBag = _df[_df['ACTIVE'] == _df['Active']]
        activeBag.to_csv('C:\\temp\\test\\activeBag.csv')
        _df = _df.drop(activeBag.index)
        _df = _df.drop(['ACTIVE'],axis = 1)
        return _df

    def filterLackofData(self,LCdelta):
        self.df = self.df.groupby(['segment','laccid']).filter(lambda x: len(x)>LCdelta)
    def computeAverTime(self,minTimeSegment,_output,push = False):
        """
        Find a mean path time for each segment.
        :param self.df: input georeferenced dataframe
        :param push: if True push result to CSV
        :return: dataframe with columns : segment | pathTime | stdev
        """
        pathTime = pd.DataFrame()
        # find minimum and maximum times for each segment by each race
        r_b = self.df.groupby(['segment','race_id','User'])['TimeStamp'].max()
        l_b = self.df.groupby(['segment','race_id','User'])['TimeStamp'].min()
        # compute path times for each race
        delta_ts = (r_b-l_b)/1000
        delta_ts = delta_ts[delta_ts>minTimeSegment]
        #delta_ts.groupby(level = ['segment']).apply(lambda x : x[(x<=np.percentile(x,85))&
        #                                                         (x>=np.percentile(x,15))])
        # compute the medians of path times for each segment. We compute medians to reduce blunders effect
        # for ex [30,30,30,60] - "60" doesn't effect on the result. median equal 30,whereas mean could be equal 37.5
        # Note! "level == 0" means that mean function applied to segments,
        # whereas level = 1 - applied to race_id
        pathTime['pathTime'] = delta_ts.groupby(level = 0).apply(np.median)
        # compute standart deviation
        pathTime['stdev'] = delta_ts.groupby(level = 0).apply(np.std)

        pathTime['stdev'] = pathTime['stdev'].apply(lambda x: float("%.2f"%x))
        pathTime['pathTime'] = pathTime['pathTime'].apply(lambda x: float("%.1f"%x))
        trueRaces = delta_ts.groupby(level=1).groups.keys()
        self.df = self.df[self.df['race_id'].isin(trueRaces)]
        if push == True:
            pathTime.to_csv(_output)
        return pathTime

    def splitByNetworkAndSave(self,path,mkey = ''):
        df = self.df
        df['netOperators'] = df.MCC.apply(str) +df.MNC.apply(str).apply(lambda x : x.zfill(2))
        netOperators = df.netOperators.unique()
        for operator in netOperators:
            if operator in self.netOperators.keys():
                fldToSave = path + "\\" + self.netOperators[operator]
                Utils.initOutFld([self.netOperators[operator]],path)
                operatorDf = df[df.netOperators == operator]
                gens = operatorDf.NetworkGen.unique()
                for gen in gens:
                    if gen in self.gens:
                        pathToSave = fldToSave+ "\\" + "pre_log_points" + mkey + '-' + gen +  ".csv"
                        operatorDf_gen =  operatorDf[operatorDf.NetworkGen == gen]
                        operatorDf_gen.to_csv(pathToSave)


if __name__ == "__main__":
    Df = pd.io.parsers.read_csv(paths.preLogPointsPath)
    delta_ts = Preproc.computeAverTime(Df,_output=paths.subwayInfo)
    print delta_ts
