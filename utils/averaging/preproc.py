__author__ = 'Alex'

import pandas as pd
import numpy as np
import utilities,variables
class Preproc():
    def __init__(self,df):
        self.df = df
        self.netOperators = variables.averaged_cell_pars['netOperators']
        self.gens = variables.averaged_cell_pars['gens']
        self.minTimeSegment = variables.averaged_cell_pars['minTime']
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
        g = lambda x,y : x[y].astype(str).zfill(3)
        self.df['laccid'] = f(self.df,'LAC') + "-" + f(self.df,'CID')
        #self.df['laccid'] =    self.df['NetworkType'].astype(str) + "-" +\
        #                       self.df['NetworkGen'].astype(str)  + "-" +\
        #                       self.df['LAC'].astype(str)         + "-" +\
        #                       self.df['CID'].astype(str)
        #create new column 'segment' to identify subway-station referencing
        self.df['segment_id'] =  self.df['id_from'].apply(lambda x:str(x).zfill(3) + '-') + self.df['id_to'].apply(lambda x:str(x).zfill(3))
        #exclude phones with bad parameters
    #    if len(self.users)!=0:
    #        for user in self.users:
    #            self.df = self.df[self.df['User']!= user]
        #exclude cells that have no lac,cid (fields contain "-1") and Power > 0(this means NoSignal)
        self.df = self.df[(self.df['LAC']!='-1')
                    &(self.df['CID']!='-1')
                    #&(self.df['Power']<0)
                    #&(self.df['Power']>-113)
                    #&(self.df['NetworkType']!='unknown')
                    &(self.df['NetworkGen']!='unknown')

                    #&(self.df['MCC']!=-1)
                    #&(self.df['MNC']!=-1)
                    #&(self.df['PSC']!=-1)
                      ]

        self.df = self.excludeActiveBag(self.df)

        self.df.loc[self.df['Active']!='1','MNC'] = self.df[self.df['Active']!='1']['Active'].apply(lambda x : x.split('-')[1])
        self.df.loc[self.df['Active']!='1','MCC'] = self.df[self.df['Active']!='1']['Active'].apply(lambda x : x.split('-')[0])

        #lcLens = self.df.groupby(['segment','laccid'])['ratio'].apply(len)
        #lcKeys = lcLens[lcLens>LCdelta].keys()
        #self.df = self.df[self.self.df['laccid'].isin(lcKeys)]


    def excludeActiveBag(self,df):

        f = lambda x,y : x[y].astype(str)
        _df = df.copy()
        _df['ACTIVE'] = f(_df,'MCC') + "-" + f(_df,'MNC') + "-" + f(_df,'LAC')  + "-" +f(_df,'CID')
        activeBag = _df[_df['ACTIVE'] == _df['Active']]
        #activeBag.to_csv(self.activeBagPath)
        _df = _df.drop(activeBag.index)
        _df = _df.drop(['ACTIVE'],axis = 1)
        return _df

    def filterLackofData(self,LCdelta):
        self.df = self.df.groupby(['segment_id','laccid']).filter(lambda x: len(x)>LCdelta)
    def computeAverTime(self):
        """
        Find a mean path time for each segment.
        :param self.df: input georeferenced dataframe
        :param push: if True push result to CSV
        :return: dataframe with columns : segment | pathTime | stdev
        """
        pathTime = pd.DataFrame()
        # find minimum and maximum times for each segment by each race
        r_b = self.df.groupby(['id_from','id_to','race_id','User'])['TimeStamp'].max()
        l_b = self.df.groupby(['id_from','id_to','race_id','User'])['TimeStamp'].min()
        # compute path times for each race
        delta_ts = (r_b-l_b)/1000
        delta_ts = delta_ts[delta_ts>self.minTimeSegment]
        #delta_ts.groupby(level = ['segment']).apply(lambda x : x[(x<=np.percentile(x,85))&
        #                                                         (x>=np.percentile(x,15))])
        # compute the medians of path times for each segment. We compute medians to reduce blunders effect
        # for ex [30,30,30,60] - "60" doesn't effect on the result. median equal 30,whereas mean could be equal 37.5
        # Note! "level == 0" means that mean function applied to segments,
        # whereas level = 1 - applied to race_id
        #delta_ts = delta_ts.reset_index()
        pathTime['time_median'] = delta_ts.groupby(level=[0,1]).apply(np.median)
        # compute standart deviation
        pathTime['time_stdev'] = delta_ts.groupby(level=[0,1]).apply(np.std)
        pathTime = pathTime.reset_index()
        pathTime['segment_id'] = pathTime['id_from'].apply(lambda x:str(x).zfill(3) + '-') + pathTime['id_to'].apply(lambda x:str(x).zfill(3))
        pathTime = utilities.dropMultipleCols(pathTime,['id_from','id_to'])
        #pathTime['time_stdev'] = pathTime['time_stdev'].apply(lambda x: float("%.2f"%x))
        #pathTime['time_median'] = pathTime['time_median'].apply(lambda x: float("%.1f"%x))
        trueRaces = delta_ts.groupby(level=2).groups.keys()
        self.df = self.df[self.df['race_id'].isin(trueRaces)]

        #if push == True:
        #    pathTime.to_csv(_output)
        return pathTime

    #def splitByNetworkAndSave(self,path,mkey = ''):
    #    df = self.df.copy()
    #    df['netOperators'] = df.MCC.apply(str) +df.MNC.apply(str).apply(lambda x : x.zfill(2))
    #    netOperators = df.netOperators.unique()
    #    for operator in netOperators:
    #        if operator in self.netOperators.keys():
    #            fldToSave = path + "\\" + self.netOperators[operator]
    #            Utils.initOutFld([self.netOperators[operator]],path)
    #            operatorDf = df[df.netOperators == operator]
    #            gens = operatorDf.NetworkGen.unique()
    #            for gen in gens:
    #                if gen in self.gens:
    #                    pathToSave = fldToSave+ "\\" + "pre_log_points" + mkey + '-' + gen +  ".csv"
    #                    operatorDf_gen =  operatorDf[operatorDf.NetworkGen == gen]
    #                    operatorDf_gen.to_csv(pathToSave)

    def exclude_constant_signals(self):
        """
        avoid situations when signal is constant on exact segment in exact race for exact user
        :return:
        """
        grouped = self.df.groupby(['NetworkGen','MNC','MCC','race_id','User'])
        self.df = grouped.filter(lambda x: np.std(x['Power'])>variables.averaged_cell_pars['constantstd'])
        #self.df = grouped.filter(lambda x: len(np.unique(x['Power']))>1)
        return self.df
if __name__ == "__main__":
    pass