__author__ = 'Alex'

import pandas as pd
import numpy as np
import paths
import random
import itertools
    
from utils import Utils

class PosAlgorithm():
    def __init__(self,type = "random"):
        self.ut = Utils()
        # original database
        self.SmoothedDf = pd.io.parsers.read_csv(paths.saveCellSmoothed)
        # source for section imitation
        self.ImitDb = pd.io.parsers.read_csv(paths.preLogPointsPath)
        # output database contained predicted points
        self.predicted_df = None
        # minimum range of grabbed samples
        self.range_min = 5
        # by default the number of unpredicted segments is 0
        self.unpredicted = 0
        # the time step as constant step between rows at the database
        self.timeStep = 1
        # The number of laccids,grabbed by user. For "byLacCidMod" algorithm it must be more then 2.
        # Otherwise, it will works as "byLacCid" algorithm
        self.numLC = 4
        # minimum of the correlation coefficient
        self.corrcoeff = 0.8
        # initialize self.segments variable
        self.generateRandomSegment()
        self.grabbedDf = self.initAlg(type = type)
        self.truthPoint = self.randomSampling(self.grabbedDf,numsamples = 1)
    def initAlg(self,type = "lc"):
        """
        initialize the algorithm of positioning.
        :param type: the type of dataframe grabbed by user.
        "lc" - using laccids info.
        "random" - randomly
        :return:
        """
        if type == "random":
            return self.randomSampling(self.ImitDb,numsamples = 50)
        if type == "lc":
            return self.getTests()
    def generateRandomSegment(self):
        """
        Generate segment where user located.
        :return:
        """
        uniqueLc = self.ImitDb.groupby(['segment'])['laccid'].unique()
        # get rows with number of laccids more than ...
        suits = uniqueLc[uniqueLc.apply(len)>=self.numLC]
        segments = list(suits.keys())
        self.randSeg = random.sample(segments,1)

    def getTests(self):
        """
        Get the dataframe grabbed by user.
        :return:
        """
        df = self.ImitDb [self.ImitDb['segment'].isin(self.randSeg)]
        lc_list = self.ut.unique(df,'laccid')
        #df = df[df['laccid'].isin(random.sample(lc_list,self.numLC-1))]
        # generate test slice
        while True:
            i = random.sample(df.index,1)
            j = random.sample(df.index,1)
            delta = np.abs(i[0]-j[0])
            if delta > self.range_min:
                Range = sorted(sum([i,j],[]))
                df = df.loc[Range[0]:Range[1]]
                break


        return df
    def randomSampling(self,df,numsamples = 50):
        """
        Generate subset from input dataframe.
        :param df: dataframe to analyse
        :param numsamples: the number of samples
        :return:
        """
        rows = random.sample(df.index,numsamples)
        self.predicted_df = df.ix[rows]
        self.predicted_segments = self.ut.unique(self.predicted_df,'segment')
        return self.predicted_df
    def byLacCid(self):
        """
        Use Lac and Cid identifiers of Base station only.
        :return:
        """
        self.grabbed_lc =  self.ut.unique(self.grabbedDf,'laccid')
        self.predicted_df = self.SmoothedDf[self.SmoothedDf['laccid'].isin(self.grabbed_lc)]
        self.predicted_segments = self.ut.unique(self.predicted_df,'segment')
    def byLacCidMod(self):
        """
        Use the information from neighbours laccids.
        :return:
        """
        #Note! Attach probability according  length of founded laccids at each step.
        # For example,probability for sublist with length 4 more than siblist with length 2,
        # because this means that in the first case 4 cell's stations were founded correctly, when
        # in the second case only 2. But it might be lack of the data in origin database.
        predicted_segments =[]
        # get predicted frame and segments according base laccid algorithm
        self.byLacCid()
        # iterate by laccids into grabbed list of laccids.
        for step in range(len(self.grabbed_lc),2,-1):
            # check all combinations
            for sublist in itertools.combinations(self.grabbed_lc,step):
                predicted_subDf = self.predicted_df[self.predicted_df['laccid'].isin(sublist)]
                segments = self.ut.unique(predicted_subDf,'segment')
                # find the right segments for this combination
                for seg in segments:
                    seg_subDf = predicted_subDf[predicted_subDf['segment'] == seg]
                    lc_subList = self.ut.unique(seg_subDf,'laccid')
                    if (set(sublist).issubset(set(lc_subList))) == True:
                        predicted_segments.append(seg)
        # if something founded - reduce the selection of predicted segments.
        if predicted_segments!=[]:
            self.predicted_segments = predicted_segments
            self.predicted_df = self.predicted_df[self.predicted_df['segment'].isin(self.predicted_segments)]
        # if no segments - use the segments from base algorithm.
        else:
            self.unpredicted = 1
    def byPowerCorr(self):
        """
        The input segment should contains varying of signal. Only in this case
        it is possible to identify truth position
        :return: predicted data frame.
        """
        predictedDf = pd.DataFrame()
        predictedIndexes = []
        # suppose that user's telephone grabbed not only the base station but neighbours too

        self.byLacCidMod()
        # 1. Split phone data on base step's sections.
        self.interpolateByTimeStep()
        # 2. Compute the correlation iteratively
        powersDf = self.predicted_df.groupby(['segment','laccid'])
        for ((seg,lc),SegLcGroup) in powersDf:
            i = 0
            # window wide
            ww = len(self.interpPowers[lc])
            while (i+ww)<=SegLcGroup.shape[0]:
                # move along indexes at the database and compare section from it
                # with grabbed section
                trueSection = np.array(SegLcGroup[i:i+ww]['Power'])
                #print len(trueSection),len(self.interpPowers[lc])
                corrcoef = np.corrcoef(trueSection,self.interpPowers[lc])[0,1]
                if abs(corrcoef) > self.corrcoeff:
                    predictedIndexes.append(([i,i+ww],corrcoef))
                i+=1
        slices = [l for (l,coeff) in predictedIndexes]
        #slices = sum(slices,[])
        #slices = sorted(list(set(slices)))
        for i in range(0,len(slices)):
            predictedDf = pd.concat([predictedDf,self.predicted_df[slices[i][0]:slices[i][1]]])
        if predictedDf.empty != True:
            predictedDf = predictedDf.drop_duplicates()
            self.predicted_df = predictedDf
        #print predictedIndexes
        #print (slices)
        #print(self.predicted_df)
            self.predicted_segments = list(self.predicted_df['segment'].unique())
        else:
            self.unpredicted = 1
        #print predictedDf
        #
    def interpolateByTimeStep(self):
        """
        Linear interpolation of grabbed log by the constant.
        :return: the dictionary were key is the LAC-CID
        and value is the array of interpolated powers
        """
        self.interpPowers = {}
        old = self.grabbedDf.groupby(['laccid'])['TimeStamp']\
            .apply(lambda x: list((x -min(x))/1000))

        new = self.grabbedDf.groupby(['laccid'])['TimeStamp']\
            .apply(lambda x: range(0,max(x -min(x))/1000+1,self.timeStep))
        for lc in old.keys():
            self.interpPowers[lc] = np.interp(new[lc],
                                         old[lc],
                                         self.grabbedDf.loc[self.grabbedDf['laccid'] == lc, 'Power'])
if __name__ == "__main__":
    powerCorr = PosAlgorithm(type = "lc")
    powerCorr.byPowerCorr()
