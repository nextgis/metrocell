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
        self.predicted_df = None
        # minimum range of grabbed sample
        self.range_min = 10
        # by default the number of unpredicted segments is 0
        self.unpredicted = 0
        # The number of laccids,grabbed by user. For "byLacCidMod" algorithm it must be more then 2.
        # Otherwise, it will works as "byLacCid" algorithm
        self.numLC = 4
        # initialize self.segments variable
        self.generateRandomSegment()
        self.grabbedDf = self.initAlg(type = type)
        self.truthPoint = self.randomSampling(self.grabbedDf,numsamples = 1)
    def initAlg(self,type = "random"):
        """
        initialize the algorithm of positioning.
        :param type: the type of dataframe grabbed by user.
        "lc" - using laccids info.
        "random" - randomly
        :return:
        """
        if type == "random":
            return self.randomSampling(self.SmoothedDf,numsamples = 50)
        if type == "lc":
            return self.getTests()
    def generateRandomSegment(self):
        """
        Generate segment where user located.
        :return:
        """
        uniqueLc = self.SmoothedDf.groupby(['segment'])['laccid'].unique()
        # get rows with number of laccids more than ...
        suits = uniqueLc[uniqueLc.apply(len)>=self.numLC]
        segments = list(suits.keys())
        self.randSeg = random.sample(segments,1)

    def getTests(self):
        """
        Get the dataframe grabbed by user.
        :return:
        """
        df = self.SmoothedDf [self.SmoothedDf['segment'].isin(self.randSeg)]
        lc_list = self.ut.unique(df,'laccid')
        #df = df[df['laccid'].isin(random.sample(lc_list,self.numLC-1))]
        # generate test slice
        while True:
            i = random.sample(df,1)
            j = random.sample(df,1)
            delta = np.abs(i-j)
            if delta > self.range_min:
                range = sorted(sum([i,j],[]))
                df = df.loc[range[0]:range[1]]
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
    def byAbdPowerCorr(self):
        """
        The input segment should contains varying of signal. Only in this case
        it is possible to identify truth position
        :return: predicted data frame.
        """
        # suppose that user's telephone grabbed not only the base station but neighbours too
        self.byLacCidMod()
        # 1. Split phone data on base step's sections.
        # 2. Compute the correlation iteratively