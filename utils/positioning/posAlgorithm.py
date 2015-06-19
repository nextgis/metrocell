__author__ = 'Alex'

import pandas as pd
import paths
import random
from utils import Utils
class PosAlgorithm():
    def __init__(self):
        self.SmoothedDf = pd.io.parsers.read_csv(paths.saveCellSmoothed)
        self.predicted_df = None
        self.grabbedDf = self.randomSampling(self.SmoothedDf,numsamples = 1)
        self.truthPoint = self.randomSampling(self.grabbedDf,numsamples = 1)
        self.ut = Utils()

    def randomSampling(self,df,numsamples = 50):
        rows = random.sample(df.index,numsamples)
        self.predicted_df = df.ix[rows]
        return self.predicted_df
    def byLacCid(self):
        gr_laccids =  self.ut.unique(self.grabbedDf,'laccid')
        self.predicted_df = self.SmoothedDf[self.SmoothedDf['laccid'].isin(gr_laccids)]
        #segments = self.ut.unique(self.predicted_df,'segment')