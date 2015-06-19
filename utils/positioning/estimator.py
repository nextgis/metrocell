#coding=utf-8
__author__ = 'Alex'
from geopy.distance import vincenty
#import numpy as np
import pandas as pd
import numpy as np
#import paths
from posAlgorithm import PosAlgorithm

class Estimator():
    def __init__(self,iters):
        # iteration limit for each algorithm. After finish find the mean of maximum error.
        self.iters = iters
    def main(self,outputs):
        """
        Initialize algorithms of estimation.
        :param outputs: which algorithms need to execute
        :return: prints several numbers for each algorithm
        """
        for out in outputs:
            # list of errors.Error - distance delta from the truth point
            self.errors = []
            # the number of true and false segments:
            # True segment is the segment that contains user's position
            # False segment is the segment that do not contains user's position
            self.trueSegs,self.falseSegs = 0,0
            if out == "random":
                self.random()
            if out == "byLacCid":
                self.byLaccid()
            if out == "byLacCidMod":
                # the number of unpredicted segments.
                # Unpredicted segment is the segment that do not have full set of laccids,
                # grabbed by user into this moment. That means, that database is not full.
                self.unpredicted=0
                self.byLacCidMod()
        return
    def random(self):
        """
        The simplest - grab random points
        :return:
        """
        print("Mean maxima-errors(meters):")
        print("--Random--")
        for i in range(1,self.iters):
            randomAlg = PosAlgorithm(type = "lc")
            randomAlg.randomSampling(randomAlg.SmoothedDf)
            #error
            error,predictedSeg= Estimator.byLatLong(randomAlg.predicted_df,
                                                    randomAlg.truthPoint)
            self.errors.append(error)

        print (np.mean(self.errors))
    def byLaccid(self):
        """
        Use Lac and Cid identifiers of Base station only.
        :return:
        """
        for i in range(0,self.iters):
            byLaccid = PosAlgorithm(type = "lc")
            byLaccid.byLacCid()
            #error
            error,predictedSeg = Estimator.byLatLong(byLaccid.predicted_df,
                                                     byLaccid.truthPoint,
                                                     byLaccid.predicted_segments)
            self.errors.append(error)
            self.trueSegs+=predictedSeg['trueSeg']
            self.falseSegs+=predictedSeg['falseSeg']
        print("--By LACCID--")
        print (np.mean(self.errors))
        print ("false segments:" + str(self.falseSegs))
        print ("true segments:" + str(self.trueSegs))
    def byLacCidMod(self):
        """
        Use the information from neighbours laccids.
        :return:
        """

        for i in range(0,self.iters):
            byLaccidMod = PosAlgorithm(type = "lc")
            byLaccidMod.byLacCidMod()
            self.unpredicted += byLaccidMod.unpredicted
            #error
            error,predictedSeg = Estimator.byLatLong(byLaccidMod.predicted_df,
                                                     byLaccidMod.truthPoint,
                                                     byLaccidMod.predicted_segments)
            self.errors.append(error)

            self.trueSegs+=predictedSeg['trueSeg']
            self.falseSegs+=predictedSeg['falseSeg']
        # percent of unpredicted segments
        unpr_per = self.unpredicted/self.iters
        print("--By LACCID Modified--")
        print (np.mean(self.errors))
        print ("Persent of unpredicted segments:")
        print (str(unpr_per)+"%")
        print ("false segments:" + str(self.falseSegs))
        print ("true segments:" + str(self.trueSegs))
    @staticmethod
    def byLatLong(predictedDf,truthPoint,truthSegments = None):
        predictedSegs = {'trueSeg':0,'falseSeg':999999}
        if truthSegments!=None:
            if truthPoint[truthPoint['segment'].isin(truthSegments)].shape[0]==1:
                predictedSegs['trueSeg'] = 1
            predictedSegs['falseSeg'] = len(truthSegments)-1
        test = pd.DataFrame()
        truth = zip(truthPoint['x'],truthPoint['y'])

        test['xy'] = zip(predictedDf['x'],predictedDf['y'])
        distances = test.apply(lambda df:vincenty(truth,df['xy']).meters,axis = 1)
        error = max(distances)
        error = float("%.1f"%error)
        return error,predictedSegs
if __name__ =="__main__":
    #parameters
    outputs = ["random","byLacCid","byLacCidMod"]
    iters = 50
    #instance
    estimator = Estimator(iters = iters)
    #initialize estimation algorithms
    estimator.main(outputs)