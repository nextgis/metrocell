#coding=utf-8
__author__ = 'Alex'
from geopy.distance import vincenty
#import numpy as np
import pandas as pd
import sys

import paths
from posAlgorithm import PosAlgorithm

class Estimator():
    def __init__(self,iters,algs):
        # iteration limit for each algorithm. After finish find the mean of maximum error.
        self.iters = iters
        self.algs = algs
    def main(self):
        """
        Initialize algorithms of estimation.
        :param outputs: which algorithms need to execute
        :return: prints several numbers for each algorithm
        """
        StatsDf = pd.DataFrame()
        for iter in range(0,self.iters):
            sys.stdout.write("\r" + str(iter) + "/" + str(self.iters))
            sys.stdout.flush()
            # initialize a situation
            self.alg = PosAlgorithm()
            for alg in self.algs.keys():
                self.unpredicted = 0
                # estimate statistics using the algorithm
                statsRow = self.estimateAlgorithm(alg = alg,iter = iter)
                # write it into the table
                StatsDf = pd.concat([StatsDf,statsRow])
        StatsDf_ix = StatsDf.set_index([range(0,self.iters*len(self.algs.keys()))])
        #print("Mean maxima-errors(meters):")
        StatsDf_ix.to_csv(paths.estimDf)
        print StatsDf_ix
        return

    def estimateAlgorithm(self,alg,iter):
        stats = {"alg":alg,"iter":iter}

        self.alg.predict(alg = alg)
        # error - distance delta from the truth point
        # predictedSeg contains the number of true and false segments:
        # True segment is the segment that contains user's position
        # False segment is the segment that do not contains user's position
        stats['error'],predictedSeg = Estimator.byLatLong(self.alg.predicted_df,
                                                 self.alg.truthPoint,
                                                 self.alg.predicted_segments)

        # Unpredicted segment is the segment that do not have full set of laccids,
        # grabbed by user into this moment. That means, that database is not full.
        stats['unpr'] = self.unpredicted
        stats.update(predictedSeg)
        stats = {i:[stats[i]] for i in stats.keys()}
        statsRow = pd.DataFrame.from_dict(stats)
        return statsRow
    @staticmethod
    def byLatLong(predictedDf,truthPoint,truthSegments = None):
        # error = None
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
    # ,"r":"random"
    algorithms = {"lc":"by LACCID","lcM":"by LACCID with neighbours","pc":"by Power correlation"}
    iters = 5
    #instance
    estimator = Estimator(iters = iters,algs = algorithms)
    #initialize estimation algorithms
    estimator.main()