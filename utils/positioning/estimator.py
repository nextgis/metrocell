#coding=utf-8
__author__ = 'Alex'
from geopy.distance import vincenty
#import numpy as np
import pandas as pd
import sys

import paths
from posAlgorithm import PosAlgorithm

class Estimator():
    def __init__(self,iters,algs,pcMethods):
        # iteration limit for each algorithm. After finish find the mean of maximum error.
        self.iters = iters
        self.algs = algs
        # methods of postProcessing (for byPowerCorrelation algorithm only)
        self.pcMethods = pcMethods
    def main(self):
        """
        Initialize algorithms of estimation.
        :param outputs: which algorithms need to execute
        :return: prints several numbers for each algorithm
        """
        StatsDf = pd.DataFrame()
        CorrResultsDf = pd.DataFrame()

        for iter in range(0,self.iters):
            sys.stdout.write("\r" + str(iter+1) + "/" + str(self.iters))
            sys.stdout.flush()
            # initialize a situation
            self.alg = PosAlgorithm()
            for alg in self.algs:
                self.alg.predict(alg = alg)
                predictedDf = self.alg.predictedDf
                # estimate statistics using the algorithm
                if alg != 'pc':
                    statsRow = self.estimateAlgorithm(predictedDf, algname = alg,iter = iter)
                    StatsDf = pd.concat([StatsDf,statsRow])
                else:
                    for method in self.pcMethods:
                        algname = alg + "-" + method
                        # process dataFrame
                        processedDf = self.alg.reducePredPowerCorrSamples(pcReducingMethod = method)
                        statsRow = self.estimateAlgorithm(processedDf,algname = algname,iter = iter)
                        StatsDf = pd.concat([StatsDf,statsRow])
                    self.alg.resultsDf['iter'] = iter
                    CorrResultsDf = pd.concat([CorrResultsDf,self.alg.resultsDf])
                # write it into the table

        StatsDf_ix = StatsDf.set_index([range(0,self.iters*(len(self.algs)+len(self.pcMethods)-1))])
        StatsDf_ix.to_csv(paths.estimDf)

        CorrResultsDf.to_csv(paths.CorrResultsDf)
        print StatsDf_ix
        return

    def estimateAlgorithm(self,predictedDf,algname,iter):
        stats = {"alg":algname,"iter":iter}
        # error - distance delta from the truth point
        # predictedSeg contains the number of true and false segments:
        # True segment is the segment that contains user's position
        # False segment is the segment that do not contains user's position
        stats['error'],predictedSeg = Estimator.byLatLong(predictedDf,
                                                          self.alg.truthPoint)

        # Unpredicted segment is the segment that do not have full set of laccids,
        # grabbed by user into this moment. That means, that database is not full.
        stats['unpr'] = self.alg.unpredicted
        #stats['numLC'] = self.alg.LCs
        #stats['corrcoeff'] = self.alg.corrCoeffs
        stats.update(predictedSeg)
        stats = {i:[stats[i]] for i in stats.keys()}
        statsRow = pd.DataFrame.from_dict(stats)
        return statsRow
    @staticmethod
    def byLatLong(predictedDf,truthPoint):
        # error = None
        predictedSegs = {'trueSeg':0,'falseSeg':None}
        predSegs = list(predictedDf['segment'].unique())
        if list(truthPoint['segment'])[0] in predSegs:
            predictedSegs['trueSeg'] = 1
        predictedSegs['falseSeg'] = len(predSegs)- predictedSegs['trueSeg']

        test = pd.DataFrame()
        truth = zip(truthPoint['x'],truthPoint['y'])

        test['xy'] = zip(predictedDf['x'],predictedDf['y'])
        distances = test.apply(lambda df:vincenty(truth,df['xy']).meters,axis = 1)
        error = max(distances)
        try:
            error = float("%.1f"%error)
        except:
            print error,distances
        return error,predictedSegs
if __name__ =="__main__":
    #parameters
    # ,"r":"random"
    #algorithms = {"lc":"by LACCID","lcM":"by LACCID with neighbours","pc":"by Power correlation"}
    algorithms = ['lc','lcM','pc']
    pcMethods = ['union','intersection','maxCorrMinDelta']
    iters = 30
    #instance
    estimator = Estimator(iters = iters,algs = algorithms,pcMethods = pcMethods)
    #initialize estimation algorithms
    estimator.main()