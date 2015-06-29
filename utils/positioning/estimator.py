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
        PredictedDf = pd.DataFrame()

        for iter in range(1,self.iters+1):
            sys.stdout.write("\r" + str(iter) + "/" + str(self.iters))
            sys.stdout.flush()
            # initialize a situation
            self.alg = PosAlgorithm()
            for alg in self.algs:
                self.alg.predict(alg = alg)
                predictedDf = self.alg.predictedDf

                # statistic estimation of prediction algorithm
                if alg != 'pc':
                    statsRow = self.estimateAlgorithm(predictedDf, algname = alg,iter = iter)
                    StatsDf = pd.concat([StatsDf,statsRow])
                else:
                    for method in self.pcMethods:
                        # print predictedDf
                        algname = alg + "-" + method
                        # postProcessing of dataFrame. reduce the number of founded rows
                        if self.alg.unpredicted!=1:
                            processedDf = self.alg.reducePredPowerCorrSamples(pcReducingMethod = method)
                            predictedDf = pd.concat([predictedDf,processedDf[method]],axis = 1)
                            predictedDf['iter'] = iter
                            PredictedDf = pd.concat([PredictedDf, predictedDf])
                        else:
                            processedDf = predictedDf
                        statsRow = self.estimateAlgorithm(processedDf,algname = algname,iter = iter)
                        StatsDf = pd.concat([StatsDf,statsRow])
                    self.alg.resultsDf['iter'] = iter
                    CorrResultsDf = pd.concat([CorrResultsDf,self.alg.resultsDf])
                # write it into the table

        #StatsDf_ix = StatsDf.set_index([range(1,self.iters*(len(self.algs)+len(self.pcMethods)-1))])
        StatsDf.to_csv(paths.estimDf)
        PredictedDf.to_csv(paths.PredictedDf)
        CorrResultsDf.to_csv(paths.CorrResultsDf)
        print StatsDf
        return

    def estimateAlgorithm(self,predictedDf,algname,iter):
        stats = {"alg":algname,"iter":iter}
        # error - distance delta from the truth point
        stats['error'] = Estimator.byLatLong(predictedDf,self.alg.truthPoint)
        predictedSeg = Estimator.byFalseTrueSegment(predictedDf,self.alg.truthPoint)
        # Unpredicted segment is the segment that do not have full set of laccids,
        # grabbed by user into this moment. That means, that database is not full.
        stats['unpr'] = self.alg.unpredicted
        stats.update(predictedSeg)
        stats = {i:[stats[i]] for i in stats.keys()}
        statsRow = pd.DataFrame.from_dict(stats)
        return statsRow
    @staticmethod
    def byFalseTrueSegment(predictedDf,truthPoint):
        """
        Estimate errors by the number of false and true segments
        The segment is true if current position of user inside of it
        The segment is false if current position of user outside of it
        :param predictedDf:
        :param truthPoint:
        :return:
        """
        predictedSegs = {'trueSeg':0,'falseSeg':None}
        predSegs = list(predictedDf['segment'].unique())
        if list(truthPoint['segment'])[0] in predSegs:
            predictedSegs['trueSeg'] = 1
        predictedSegs['falseSeg'] = len(predSegs)- predictedSegs['trueSeg']
        return predictedSegs
    @staticmethod
    def byLatLong(predictedDf,truthPoint):
        """
        Estimate errors using point's latitude and longitude.
        :param predictedDf: the DataFrame of predicted Points
        :param truthPoint: Point of user's position
        :return:
        """

        # the set of truth Point XY-coordinates.
        truth = zip(truthPoint['x'],truthPoint['y'])
        # the dataFrame of predicted Points XY-coordinates
        test = pd.DataFrame()
        test['xy'] = zip(predictedDf['x'],predictedDf['y'])
        # compute distances from truth Point to the each predicted Point
        # here used vincenty formula of computation distances for WGS-84 ellipsoid
        distances = test.apply(lambda df:vincenty(truth,df['xy']).meters,axis = 1)
        # the maxima error - is the second Error mark
        error = max(distances)
        error = float("%.1f"%error)
        return error
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