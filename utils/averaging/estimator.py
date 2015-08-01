#coding=utf-8
__author__ = 'Alex'

from geopy.distance import vincenty
import pandas as pd
import sys
from posAlgorithm import PosAlgorithm,SignalCorrelator
from argparse import ArgumentParser
class Estimator():
    def __init__(self,testDfPath,mainDfPath,segmentsStepsDf,iters,algs,
                 pcMethods,dbType,console,output,spread):

        self.testDfPath,self.mainDfPath,self.segmentsStepsDf = testDfPath,mainDfPath,segmentsStepsDf
        # iteration limit for each algorithm. After finish find the mean from maximum errors.
        self.iters = iters
        self.algs = algs
        self.spread = spread
        self.output = output
        self.console = console
        # methods of postProcessing (for byPowerCorrelation algorithm only)
        self.pcMethods = pcMethods
        self.dbType = dbType
        self.powerCorrelator = SignalCorrelator(minCorrcoeff = 0.4,
                                                    corrDelta = 0.3,
                                                    absDelta = 0.3,
                                                    windowStep = 1,
                                                    minVariance = 0.3)
        self.statsDfPath = self.output + "\\estimDf.csv"
        self.predictedDfPath = self.output + "\\PredictedDf_PowCorrMethod.csv"
        self.corrDfPath = self.output + "\\corrResults.csv"
        self.analyzedDfPath = self.output + "\\analyzedDf.csv"
        # let's go!
        self.iterate()

    def iterate(self):
        """
        Initialize algorithms of estimation.
        :param outputs: which algorithms need to execute
        :return: prints several numbers for each algorithm
        """
        StatsDf = pd.DataFrame()
        CorrResultsDf = pd.DataFrame()
        PredictedDf = pd.DataFrame()
        AnalyzedDf = pd.DataFrame()
        self.alg = PosAlgorithm(testDf=self.testDfPath,
                                mainDf=self.mainDfPath,
                                segmentsStepsDf= self.segmentsStepsDf,
                                correlator = self.powerCorrelator,
                                spread = self.spread)
        for iter in range(1,self.iters+1):
            sys.stdout.write("\r" + str(iter) + "/" + str(self.iters))
            sys.stdout.flush()
            # initialize a situation
            self.alg.initGrabbedSet()
            for alg in self.algs:
                self.alg.predict(alg = alg,useSmoothed = self.dbType.values()[0])
                predictedDf = self.alg.predictedDf
                analyzedDf = self.alg.analyzedDf
                # statistic estimation of prediction algorithm
                statsRow = self.estimateAlgorithm(predictedDf, algname = alg,iter = iter)
                StatsDf = pd.concat([StatsDf,statsRow])
                if alg == 'pc':
                    # loop by the postProcessing methods
                    if self.alg.unpredicted!=1:
                        predictedDf = self.alg.powerCorrelator.reducePredPowerCorrSamples(predictedDf,by = '')
                        # postProcessing of dataFrame(reducing the number of founded rows)
                        for method in self.pcMethods:
                            algname = alg + "-" + method
                            processedDf = predictedDf[predictedDf[method] == 1]
                            statsRow = self.estimateAlgorithm(processedDf,algname = algname,iter = iter)
                            StatsDf = pd.concat([StatsDf,statsRow])
                        fullPredicted = self.attachControls(self.alg.fullPredicted,predictedDf)
                        fullPredicted.loc[:,'iter'] = analyzedDf.loc[:,'iter'] = iter
                        PredictedDf = pd.concat([PredictedDf, fullPredicted])
                        AnalyzedDf = pd.concat([AnalyzedDf,analyzedDf])
                    self.alg.resultsDf['iter'] = iter
                    CorrResultsDf = pd.concat([CorrResultsDf,self.alg.resultsDf])
        # write statistics into the tables
        StatsDf_ix = StatsDf.set_index([range(0,StatsDf.shape[0])])
        print StatsDf_ix
        if not self.console:
            StatsDf_ix.to_csv(self.statsDfPath)
            PredictedDf.to_csv(self.predictedDfPath)
            AnalyzedDf.to_csv(self.analyzedDfPath)
            #CorrResultsDf.to_csv(self.corrDfPath)
        return
    def attachControls(self,fullPredicted,predictedDf):
        """
        attach control points to predicted DataFrame
        :param fullPredicted: full set of points from observations
        :param predictedDf: predicted points with control fields. One point = last point from predicted observation
        :return: binded frames
        """
        UpdatedDf = pd.DataFrame()
        cols=['controls','maxCorrMinDelta']
        grouped = fullPredicted.groupby(['sliceNumber'])
        for sl,gr in grouped:
            row = predictedDf[predictedDf.sliceNumber == sl]
            _gr = pd.concat([gr,row[cols]],axis = 1)
            _gr[cols] = _gr[cols].fillna(method = 'bfill')
            UpdatedDf = pd.concat([UpdatedDf,_gr])
        return UpdatedDf
    def estimateAlgorithm(self,predictedDf,algname,iter):
        stats = {"alg":algname,"iter":iter}
        # error - distance delta from the truth point
        try:
            stats['error'] = Estimator.byLatLong(predictedDf,self.alg.truthPoint)
        except:
            print
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
def main():
    dbType = {'':True}
    algorithms = ['lc','lcM','pc']
    pcMethods = ['controls','maxCorrMinDelta']
    parser = ArgumentParser()
    parser.add_argument('-a', '--algorithm', choices = algorithms, type = str,help = 'Prediction algorithm.'
                                                                                     'lc : by laccids '
                                                                                     'lcM : laccid modified(with neighbours)'
                                                                                     'pc : power correlation')
    parser.add_argument('-t','--testDf',type = str,required = True,help = 'path to test sets CSV file')
    parser.add_argument('-m','--mainDf',type = str,required = True,help = 'path to CSV file with smoothed signals')
    parser.add_argument('-st','--segmentsStepsDf',type = str,required = True,help = 'Df contains interpolations steps per segment')
    parser.add_argument('-c','--console',action = 'store_true',help = 'writing result into the console or not')
    parser.add_argument('-i','--iterations',type = int,required = True,help = 'number of iterations. one iteration - one imitation of prediction')
    parser.add_argument('-s','--spread',type = int,default = 15,help = "number of rows(seconds) received from the user's phone")
    parser.add_argument('-o','--output',type = str,help = 'DIR to write out output dataFrames')
    args = parser.parse_args()
    if not args.algorithm:
        algorithms = ['lc','lcM','pc']
    else:
        algorithms = args.algorithm
    Estimator( testDfPath= args.testDf,
               mainDfPath = args.mainDf,
               segmentsStepsDf = args.segmentsStepsDf,
               iters = args.iterations,
               algs = algorithms,
               pcMethods = pcMethods,
               dbType = dbType,
               console = args.console,
               output = args.output,
               spread = args.spread
               )


if __name__ =="__main__":
    main()

