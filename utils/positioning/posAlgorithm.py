__author__ = 'Alex'

import pandas as pd
import numpy as np
from scipy.signal import argrelextrema

import paths
import random
import itertools
    
from utils import Utils

class PosAlgorithm():
    def __init__(self,range = 15,
                 timeStep = 1,
                 numLC = 4,
                 minCorrcoeff = 0.4,
                 corrDelta = 0.3,
                 absDelta = 2,
                 LCshape = 6,
                 windowStep = 1,
                 numRaces = 3,
                 minVariance = 0.3):
        self.ut = Utils()
        # original database
        self.SmoothedDf = pd.io.parsers.read_csv(paths.saveCellSmoothed)
        # source for section imitation
        self.ImitDb = pd.io.parsers.read_csv(paths.saveCellSmoothed)
        # output database contained predicted points
        self.predicted_df = None
        # by default the number of unpredicted segments is 0
        self.unpredicted = 0
        # a list of grabbed laccids
        self.LCs = None
        # the dictionary of coefficients of correlation
        self.corrCoeffs = {}
        # range of indexes at the test data frame
        self.Range = None
        # range between indexes of grabbed section.
        # Other words it is just about the time of user's waiting in seconds
        self.range = range
        # the time step as constant step between rows at the database
        self.timeStep = timeStep
        # The number of laccids,grabbed by user. For "byLacCidMod" algorithm it must be more then 2.
        # Otherwise, it will works as "byLacCid" algorithm
        self.numLC = numLC
        # minimum of the correlation coefficient (only for byPowerCorr algorithm)
        self.minCorrcoeff = minCorrcoeff
        # delta error of correlation coefficient (only for byPowerCorr algorithm)
        self.corrDelta = corrDelta
        # delta error of range between means of Powers (only for byPowerCorr algorithm)
        self.absDelta = absDelta
        # size of data set for grabbed data frame grouped by laccid
        self.LCshape = LCshape
        # step over which the coefficient of correlation will be computed (only for byPowerCorr algorithm)
        self.windowStep = windowStep

        self.numRaces = numRaces
        # the minimum variance of Powers in the grabbed dataFrame for each cell(only for byPowerCorr algorithm)
        self.minVariance = minVariance
        # initialize self.segments variable
        self.generateRandomSegment()
        self.grabbedDf = self.getTestSection()

        self.truthPoint = self.randomSampling(self.grabbedDf,numsamples = 1)
        self.trueSegment = self.truthPoint['segment'].unique().all()

    def generateRandomSegment(self):
        """
        Generate segment where user is located.
        :return:
        """

        # additional criterias
        """
        uniqueLc = self.ImitDb.groupby(['segment'])['laccid'].unique()
        # get rows with number of laccids more than ...
        byLc = uniqueLc[uniqueLc.apply(len)>=self.numLC]
        segments = list(byLc.keys())
        # get rows with number of races more than ...
        byRaces = self.ImitDb.groupby('segment')['race_id'].unique().apply(len)
        segments2 = list(byRaces[byRaces>self.numRaces].keys())
        # find the intersection of founded sets
        Segments = set(segments).intersection(segments2)
        """
        # generate test segment
        # simple random
        segLens = self.ImitDb.groupby(['segment']).apply(len)
        self.randSeg = segLens[segLens>self.range].sample(1).keys()
        #print self.randSeg
    def getTestSection(self):
        """
        Get the dataframe grabbed by user.
        :return:
        """
        df = self.ImitDb [self.ImitDb['segment'].isin(self.randSeg)]

        # generate test slice
        i = random.sample(df.index[:-self.range],1)[0]
        df = df.loc[i:i+self.range]
        """
        # extract laccid only if grabbed dataset for this laccid more than default size
        d = df.groupby(['laccid']).apply(len).to_dict()
        LCs = [lc for lc in d.keys() if d[lc]>self.LCshape]
        df = df[df['laccid'].isin(LCs)]
        self.LCs = len(LCs)
        """
        return df
    def predict(self,alg):
        """
        initialize the algorithm of postiioning prediction.
        :param alg: keyword for algoruthm
        :return:
        """
        self.corrCoeffs = {}
        if alg == "r":
            self.randomSampling(self.SmoothedDf)
        if alg == "lc":
            self.byLacCid()
        if alg == "lcM":
            self.byLacCidMod()
        if alg == "pc":
            self.byPowerCorr(useSmoothed = True)
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
        if self.predicted_df[self.predicted_df['segment'].isin(self.truthPoint['segment'].unique())].empty == True:
            self.unpredicted = 1
            print self.truthPoint
    def byLacCidMod(self):
        """
        Use the information from neighbours laccids.
        :return:
        """
        #Note! Attach probability according  length of founded laccids for each step.
        # For example,probability for sublist with length 4 more than siblist with length 2,
        # because this means that in the first case 4 cell's stations were founded correctly, when
        # in the second case only 2. But it might be lack of the data in origin database.
        predicted_segments =[]
        # get predicted frame and segments according base laccid algorithm
        #self.byLacCid()
        self.unpredicted = 0
        # iterate by laccids at grabbed list of laccids.
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
            if predicted_segments!=[]:
                break
        # if something founded - reduce the selection of predicted segments.
        if predicted_segments!=[]:
            self.predicted_df = self.predicted_df[self.predicted_df['segment'].isin(predicted_segments)]
        # if no segments - use the segments from base algorithm.
        else:
            self.unpredicted = 1
    def byPowerCorr(self,
                    useSmoothed = False):
        """
        The input segment should contains varying of signal. Only in this case
        Suppose that user's telephone grabbed not only the base station but neighbours too
        it is possible to identify truth position
        :return: predicted data frame.
        """
        self.resultsDf = pd.DataFrame()
        self.unpredicted = 0
        predictedDf = pd.DataFrame()
        resIndex = 0
        # predicted slices of indexes
        Slices = []

        # 1. Split phone data on base step's sections.

        if useSmoothed  ==True:
            self.interpPowers = self.grabbedDf.groupby(['laccid'])['Power'].apply(list).to_dict()
        else:
            self.interpolateByTimeStep()
        # 2. Compare powers of grabbed log with powers from database

        # a) If the variance of grabbed log close to zero --> compare Mean by list of absolute Power values.
        # b) Else --> compare the coefficients of correlation
        #       If corrCoeff < 0 : extract this indexes from predicted dataFrame
        #       If corrCoeff > 0 : find local maximums at the list of corrCoeffs and
        #                          extract all of the others from predicted dataFrame

        absMeans = self.analyzeLC()
        # Extract indexes iteratively
        powersDf = self.predicted_df.groupby(['segment','laccid'])
        for ((seg,lc),SegLcGroup) in powersDf:
            predictedIndexes = {'byAbs':[],'byCorr':[]}
            i = 0
            # window wide
            ww = len(self.interpPowers[lc])
            while (i+ww)<=SegLcGroup.shape[0]:
                resIndex+=1
                # move along indexes at the database and compare section from it
                # with grabbed section
                keys = absMeans.keys()
                grabSection = np.array(SegLcGroup[i:i+ww]['Power'])

                # Check if the signal is in list of constant signals
                if lc in absMeans.keys():
                    method = 'byAbs'
                    # check the variance
                    # if variance close to zero --> compare absolute Powers
                    lcVariance = np.var(grabSection)

                    if lcVariance< self.minVariance:
                        deltaPower = abs(absMeans[lc] - np.mean(grabSection))
                        predictedIndexes['byAbs'].append(([i,i+ww],deltaPower))
                else:
                    method = 'byCorr'
                    # compute correlation
                    corrcoef = np.corrcoef(grabSection,self.interpPowers[lc])[0,1]
                    if corrcoef > self.minCorrcoeff:
                        predictedIndexes['byCorr'].append(([i,i+ww],corrcoef))

                # write result into the table
                resDict = {'index'          :   resIndex,
                           'trueSegment'    :   self.trueSegment,
                           'predSegment'    :   seg,
                           'lc'             :   lc,
                           'predPowers'     :   grabSection,
                           'truePowers'     :   self.interpPowers[lc],
                           'type'           :   method
                            }
                resRow = pd.DataFrame.from_dict(resDict)
                self.resultsDf = pd.concat([self.resultsDf,resRow])
                # step forward
                i+=self.windowStep
            # extract indexes and append them to the main list
            Slices = Slices + self.getSlices(predictedIndexes['byAbs'],method ='byAbs',type = 'maxLimit')
            Slices = Slices + self.getSlices(predictedIndexes['byCorr'],method ='byCorr',type = 'localMaxima')
        for i in range(0,len(Slices)):
            predictedDf = pd.concat([predictedDf,self.predicted_df[Slices[i][0]:Slices[i][1]]])
        if predictedDf.empty != True:
            # exclude duplicates(rows that are intersected)
            predictedDf = predictedDf.drop_duplicates()
            self.predicted_df = predictedDf
        else:
            self.unpredicted = 1
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

    def analyzeLC(self):
        """
        Compute the means if the variance of grabbed log close to zero
        :return:
        """
        powerVariances = {lc:np.var(self.interpPowers[lc]) for lc in self.interpPowers.keys()}
        splittdLcs = [lc for lc in powerVariances if powerVariances[lc]<self.minVariance]
        absInfo = {lc: np.mean(self.interpPowers[lc]) for lc in splittdLcs}
        print powerVariances,absInfo
        return absInfo
    def getSlices(self,predictedIndexes,method ='byCorr',type = 'localMaxima'):
        slices = []
        values = self.extractUniqueValuesFromSet(predictedIndexes)
        if (values!=[np.nan]) and (values!=[]):
            if method == 'byCorr':
            # branch according to the type of coefficients of correlation extraction
                if type == 'localMaxima':
                    # find local maximums
                    values = [-np.Inf]+values+[-np.Inf]
                    localMaximaIx = argrelextrema(np.array(values),np.greater)[0]
                    localMaximaValues = [values[localMaximaIx[i]] for i in range(0,len(localMaximaIx))]
                    #print 'localMaxima:'
                    #print values
                    #print localMaximaValues
                    # extract indexes by condition
                    slices = [i for (i,val) in predictedIndexes if val in localMaximaValues]
                if type == 'minLimit':
                    # find minimum acceptable level of coefficients
                    maxCoeff = max(values)
                    minCoeff = maxCoeff - self.corrDelta
                    # extract indexes by condition
                    slices = [i for (i,val) in predictedIndexes if val > minCoeff]
            if method == 'byAbs':
                if type == 'maxLimit':
                    minDelta = min(values)
                    maxDelta = minDelta + self.absDelta
                    slices = [i for (i,delta) in predictedIndexes if delta < maxDelta]
                    # print slices
            # add slices
        return slices
    def extractUniqueValuesFromSet(self,_set):
        vals = [p for (l,p) in _set]
        #vals = list(set(vals))
        return vals


if __name__ == "__main__":
    powerCorr = PosAlgorithm()
    powerCorr.byPowerCorr()
