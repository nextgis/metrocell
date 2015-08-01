__author__ = 'Alex'

import pandas as pd
import numpy as np
from scipy.signal import argrelextrema


import random
import itertools

from filters import Filters
class PosAlgorithm():
    def __init__(self,testDf,
                 mainDf,segmentsStepsDf,
                 correlator,
                 spread = 15,
                 timeStep = 1,
                 ):
        # original database
        self.SmoothedDf = pd.io.parsers.read_csv(mainDf,index_col = 'index')
        # source for section imitation
        self.testDf = pd.io.parsers.read_csv(testDf,index_col = 'index')
        self.segmentsStepsDf = pd.io.parsers.read_csv(segmentsStepsDf)
        self.powerCorrelator = correlator
        # output database contained predicted points
        self.predicted_df = None
        # by default the number of unpredicted segments is 0
        self.unpredicted = 0
        # the dictionary of coefficients of correlation
        self.corrCoeffs = {}
        # range of indexes at the test data frame
        # range between indexes of grabbed section.
        # Other words it is just about the time of user's waiting in seconds
        self.spread = spread
        # the time step as constant step between rows at the database
        self.timeStep = timeStep
        # The number of laccids,grabbed by user. For "byLacCidMod" algorithm it must be more then 2.
        # Otherwise, it will works as "byLacCid" algorithm
        # self.numLC = numLC
        self.filters = Filters()

    def initGrabbedSet(self):
        # initialize self.segments variable
        self.generateRandomSegment()
        self.grabbedDf = self.getTestSection()

        #self.truthPoint = self.randomSampling(self.grabbedDf,numsamples = 1)
        self.truthPoint = self.grabbedDf.tail(1)
        self.trueSegment = self.truthPoint['segment'].unique().all()
    def generateRandomSegment(self):
        """
        Generate segment where user is located.
        :return:
        """

        # additional criterias
        """
        uniqueLc = self.testDf.groupby(['segment'])['laccid'].unique()
        # get rows with number of laccids more than ...
        byLc = uniqueLc[uniqueLc.apply(len)>=self.numLC]
        segments = list(byLc.keys())
        # get rows with number of races more than ...
        byRaces = self.testDf.groupby('segment')['race_id'].unique().apply(len)
        segments2 = list(byRaces[byRaces>self.numRaces].keys())
        # find the intersection of founded sets
        Segments = set(segments).intersection(segments2)
        """
        # generate test segment
        # simple random
        segLens = self.testDf.groupby(['segment']).apply(len)
        self.randSeg = segLens[segLens>self.spread].sample(1).keys()
        #print self.randSeg
    def getTestSection(self):
        """
        Get the dataframe grabbed by user.
        :return:
        """
        #self.randSeg = ['074-075']
        df = self.testDf [self.testDf['segment'].isin(self.randSeg)]

        self.analyzedDf = df.copy()
        # filtrate
        self.analyzedDf = self.filters.medianFilter(self.analyzedDf)

        # Note! change 'ratio' to 'TimeStamp' for real situation or remove this sorting!
        #grouped = self.analyzedDf.groupby('ratio').sort('ratio')

        # generate test slice
        #firstStamp = 41.0
        firstStamp = random.sample(self.analyzedDf[self.analyzedDf.TimeStamp < max(self.analyzedDf.TimeStamp) - self.spread].TimeStamp,1)[0]
        print " : " + str(firstStamp)
        self.analyzedDf.loc[:,'grabbed'] = np.nan
        self.analyzedDf = self.analyzedDf.sort('ratio')
        #lastIx = self.analyzedDf[self.analyzedDf.TimeStamp == self.analyzedDf.TimeStamp[firstIx] + self.spread].index
        self.analyzedDf.loc[self.analyzedDf[(self.analyzedDf.TimeStamp>=firstStamp)&(self.analyzedDf.TimeStamp<=firstStamp+self.spread)].index,'grabbed'] = 1
        #self.analyzedDf.loc[i:i+self.spread,'grabbed'] = 1
        self.analyzedDf['grabbed'] = self.analyzedDf['grabbed'].fillna(0)
        grabbed_df = self.analyzedDf[self.analyzedDf['grabbed'] == 1]
        grabbed_df = grabbed_df.sort(['TimeStamp','laccid'])
        #grabbed_df['index'] = range(0,len(grabbed_df))
        return grabbed_df
    def predict(self,alg,useSmoothed):
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
            #self.()
        if alg == "pc":
            self.byPowerCorr(useSmoothed = useSmoothed)


    def randomSampling(self,df,numsamples = 50):
        """
        Generate subset from input dataframe.
        :param df: dataframe to analyse
        :param numsamples: the number of samples
        :return:
        """
        rows = random.sample(df.index,numsamples)
        self.predictedDf = df.ix[rows]
        self.predicted_segments = self.predictedDf['segment'].unique()
        return self.predictedDf
    def byLacCid(self):
        """
        Use Lac and Cid identifiers of Base station only.
        :return:
        """
        self.grabbed_lc =  self.grabbedDf['laccid'].unique()
        self.predictedDf = self.SmoothedDf[self.SmoothedDf['laccid'].isin(self.grabbed_lc)]
        self.predicted_segments = self.predictedDf['segment'].unique()
        if self.predictedDf[self.predictedDf['segment'].isin(self.truthPoint['segment'].unique())].empty == True:
            self.unpredicted = 1
            print self.truthPoint
    def byLacCidMod(self):
        predictedInfo = pd.DataFrame()
        check = True
        laccids = self.grabbedDf.laccid.unique()
        if laccids.__len__()>1:
            actives = self.grabbedDf.Active.unique()
            uniqueLevels = {'before':self.spread,'after':self.spread}
            changedLcs = self.extractChanges()
            if changedLcs:
                predictedInfo = self.findChanges(changedLcs,uniqueLevels)
            else:
                if actives.__len__()>1:
                    predictedInfo = self.findActives(uniqueLevels)
            if not predictedInfo.empty:
                predictedDf = self.reduceByChanges(predictedInfo)
                self.predictedDf = predictedDf.sort(columns = ['segment','ratio','laccid'])
            if predictedInfo.empty:
                self.unpredicted = 1
        else:
            check = False
        return check
    def reduceByChanges(self,predictedInfo):
        predictedDf = pd.DataFrame()
        grouped = self.predictedDf.groupby('segment')
        for seg,gr in grouped:
            segInfo = predictedInfo[predictedInfo.segment == seg]
            for ix,row in segInfo.iterrows():
                #it might be more than one if segment contains several "change points"
                _gr = gr[(gr.ratio>=row['left'])&(gr.ratio<=row['right'])]
                predictedDf = pd.concat([predictedDf,_gr])
        predictedDf = predictedDf.drop_duplicates()
        return predictedDf
    def extractChanges(self):
        grouped = self.grabbedDf.groupby(['TimeStamp'])
        LcsPrev = np.array([])
        changed = []
        for ts,gr in grouped:
            uniqueLcs = gr.laccid.unique()
            if len(LcsPrev)>0:
                uniqueLcsNext = uniqueLcs
                if sorted(list(LcsPrev))!=sorted(list(uniqueLcsNext)):
                    changed.append({'prev':list(LcsPrev),'next':list(uniqueLcsNext)})
                LcsPrev = uniqueLcs
            if not len(LcsPrev)>0:
                LcsPrev = uniqueLcs
        return changed
    def findChanges(self,changedLcs,uniqueLevels):

        grouped = self.predictedDf.groupby(['segment','ratio'])
        predictedInfo = pd.DataFrame()
        LcsPrev = np.array([])
        ix = 0
        #LcsNext = None
        for pare in changedLcs:
            for (seg,rat),gr in grouped:
                if len(LcsPrev)>0:
                    uniqueLcsNext = gr.laccid.unique()
                    if (pare['next'] in uniqueLcsNext)&(pare['next'] not in LcsPrev):
                        leftDelta,rightDelta = self.findDiff(seg,uniqueLevels)
                        row = pd.DataFrame({'segment':seg,'left':prevPoint-leftDelta,'right':rat+rightDelta},index = [ix])
                        predictedInfo = pd.concat([predictedInfo,row])
                        ix+=1
                    LcsPrev = np.array([])
                if not (LcsPrev)>0:
                    uniqueLcsPrev = gr.laccid.unique()
                    if pare['prev'] in uniqueLcsPrev:
                        LcsPrev = uniqueLcsPrev
                        prevPoint = rat
                    else:
                        LcsPrev = np.array([])
                        prevPoint = None
        return predictedInfo


    def findActives(self,uniqueLevels):
        lcGrouped = self.grabbedDf.groupby('TimeStamp').\
            filter(lambda x : len(x)>1).groupby('TimeStamp').\
            apply(lambda x: np.unique(x['laccid']))
        laccidsAll = np.unique(lcGrouped.to_dict().values())

        filtered = self.predictedDf.groupby(['segment','ratio']).filter(lambda x : len(x)>1)

        activeGroup = filtered.groupby(['segment','ratio'])
        activePoints = activeGroup['laccid'].apply(np.unique)
        d = activePoints.apply(lambda x: sorted(list(x)) == sorted(laccidsAll)).to_dict()
        predictedFrame = pd.DataFrame([key for key in d.keys() if d[key] == True],columns = ['segment','ratio']).sort(['segment','ratio'])
        predictedInfo = self.extractBounds(predictedFrame,uniqueLevels)

        return predictedInfo
    def extractBounds(self,frame,uniqueLevels = 'default',clip = True):
        """
        Extract minimum and maximum ratios from the frame by each segment and clip predicted earlier frame by them.
        :param frame: frame contains "active points" with 2 fields : segment and ratio {pd.DataFrame}
        :param uniqueLevels: length of boundaries by which is need to clip (seconds) {int}
        :param clip: if need to clip or not {boolean}
        :return: clipped dataFrame {pd.DataFrame}
        """
        if uniqueLevels == 'default':
            uniqueLevels = {'after':0,'before':0}
        leftDelta,rightDelta = 0,0
        grouped = frame.groupby('segment')
        Predicted = pd.DataFrame()
        for seg,gr in grouped:
            _gr = pd.DataFrame({'segment':[seg]})
            if not clip:
                leftDelta,rightDelta = self.findDiff(seg,uniqueLevels)
            _gr.loc[:,'left'],_gr.loc[:,'right'] = min(gr['ratio'])-leftDelta,max(gr['ratio'])+rightDelta
            Predicted = pd.concat([Predicted,_gr])
        return Predicted
    def findDiff(self,seg,spread):
        #frame = frame.sort(['segment','ratio','laccid'])
        #diffs = np.diff(frame['ratio'],1)

        interpStep = self.segmentsStepsDf[self.segmentsStepsDf.segment == seg].interpStep.values[0]
            #diffs[diffs!=0][0]
        left,right = interpStep*spread['before'],interpStep*spread['after']
        return left,right

    def byLacCidMod2(self):
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
        for step in range(len(self.grabbed_lc),1,-1):
            # check all combinations
            for sublist in itertools.combinations(self.grabbed_lc,step):
                predicted_subDf = self.predictedDf[self.predictedDf['laccid'].isin(sublist)]
                segments = predicted_subDf['segment'].unique()
                # find the right segments for this combination
                for seg in segments:
                    seg_subDf = predicted_subDf[predicted_subDf['segment'] == seg]
                    lc_subList = seg_subDf['laccid'].unique()
                    if (set(sublist).issubset(set(lc_subList))) == True:
                        predicted_segments.append(seg)
            if predicted_segments!=[]:
                break
        # if something founded - reduce the selection of predicted segments.
        if predicted_segments!=[]:
            self.predictedDf = self.predictedDf[self.predictedDf['segment'].isin(predicted_segments)]
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
        self.unpredicted = 0
        self.resultsDf = pd.DataFrame()
        predictedDf = pd.DataFrame()
        fullPredicted = pd.DataFrame()

        # dataFrame contained control Rows.
        ReducingTypes = {'byAbs':'maxLimit','byCorr':'localMaxima'}
        # 1. Split phone data on base step's sections.
        if useSmoothed  ==True:
            #self.interpPowers = self.grabbedDf.groupby(['laccid'])['Power'].apply(list).to_dict()
            self.interpPowers = list(self.grabbedDf['Power'])
        else:
            self.interpolateByTimeStep()
        # 2. Compare powers of grabbed log with powers from database
        # a) If the variance of grabbed log close to zero --> compare Mean by list of absolute Power values.
        # b) Else --> compare the coefficients of correlation
        #       If corrCoeff < 0 : extract this indexes from predicted dataFrame
        #       If corrCoeff > 0 : find local maximums at the list of corrCoeffs and
        #                          extract all of the others from predicted dataFrame

        absMeans = self.powerCorrelator.analyzeLC(self.grabbedDf.groupby(['laccid'])['Power'].apply(list).to_dict())
        # Extract indexes iteratively
        powersDf = self.predictedDf.groupby(['segment'])
        first,last = 0,0
        for (seg,SegLcGroup) in powersDf:
            #analyzedSection = self.interpPowers[lc]
            analyzedSection = self.interpPowers
            if len(self.grabbed_lc) == 1:
                method = self.powerCorrelator.checkPredictionMethod(self.grabbed_lc[0], absMeans)
            else:
                method = 'byCorr'
            redType = ReducingTypes[method]
            predictedPart,allPredicted,last = self.powerCorrelator.loopThroughLaccid(SegLcGroup,method,analyzedSection,redType,return_all=True,last = last)

            predictedPart['sliceNumber'] = range(first,last)
            first = last
            predictedDf = pd.concat([predictedDf,predictedPart])
            fullPredicted = pd.concat([fullPredicted,allPredicted])

        if predictedDf.empty != True:
            controlCheck = 'controls' not in predictedDf.columns.values
            if controlCheck == True:
                print ""
            self.predictedDf = predictedDf
            self.fullPredicted = fullPredicted
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



class SignalCorrelator():
    def __init__(self,minCorrcoeff = 0.4,
                        corrDelta = 0.3,
                        absDelta = 0.3,
                        windowStep = 1,
                        minVariance = 0.3):
        # minimum of the correlation coefficient (only for byPowerCorr algorithm)
        self.minCorrcoeff = minCorrcoeff
        # delta error of correlation coefficient (only for byPowerCorr algorithm)
        self.corrDelta = corrDelta
        # delta error of range between means of Powers (only for byPowerCorr algorithm)
        self.absDelta = absDelta
        # size of data set for grabbed data frame grouped by laccid
        #self.LCshape = LCshape
        # step over which the coefficient of correlation will be computed (only for byPowerCorr algorithm)
        self.windowStep = windowStep
        # Quantile values to filter data computation by correlation from by AbsPowerMedians
        self.fQua,self.lQua = 25,75
        self.deltaQua = 0.1

        #self.numRaces = numRaces
        # the minimum variance of Powers in the grabbed dataFrame for each cell(only for byPowerCorr algorithm)
        self.minVariance = minVariance
        self.predictedDf = pd.DataFrame()

    def loopThroughLaccid(self,SegLcFrame,method,analyzedSection,redType,return_all = False,last = 0):
        """
        predict position of analyzedSection into the origin DataFrame, each of belongs to one segment and one laccid
        :param SegLcFrame: origin DataFrame contains info about one segment and one laccid {pd.DataFrame}
        :param method: prediction method: ('byCorr','byAbs') {str}
        :param analyzedSection: section containing Power values {np.array}
        :param redType: method of reducing of prediction frame ('maxLimit','localMaxima') {'str'}
        :return:
        """
        predictedGroup = pd.DataFrame()
        allPredicted = pd.DataFrame()

        predictedIndexes = self.movingAverage(SegLcFrame,analyzedSection,method)
        # extract indexes and append them to the main list
        extractedInfo,allIndexes = self.processPC(predictedIndexes,method = method,redType = redType,return_all=return_all)
        if not extractedInfo.empty:
            predictedGroup = self.writePredictedInfo(extractedInfo,SegLcFrame)
            #predictedDf = pd.concat([predictedDf,predictedGroup])
            if return_all:
                allPredicted,last = self.slicesFromPoints(allIndexes,method,SegLcFrame,last)
                #predictedAll = pd.concat([predictedAll,allPredicted])
        if return_all:
            return predictedGroup,allPredicted,last
        else:
            return predictedGroup
    def checkPredictionMethod(self,lc,absMeans):
        if lc in absMeans.keys():
            method = 'byAbs'
        else:
            method = 'byCorr'
        return method
    def reducePredPowerCorrSamples(self,predictedDf,by = 'laccid'):
        self.unpredicted = 0
        controlDf = predictedDf[(predictedDf['controls'] == 1)]
        byAbs = controlDf[controlDf['method'] =='byAbs']
        byCorr = controlDf[controlDf['method'] =='byCorr']
        byAbsIxs = self.filterMod(byAbs,'coeffs','min',by = by)
        byCorrIxs = self.filterMod(byCorr,'coeffs','max',by = by)
        maxCorrMinDeltaIx = np.array(byAbsIxs + byCorrIxs)
        predictedDf.loc[predictedDf.index.isin(maxCorrMinDeltaIx),'maxCorrMinDelta'] = 1
        predictedDf['maxCorrMinDelta'] = predictedDf['maxCorrMinDelta'].fillna(0)
        return predictedDf
    def filterMod(self,df,val,func,by = 'laccid'):
        """
        Split data on groups and filter the data at column by simple function
        :param df: input dataFrame {pd.DataFrame}
        :param by: column to group {str}
        :param val: column to filter {str}
        :param func: filter function name {str}
        :return:
        """
        Ixs = []
        if df.empty!=True:
            #if not mode:
            if by:
                byAbsVals = df.groupby([by])[val].apply(eval(func)).to_dict()
                for key in df[by].unique():
                    lcGr = df[df[by] == key]
                    filteredGr = lcGr[lcGr[val].isin([byAbsVals[key]])]
                    grIxs = list(filteredGr.index)
                    Ixs = Ixs + grIxs
            else:
                byAbsVal = df.apply(eval(func))[val]
                Ixs = list(df[df[val] == byAbsVal].index)

            #else:
            #byAbsVals = df.groupby([by])[val].last().to_dict()
        return Ixs
    def analyzeLC(self,analyzedDict):
        """
        Compute the means if the variance of grabbed log close to zero
        :return:
        """
        #first quantile
        interpPowers = pd.Series(analyzedDict)
        fQua = interpPowers.apply(lambda x : np.percentile(x,self.fQua))
        lQua =  interpPowers.apply(lambda x : np.percentile(x,self.lQua))
        deltaQua = abs(lQua - fQua)
        byAbsKeys = deltaQua[deltaQua<self.deltaQua].keys()
        filteredLC = interpPowers[interpPowers.keys().isin(byAbsKeys)]
        absInfo = filteredLC.apply(lambda x : np.median(x)).to_dict()

        #powerVariances = {lc:np.var(self.interpPowers[lc]) for lc in self.interpPowers.keys()}
        # splittdLcs = [lc for lc in powerVariances if powerVariances[lc]<self.minVariance]
        #absInfo = {lc: np.mean(self.interpPowers[lc]) for lc in splittdLcs}
        return absInfo
    def processPC(self,predictedIndexes,method ='byCorr',redType = 'localMaxima',return_all= False):
        indexes = {}
        allIndexes = []
        decs = lambda x : float("%.5f" %x)

        predInfo = pd.DataFrame()
        allPredicted = pd.DataFrame()
        values = [p for (l,p) in predictedIndexes if p!=np.nan]
        if (values!=[np.nan]) and (values!=[]):
            if method == 'byCorr':
                controlPoint = max(values)
            # branch according to the type of coefficients of correlation extraction
                if redType == 'localMaxima':
                    # find local maximums
                    values = [-np.Inf]+values+[-np.Inf]
                    localMaximaIx = argrelextrema(np.array(values),np.greater)[0]
                    localMaximaValues = [values[localMaximaIx[i]] for i in range(0,len(localMaximaIx))]
                    # extract the last index from list  by condition
                    # "last" because it is the last user's position before phone has been pushed the data
                    indexes = {ix[-1]: decs(val) for (ix,val) in predictedIndexes if val in localMaximaValues}
                    allIndexes = [(ix,val) for (ix,val) in predictedIndexes if val in localMaximaValues]
                if redType == 'minLimit':
                    # find minimum acceptable level of coefficients
                    minCoeff = controlPoint - self.corrDelta
                    # extract the last index from list by condition
                    indexes = {ix[-1]: decs(val) for (ix,val) in predictedIndexes if val > minCoeff}
                    allIndexes = [(ix,val) for (ix,val) in predictedIndexes if val > minCoeff]
            if method == 'byAbs':
                controlPoint = min(values)
                if redType == 'maxLimit':
                    maxDelta = controlPoint + self.absDelta
                    indexes = {ix[-1]:delta for (ix,delta) in predictedIndexes if delta < maxDelta}
                    allIndexes = [(ix,delta) for (ix,delta) in predictedIndexes  if delta < maxDelta]
            # find control indexes(maximum coefficient of correlation or minimum of deltaPowers)
            controlIndexes = {ix[-1]:1 for (ix,val) in predictedIndexes if val == controlPoint}
            if controlIndexes =={}:
                print ""
            d = {'coeffs':indexes,'method':method,'controls':controlIndexes}
            predInfo = pd.DataFrame.from_dict(d)
            predInfo['controls'] = predInfo['controls'].fillna(0)
        return predInfo,allIndexes
    def slicesFromPoints(self,ixs,method,SegLcFrame,last):
        allPredicted = pd.DataFrame()
        step = last
        for sl,val in ixs:
            fr = pd.DataFrame(index = range(sl[0],sl[1]+1),data = {'coeffs':val,'method':method,'sliceNumber':step})
            fr = self.writePredictedInfo(fr,SegLcFrame)
            allPredicted = pd.concat([allPredicted,fr])
            step +=1
        #allPredicted.loc[controlIndexes,'controls'] = 1
        #allPredicted['controls'] = allPredicted['controls'].fillna(0)
        return allPredicted,step
    def movingAverage(self,originDf,analyzedSection,method):
        predictedIndexes = []
        # window wide
        ww = len(analyzedSection)
        i = 0
        while (i+ww)<=originDf.shape[0]:
            #print originDf.iloc[i+ww-1]['ratio'] - originDf.iloc[i]['ratio']
            # move along indexes at the database and compare section from it
            # with grabbed section
            originSection = np.array(originDf[i:i+ww]['Power'])
            originIndexes = np.array(originDf[i:i+ww].index)
            # Check if the signal is in list of constant signals
            # if yes --> compute deltaPower
            if method =='byAbs':
                coeff = self.checkAbsolutePower(analyzedSection)
            # if no --> compute the correlation
            if method == 'byCorr':
                coeff = self.checkCorrelation(originSection,analyzedSection)
            if coeff:
                predictedIndexes.append(([originIndexes[0],originIndexes[-1]],coeff))
            # step forward
            i+=self.windowStep
        return predictedIndexes
    def writePredictedInfo(self,extractedFrame,originFrame):
        extractedIxs = np.array(extractedFrame.index)
        predictedGroup = originFrame[originFrame.index.isin(extractedIxs)]
        predictedGroup = pd.concat([predictedGroup,extractedFrame],axis = 1)
        return predictedGroup

    def checkCorrelation(self,analyzedSection,originSection):
        coeff = None
        corrCoeff = np.corrcoef(originSection,analyzedSection)[0,1]
        # around 0 - 0.5 else --> no correlation.Else --> append to the list
        if corrCoeff > self.minCorrcoeff:
            coeff = corrCoeff
        return coeff
    def checkAbsolutePower(self,analyzedSection):
        coeff = None
        # check the variance
        # if variance close to zero --> compare absolute Powers. Else --> step over
        fQua = np.percentile(analyzedSection,q = self.fQua)
        lQua = np.percentile(analyzedSection,q = self.lQua)
        deltaQua = lQua-fQua
        if deltaQua < self.deltaQua:
            coeff = np.median(analyzedSection)
            # coeff = abs(absMeans[lc] - np.mean(grabSection))
        return coeff
if __name__ == "__main__":
    powerCorr = PosAlgorithm()
    powerCorr.byPowerCorr()
