# coding=utf-8
from sklearn.ensemble import AdaBoostRegressor
from scipy.stats import signaltonoise
from filters import Filters
import pandas as pd
import numpy as np

from utils import Utils
import os

class Smooth():
    def __init__(self,correlator,filters
                 ):
        self.correlator = correlator
        self.filters = filters

        self.segmentTime = None
        self.predictionMethod = 'byCorr'
        self.reducingType = 'localMaxima'
        #Parameters to check if data corresponds to correlation computation algorithm
        # minimum length of signal
        self.frameLength = 15
        # ratio difference between 2 signals of one cell grabbed by two different users.
        self.defaultDiff = 0.05 * 2

        self.journal = pd.DataFrame()

        self.ut = Utils()
        return

    def mainUserByNoise(self,df):
        mainSTN = -10000
        grouped = df.groupby('User')
        for user,gr in grouped:
            ar = gr['Power'].__array__()
            STN = signaltonoise(ar)
            if STN > mainSTN:
                mainSTN = STN
                mainUser = user
        if not mainUser:
            raise ValueError("Can't compute signal to noise value")
        print mainUser,mainSTN
        return mainUser

    def lastFirstDelta(self,frame,col):
        delta = frame.iloc[-1][col] - frame.iloc[0][col]
        return delta
    def checkBoundaries(self,originFrame,analyzedFrame):
        readyToCorr = True
        mainDeltaRatios = self.lastFirstDelta(analyzedFrame,'ratio')
        grouped = originFrame.groupby('User')
        for indx,gr in grouped:
            userDeltaRatios = self.lastFirstDelta(gr,'ratio')
            boundaries = userDeltaRatios - self.defaultDiff
            diff = mainDeltaRatios - boundaries
            if diff>0:
                # clip by new boundaries
                analyzedFrame = self.clipByBoundaries(analyzedFrame,diff/2)
                mainDeltaRatios = self.lastFirstDelta(analyzedFrame,'ratio')
        if len(analyzedFrame) < self.frameLength:
            readyToCorr = False
        return analyzedFrame,readyToCorr


    def clipByBoundaries(self,frame,clip):
        r_bound = max(frame['ratio'])-clip
        l_bound = min(frame['ratio'])+clip
        analyzedFrame = frame[(frame['ratio']<r_bound)&(frame['ratio']>l_bound)]
        return analyzedFrame

    def initCombinations(self,df,combinations = 'default'):
        # 4 steps:
        # 1. smooth by Races.
        # 2. smooth using ??? method to get an ideal line.
        # 3. lead to the one ratio based on the "lag theory".
        # 4. smooth by Users.
        filtered = self.smoothByAlg(df,'median')
        translated = self.moveAlongRatio(filtered,by = ['race_id'],powerColumn = 'medFilt')
        """
        byUsers = self.smoothByAlg(df,'kmeans',missedField = 'race_id',by = 'User')
        Ideal = self.smoothByAlg(byUsers,'kmeans',missedField = 'User',by = '')
        """
        return translated
            # for loop through the other data by User and say that each user's group is origin data
    def smoothByAlg(self,df,filt,missedField = '',by = ''):
        missedName = 'Num' + missedField
        PredictedDf = pd.DataFrame()
        if by:
            byField = df.groupby(by)
            for index,gr in byField:
                PredictedDf = self.filters.prepareAndPredict(gr,filt,PredictedDf,missedName,missedField,by)
        else:
            PredictedDf = self.filters.prepareAndPredict(df,filt,PredictedDf)
        return PredictedDf

    def moveAlongRatio(self,df,by = None,powerColumn = 'Power'):
        #mainDf = df[df['User'] == mainUser]
        UpdatedDf = pd.DataFrame()
        TransFrame = pd.DataFrame()
        if by:
            groups = df.groupby(by)
            for index, group in groups:
                updatedGroup,transFrame = self.getMovedData(group,index)
                TransFrame = pd.concat([TransFrame,transFrame])
                UpdatedDf = pd.concat([UpdatedDf,updatedGroup])
            print TransFrame
        else:
            UpdatedDf,updatedTrans = self.getMovedData(df)
        return UpdatedDf
    def getMovedData(self,df,index = ''):
        df,trans = self.initMovingAlongAxis(df)
        updatedFrame,updatedTrans = self.updateRatios(df,trans,toTime= False)
        transFrame = pd.DataFrame({index:updatedTrans}).transpose()
        return updatedFrame,transFrame
    def initMovingAlongAxis(self,df):
        trans = {}
        df,originFrame,analyzedFrame,readyToCorr = self.prepareToCorrelation(df)
        if readyToCorr:
            trans = self.loopThroughOriginUsers(originFrame,analyzedFrame)
        return df,trans

    def prepareToCorrelation(self,df):
        #self.mainUser = self.mainUserByNoise(df)
        self.mainUser,noises = Filters.noisyUser(df,'rawPower')
        dfFiltered = self.filters.rollingMean(df,'Power',noises)
        analyzedFrame,originFrame = self.splitFrameByMinLen(dfFiltered)
        analyzedFrame,readyToCorr = self.checkBoundaries(originFrame,analyzedFrame)
        return dfFiltered,originFrame,analyzedFrame,readyToCorr
    def loopThroughOriginUsers(self,originFrame,analyzedFrame):
        #mainUser = 'Anna'
        #analyzedFrame = self.clipByBoundaries(analyzedFrame,clip = 0.05)
        analyzedData = self.extractDataToAnalyze(analyzedFrame)
        trans = {analyzedData['User']:0}
        originGroups = originFrame.groupby('User')
        for index,originGroup in originGroups:
            predictedPart = self.correlator.loopThroughLaccid(originGroup,self.predictionMethod,analyzedData['section'],self.reducingType)
            if not predictedPart.empty:

                processedPart = self.correlator.reducePredPowerCorrSamples(predictedPart)
                predictedPoints = processedPart[processedPart['controls'] == 1]
                if len(predictedPoints) == 1:
                    predictedPoint = predictedPoints.iloc[0]
                    delta = predictedPoint['ratio'] - analyzedData['ratio']
                    self.toJournal(predictedPoints)
                    trans.update({predictedPoint['User']:delta})
                else:
                    print ""
        trans = self.shiftbyValue(trans,self.mainUser,analyzedData['User'])
        return trans
    def toJournal(self,row):
        self.journal = pd.concat([self.journal,row])
    def updateRatios(self,df,translations,toTime = False):
        updatedDf = pd.DataFrame()
        for user in translations.keys():
            userDf = df[df['User'] == user].copy()
            userDf.loc[:,'ratio'] -=translations[user]
            if toTime:
                translations[user] = translations[user]*self.segmentTime
            updatedDf = pd.concat([updatedDf,userDf])
        return updatedDf,translations

    def shiftbyValue(self,trans,mainUser,analyzedUser):
        """

        :param trans: key = User, value = delta ratios{dict}
        :param mainUser: shift to
        :param analyzedUser: shift from
        :return: origin dict with updated values {dict}
        """
        if mainUser in trans.keys():
            shift = trans[mainUser] - trans[analyzedUser]
            for user in trans.keys():
                trans[user] = trans[user] - shift
        return trans
    def extractDataToAnalyze(self,analyzedFrame):
        section  = analyzedFrame['Power'].__array__()
        lastRatio = analyzedFrame.iloc[-1]['ratio']
        user = analyzedFrame.iloc[0]['User']
        data = {'section':section,'ratio':lastRatio,'User':user}
        return data
    def splitFrameByMinLen(self,df):
        """
        :return:
        """
        # find section with the lowest delta ratio if it more than default and than say that it is analyzed section
        analyzedUser = df.groupby('User').apply(len).argmin()
        analyzedFrame = df[df['User'] == analyzedUser]
        originFrame = df[df['User'] != analyzedUser]
        return analyzedFrame,originFrame
