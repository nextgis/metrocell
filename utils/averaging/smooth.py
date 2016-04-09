# coding=utf-8
__author__ = 'Alex'

from scipy.stats import signaltonoise
from filters import Filters
import pandas as pd
import utilities,variables

class Smooth():
    def __init__(self):
        self.combinations = {'U':('User','NumRaces'),'R':('race_id','NumUsers')}
        self.correlator = None
        self.filters = None
        self.segmentTime = None
        self.predictionMethod = variables.averaged_cell_pars['predictionMethod']
        self.reducingType = variables.averaged_cell_pars['reducingType']
        self.MNC = None
        self.num_races = 1
        self.num_users = 1

        #Parameters to check if data corresponds to correlation computation algorithm
        # minimum length of signal
        self.frameLength = variables.averaged_cell_pars['frameLength']
        self.maxLag = variables.averaged_cell_pars['maxLag']
        self.interpStep = None
        # ratio difference between 2 signals of one cell grabbed by two different users.

        self.journal = {'User':pd.DataFrame(),'race_id':pd.DataFrame()}


        return


    def initCombinations(self,df,combinations = 'RU'):
        """
        Initialize filter - move - smooth algorithms.
        :param df: frame to predict. Must contain one laccid on segment!
        :param combinations: sequence of litters,to identify an order of moving. 'R' - by race; 'U' - by user. {'RU','UR'}
        :return: smoothed dataframe or input dataframe if
        """

        self.filters.interpStep = self.interpStep
        by1,checkField1 = self.combinations[combinations[0]]
        by2,checkField2 = self.combinations[combinations[1]]

        self.defaultDiff = self.interpStep * self.maxLag



        df = self.smoothByAlg(df,'median',by = 'race_id')
        df = self.computeDataByRaces(df,store_vals= True)

        translated,fewDataDf = self.initMoving(df,by = [by1],checkField=checkField1)
        if (not translated.empty) and (self.num_races != 1):
            translated = self.computeDataByRaces(translated)
            #translated['NumUsers'] = self.num_users
            #translated['NumRaces'] = self.num_races
            df = self.smoothByAlg(translated,'kmeans',missedFields= ['NumRaces',by1], by = by1,checkFields = [checkField1])
            #if combinations == 'UR':
                #df = self.interpolatePowers(df)
            #df,qualities = self.smoothByAlg(df,'median',by = '')
            #fewDataDf!!!
            transFrame,fewDataDf2 = self.initMoving(df,checkField=checkField2,return_trans = True)
            df = self.translateByShifts(translated,transFrame,excludeDf = fewDataDf2)
            #self.filters.endIndexes,self.filters.levelsNum = self.getBoundaries(df,end = True)
            Ideal = self.smoothByAlg(df,'kmeans',missedFields= ['NumRaces'],by = '',checkFields = [checkField1,checkField2],end = True) #,checkFields = [checkField2]

            #IdealFiltered = self.filters.rollingMean(Ideal,'Power',by = '',window = 30)
            return Ideal,fewDataDf
        else:
            Ideal = self.smoothByAlg(df,'kmeans',missedFields= ['NumRaces'],by = '',checkFields = [checkField1,checkField2],end = True,check_data = False) #,checkFields = [checkField2]
            return Ideal,fewDataDf
            # for loop through the other data by User and say that each user's group is origin data

    def smoothByAlg(self,df,filt,missedFields = None,by = '',checkFields = 'default',end = False,check_data = True):
        """
        group by column if need, initialize smoothing algorithm and concatenate the result
        :param df: {pd.DataFrame}
        :param filt: algorithm to filtrate {'median','kmeans'}
        :param missedFields: additional fields will be missed after filtration
        :param by: column to group
        :param checkFields: field to check if dataframe should be smoothed
        :param end: if smoothing is last.
        :return: concatenated smoothed frames
        """
        PredictedDf = pd.DataFrame()
        if by:
            byField = df.groupby(by)
            for index,gr in byField:
                PredictedDf = self.filters.prepareAndPredict(gr,filt,PredictedDf,missedFields = missedFields,checkFields = checkFields,end = end,check_data = check_data)
        else:
            PredictedDf = self.filters.prepareAndPredict(df,filt,PredictedDf,missedFields = missedFields,checkFields = checkFields,end = end,check_data = check_data)
        return PredictedDf

    def initMoving(self,df,checkField,by = None,return_trans = False):
        """
        Initialize translation algorithm.
        :param df: frame to translate {pd.DataFrame}
        :param checkField: field to check if group contains more than one unique value. {'NumUsers','NumRaces'}
        :param by: column to group by {str}
        :param return_trans: whether or not need to return translation value. If true - return,else - return translated frame {boolean}
        :return:UpdatedDf  : translated df {pd.DataFrame}
                fewDataDf  : data frame contains "lack of data frames" {pd.DataFrame}
                TransFrame : data frame contains translations per user or race {pd.DataFrame}
        """

        UpdatedDf = pd.DataFrame()
        TransFrame = pd.DataFrame()
        fewDataDf = pd.DataFrame()
        if by:
            groups = df.groupby(by)
            for index, group in groups:
                UpdatedDf, TransFrame,fewDataDf = self.move(group,TransFrame,UpdatedDf,fewDataDf,checkField = checkField,index = index)
        else:
            UpdatedDf, TransFrame,fewDataDf = self.move(df,TransFrame,UpdatedDf,fewDataDf,checkField = checkField)
        if not TransFrame.empty:
            print TransFrame
        if not return_trans:
            return UpdatedDf,fewDataDf
        else:
            return TransFrame,fewDataDf
    def move(self,frame,TransFrame,UpdatedDf,fewDataDf,checkField,index = ''):
        """
        Initialize checking,preparing and moving algorithms
        :param frame: input df {pd.DataFrame}
        :param TransFrame: data frame contains translations per user or race {pd.DataFrame}
        :param UpdatedDf: translated df {pd.DataFrame}
        :param fewDataDf: data frame contains "lack of data frames" {pd.DataFrame}
        :param checkField: field to check if group contains more than one unique value. {'NumUsers','NumRaces'}
        :param index: group index {'race_id','User'}
        :return:
        """
        if checkField == 'NumUsers':
            loopBy = 'User'
        if checkField == 'NumRaces':
            loopBy =  'race_id'
        readyToCorr = False
        check = len(frame[loopBy].unique())
        while (readyToCorr == False) or (frame.empty == True):
            if check>1:
                # move signal only if df contains more than  1 user
                frame,originFrame,analyzedFrame,fewDataFrame,readyToCorr = self.prepareToCorrelation(frame,by = loopBy)
                if not fewDataFrame.empty:
                    fewDataDf = pd.concat([fewDataDf,fewDataFrame])
                    check-=1
                if readyToCorr:
                    translations = self.loopThroughOrigins(originFrame,analyzedFrame,loopBy)
                    updatedGroup,transFrame = self.getMovedData(frame,translations,loopBy,index)
                    TransFrame = pd.concat([TransFrame,transFrame])
                    UpdatedDf = pd.concat([UpdatedDf,updatedGroup])
            else:
                UpdatedDf = pd.concat([UpdatedDf,frame])
                break
        return UpdatedDf, TransFrame, fewDataDf
    def getMovedData(self,df,trans,field,index = ''):
        """
        get translated Data
        :param df: input df {pd.DataFrame}
        :param trans: translation value {int}
        :param field: field of group
        :param index: field of index
        :return:
        """
        updatedFrame,updatedTrans = self.updateRatios(df,trans,field,toTime= False)
        transFrame = pd.DataFrame({index:updatedTrans}).transpose()
        return updatedFrame,transFrame
    def prepareToCorrelation(self,df,by):
        """
        prepare dataframe to correlation algorithm
        :param df: input df {pd.DataFrame}
        :param by: group by column
        :return: df                     : input dataframe from
                 originFrame            : frames to move
                 analyzedFrameUpdated   : stable main frame contains the most really coordinates
                 fewDataFrame           : lack of data frame
                 readyToCorr            : if data frame is ready to be pushed to correlation algorithm
        """
        _df = df.copy()
        fewDataFrame = pd.DataFrame()
        if by == 'User':
            self.startPoint,noises = Filters.noisyUser(_df,by,col = 'rawPower')
            #df = self.filters.rollingMean(df,'Power',by = by,noises = noises)
        if by == 'race_id':
            self.startPoint = _df['race_id'].iloc[0]
        analyzedFrame,originFrame = self.splitFrameByMinLen(_df,by)
        analyzedFrameUpdated,readyToCorr = self.checkBoundaries(originFrame,analyzedFrame,by)
        if (not readyToCorr):
            _df.drop(analyzedFrame.index,inplace=True)
            fewDataFrame = analyzedFrame
        return _df,originFrame,analyzedFrameUpdated,fewDataFrame,readyToCorr
    def loopThroughOrigins(self,originFrame,analyzedFrame,loopBy):
        """
        loop through the origin data frame and get the translation for each of them
        :param originFrame: : frames to move  {pd.DataFrame}
        :param analyzedFrame: stable main frame contains the most really coordinates {pd.DataFrame}
        :param loopBy:  group by column {str]
        :return:translations {dict]
        """
        jname = loopBy
        analyzedData = self.extractDataToAnalyze(analyzedFrame,loopBy)
        translations = {analyzedData[loopBy]:0}
        originGroups = originFrame.groupby(loopBy)
        for index,originGroup in originGroups:
            trans = self.getTranslation(originGroup,analyzedData,loopBy,jname = jname)
            translations.update(trans)
        translations = self.shiftbyValue(translations,self.startPoint,analyzedData[loopBy])
        return translations
    def getTranslation(self,originGroup,analyzedData,loopBy,jname):
        """
        initialize the correlation algorithm and get the translation
        :param originGroup: frame (of one group!) to move
        :param analyzedData: stable main array {np.array}
        :param loopBy:  group by column {str]
        :param jname: name of journal to save the results of correlation
        :return:
        """
        trans = {}
        predictedPart = self.correlator.loopThroughLaccid(originGroup,self.predictionMethod,analyzedData['section'],self.reducingType)
        if not predictedPart.empty:
            processedPart = self.correlator.reducePredPowerCorrSamples(predictedPart)
            predictedPoints = processedPart[processedPart['controls'] == 1]
            predictedPoint = predictedPoints.iloc[0]
            delta = predictedPoint['ratio'] - analyzedData['ratio']
            # todo: the algorithm of shifting should exclude bad signals and choose main signal as median but not as the most noisy
            if delta>variables.averaged_cell_pars['max_shift_delta']:
                delta = 0
            self.toJournal(predictedPoints,jname)
            trans = {predictedPoint[loopBy]:delta}
        return trans
    def toJournal(self,row,jname):
        """
        push correlation result to journal
        :param row:  row contains information about passed correlation algorithm {pd.DataFrame}
        :param jname: key name of journal
        :return:
        """
        self.journal[jname] = pd.concat([self.journal[jname],row])
    def updateRatios(self,df,translations,field,toTime = False):
        """
        Translate frame along ratio axis
        :param df: frame to translate {pd.DataFrame}
        :param translations: translations dictionary {dict}
        :param field: key field
        :param toTime: translate ratio to time {boolean}
        :return: updatedDf    : translated dataFrame
                 translations : translations dictionary (may be need to return if it has been translated to time)
        """
        updatedDf = pd.DataFrame()
        for k in translations.keys():
            userDf = df[df[field] == k].copy()
            userDf.loc[:,'ratio'] -=translations[k]
            if toTime:
                translations[k] = translations[k]*self.segmentTime
            updatedDf = pd.concat([updatedDf,userDf])
        return updatedDf,translations

    def shiftbyValue(self,trans,startPoint,analyzedPoint):
        """
        Shift 0 point from analyzed(containing minimum data length) to main (containing the most noisy Powers)
        :param trans: key = User, value = delta ratios{dict}
        :param mainUser: shift to
        :param analyzedUser: shift from
        :return: origin dict with updated values {dict}
        """
        if startPoint in trans.keys():
            shift = trans[startPoint] - trans[analyzedPoint]
            for point in trans.keys():
                trans[point] = trans[point] - shift
        return trans
    def extractDataToAnalyze(self,analyzedFrame,by):
        """
        extract section,ratio and 'by' field value from analyzed Df
        :param analyzedFrame: frame to analyze {pd.DataFrame}
        :param by: group by column {str}
        :return: {dict}
        """
        section  = analyzedFrame['Power'].__array__()
        lastRatio = analyzedFrame.iloc[-1]['ratio']
        fieldVal = analyzedFrame.iloc[0][by]
        data = {'section':section,'ratio':lastRatio,by:fieldVal}
        return data
    def splitFrameByMinLen(self,df,by):
        """
        find section with the lowest delta ratio (only if it more than default min)
        and assign it as analyzed section
        :return:
        """
        analyzedField = df.groupby(by).apply(len).argmin()
        keys = df.groupby(by).groups.keys()
        if len(keys)>1:
            analyzedFrame = df[df[by] == analyzedField]
            originFrame = df[df[by] != analyzedField]
        else:
            analyzedFrame = pd.DataFrame()
            originFrame = df
        return analyzedFrame,originFrame
    def lastFirstDelta(self,frame,col):
        delta = frame.iloc[-1][col] - frame.iloc[0][col]
        return delta
    def checkBoundaries(self,originFrame,analyzedFrame,by):
        """
        check if boundaries length more than default minumum
        :param originFrame: : frames to move  {pd.DataFrame}
        :param analyzedFrame: stable main frame contains the most really coordinates {pd.DataFrame}
        :by: column to group
        """

        readyToCorr = True
        mainDeltaRatios = self.lastFirstDelta(analyzedFrame,'ratio')
        if mainDeltaRatios > 2*self.defaultDiff:
            grouped = originFrame.groupby(by)
            for index,gr in grouped:
                userDeltaRatios = self.lastFirstDelta(gr,'ratio')
                boundaries = userDeltaRatios - self.defaultDiff
                diff = mainDeltaRatios - boundaries
                if diff>0:
                    # clip by new boundaries
                    analyzedFrame = self.clipByBoundaries(analyzedFrame,diff/2)
                    if len(analyzedFrame) < self.frameLength:
                        readyToCorr = False
                        break
                    else:
                        mainDeltaRatios = self.lastFirstDelta(analyzedFrame,'ratio')
        else:
            readyToCorr = False
        return analyzedFrame,readyToCorr


    def clipByBoundaries(self,frame,clip):
        """
        clip frame by ratio boundaries
        :param frame: df to clip{pd.DataFrame}
        :param clip: clip value {float}
        """
        r_bound = max(frame['ratio'])-clip
        l_bound = min(frame['ratio'])+clip
        analyzedFrame = frame[(frame['ratio']<r_bound)&(frame['ratio']>l_bound)]
        return analyzedFrame

    def mainUserByNoise(self,df):
        """
        Identify main User according to the theory that the most noisy signal has the most actual coordinates
        """
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
    def computeDataByRaces(self,df,store_vals = False):
        """
        compute the number of races and users per each race
        """
        df_updated = pd.DataFrame()
        _df = df.copy()
        _df.loc[:,'NumRaces']  = num_races = len(_df.race_id.unique())
        _grouped = _df.groupby('race_id')
        for race_id,gr in _grouped:
            gr.is_copy = False
            gr.loc[:,'NumUsers'] = num_users = len(gr.User.unique())
            df_updated = pd.concat([df_updated,gr])
        if store_vals:
            self.num_races = num_races
            self.num_users = num_users
        return df_updated

    def translateByShifts(self,df,transFrame,by = 'race_id', excludeDf = pd.DataFrame()):
        """
        translate dataframe by shifts
        :param df: {pd.DataFrame}
        :param transFrame: {pd.DataFrame}
        :param by: column to groupby {str}
        :param excludeDf: frame need to exclude from tranlation
        :return:
        """
        if not excludeDf.empty:
            df.drop(excludeDf.index,inplace = True )
        if not transFrame.empty:
            UpdatedDf = pd.DataFrame()
            grouped = df.groupby(by)
            for ix,gr in grouped:
                _gr = gr.copy()
                if ix in transFrame.columns.values:
                    translation = transFrame.irow(0)[ix]
                    _gr.loc[:,'ratio'] -=translation
                    UpdatedDf = pd.concat([UpdatedDf,_gr])
            return UpdatedDf
        else:
            return df

