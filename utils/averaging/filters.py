__author__ = 'Alex'

import variables
from sklearn import neighbors
from sklearn import cross_validation
from scipy.signal import medfilt

import pandas as pd
import numpy as np
#from matplotlib.pyplot import plot

class Filters:
    def __init__(self):
        self.unique_names =  None
        self.nNeighbours =   None
        #self.minData =       minData
        self.test_size =     None
        self.interpStep = None
        self.endIndexes = None
        #self.min_lc_ratio =  None
        #self.max_lc_ratio =  None
        self.levelsNum =     None
        self.time_Df = None
        self.median_window = variables.averaged_cell_pars['median_window']
        # percent of noise data at each race
        self.minNoise = variables.averaged_cell_pars['minNoise']
    def checkDataBeforeSmooth(self,df,checkFields = None):
        check = True
        if not checkFields:
            checkFields = ['NumUsers','NumRaces']
        cols = [col for col in checkFields if col in df.columns.values]
        checkVals = [1]*len(cols)
        uniques = [list(df[col].unique()) for col in cols]
        uniques = sum(uniques,[])
        try:
            if uniques == checkVals:
                check = False
        except:
            print ""
        return check
    def prepareAndPredict(self, df, filt, PredictedDf,missedFields = None,checkFields = 'default',end = False,check_data = True):
        """
        initialize smoothing algorithm and check if dataframe should be smoothed
        :param df: {pd.DataFrame}
        :param filt: algorithm to filtrate {'median','kmeans'}
        :param missedFields: additional fields will be missed after filtration
        :param checkFields: field to check if dataframe should be smoothed
        :param end: if smoothing is last.
        :return:smooothed frame or input frame
        """
        quality = None
        if filt == 'kmeans':
            if check_data:
                check = self.checkDataBeforeSmooth(df,checkFields)
            else:
                check = True
            if check:
                unique_dict = self.addUniqueFields(df,missedFields)
                self.endIndexes,self.levelsNum = self.getBoundaries(df,end)
                if self.levelsNum>=5:
                    # predictedDict,quality = self.kmeansRegressor(df,end = end)
                    predictedDict,quality = self.LSPfit(df)
                    smoothed = self.combineColumns(predictedDict,unique_dict,quality)
                else:
                    smoothed = pd.DataFrame()
            else:
                smoothed = df
        elif filt == 'median':
            smoothed = self.medianFilter(df)
        PredictedDf = pd.concat([PredictedDf,smoothed])
        return PredictedDf
    def check_uniqueness(self,df,unique_names):
        """
        check fields on uniqueness. If column contains more than one unique value -error will be raised
        :param df:
        :param unique_names:
        :return:
        """
        unique_dict = dict.fromkeys(unique_names)
        for name in unique_names:
            unique_values = list(df[name].drop_duplicates())
            if len(unique_values)==1:
                unique_dict[name] = unique_values[0]
            else:
                raise ValueError("To many other unique values at field %s : %s"%(name,unique_values))
        return unique_dict
    def addUniqueFields(self,df,missedFields):
        unique_dict = self.check_uniqueness(df,self.unique_names + missedFields)
        return unique_dict
    def medianFilter(self,df):
        """
        Filter by median
        :param df: frame to filter
        :param window: rolling window
        :return:filtered frame
        """

        Filtered = pd.DataFrame()
        grouped = df.groupby(['User'])
        for user,gr in grouped:
            _gr = gr.copy()
            _gr.loc[:,'rawPower'] = _gr.loc[:,'Power']
            _gr.loc[:,'Power'] = medfilt(_gr['Power'],self.median_window)
            Filtered = pd.concat([Filtered,_gr])
        return Filtered
    def kmeansRegressor(self,df,
                    algorithm = 'auto',
                    w = 'default',end = False):
        """
        smoothing data using k-means algorithm
        :param df: dataframe to smooth
        :param algorithm: algorithm of finding the neighbours. see the documentation to scipy kmeans regressor {str}
        :param w: wigth {str}
        :param end: if true bring ratios to the end points.Else use minumum - maximum boundaries {boolean}
        :return: smoothed : smoothed dictionary with power and ratio keys
                 quality  : R^2 coefficient
        """
        if w == 'default':
            weights = ['uniform','distance']
        else:
            weights = w
        t = np.array(df['ratio'])
        y = np.array(df['Power'])
        #size = len(t)
        t.shape = (-1, 1)
        t_train, t_test, y_train, y_test = cross_validation.train_test_split(t, y, test_size=self.test_size, random_state=1)
        quality = best_q = -1000000
        best_params = (None, None)
        max_iter = min(len(t_train), self.nNeighbours)
        for n_count in range(1, max_iter):
            for w in weights:
                model = neighbors.KNeighborsRegressor(n_neighbors = n_count,algorithm = algorithm,weights = w)
                #model = AdaBoostRegressor(n_estimators = n_estimators)
                model = model.fit(t_train, y_train)
                quality = model.score(t_test, y_test)
                if quality > best_q:
                    best_q = quality
                    best_params = n_count,w
        if best_params == (None, None):
            raise ValueError("No best parameters!")
        n_count,weight = best_params

        print "bestParams : " + str(best_params[0]) + " : " + str(best_params[1])

        model = neighbors.KNeighborsRegressor(n_neighbors=n_count,algorithm = algorithm,weights = weight)
        #model = AdaBoostRegressor(n_estimators=n_estimators)
        model = model.fit(t_train, y_train)

        #ti = np.linspace(self.min_lc_ratio, self.max_lc_ratio,self.levelsNum)[:, np.newaxis]
        ti = self.endIndexes[:,np.newaxis]
        try:
            y_model = model.predict(ti)
        except:
            print ""
        y_model = np.around(y_model)

        smoothed = {'Power': y_model,'ratio': ti[:, 0]}
        return smoothed,quality

    def combineColumns(self,predictedDict,uniqueDict,predictedQuality):
        """
        combine predicted power and ratio columns with the other columns.
        :param predictedDict: predicted dictionary {dict}
        :param uniqueDict: dictionary of unique columns {dict}
        :param predictedQuality: predicted quality
        :return: binded dataframe {pd.DataFrame}
        """
        quality = None
        if predictedQuality!=None:
            quality = float("%.2f"%predictedQuality)
        uniqueDict.update({'quality':quality})
        for name in uniqueDict.keys():
            uniqueDict[name] = [uniqueDict[name]]*self.levelsNum
        predictedDict.update(uniqueDict)
        df_aver = pd.DataFrame(predictedDict)
        return df_aver

    @staticmethod
    def noisyUser(df,by,col = 'Power',window = 9):
        """
        Define user with maximum signal noise.
        :param df: pd.DataFrame contains several user's and per one laccid  {pd.DataFrame}
        :param col: column to compute {'str'}
        :param window: rolling window length {'int'}
        :return: name of user {'str'}
        """
        maxNoise = 0
        noises = {}
        grouped = df.groupby(by)
        for user,gr in grouped:
            user_fltrd = pd.rolling_median(gr[col],window,center = True)
            noisy_part = gr[user_fltrd != gr[col]].shape[0]/float(gr.shape[0])
            if noisy_part > maxNoise:
                noisyUser = user
                maxNoise = noisy_part
            noises.update({user:noisy_part})
        return noisyUser,noises

    def rollingMean(self,df,col,by = 'User',noises = None,window = 4):
        """
        smooth by rollin mean filter
        :param df: input dataframe {pd.DataFrame}
        :param col: column to smooth. 'Power', for ex {str}
        :param by: column to groupby {str}
        :param noises: value to compare user's noise. If defined, check if dataframe's noise more than minimum noise.{dict}
        :param window: rolling window
        :return: FilteredDf {pd.DataFrame}
        """
        FilteredDf = pd.DataFrame()
        rM = lambda gr,col,window: pd.rolling_mean(gr[col].interpolate(),window,center = True)
        if by:
            grouped = df.groupby(by)
            for user,gr in grouped:
                _gr = gr.copy()
                if noises[user]>self.minNoise:
                    _gr.loc[:,col] = rM(_gr,col,window)
                FilteredDf = pd.concat([FilteredDf,_gr])
        else:
            FilteredDf = df.copy()
            FilteredDf.loc[:,col] = rM(FilteredDf,col,window)
        FilteredDf = FilteredDf[~FilteredDf[col].isnull()]
        return FilteredDf
    def LSPfit(self,df,max_rank = 2):
        ideal_rank = None
        t = np.array(df['ratio'])
        y = np.array(df['Power'])
        coeffs = {}
        for rank in range(1,max_rank+1):
            coef = np.polyfit(t,y,rank,full = True)
            try:
                coeffs.update({coef[1][0]:rank})
            except:
                pass
           #new_x = np.linspace(np.min(t),np.max(y),50)
        try:
            ideal_rank = coeffs[min(coeffs.keys())]
        except:
            pass
        #RANK = 2
        if ideal_rank:
            coef = np.polyfit(t,y,ideal_rank,full = True)
            quality = np.sqrt(coef[1][0]/(len(t)-1))
            model = np.poly1d(coef[0])
            ti = self.endIndexes[:,np.newaxis][:, 0]
            predicted = model(ti)
            #plot(ti,predicted,'.')
            #predicted = [a[0] for a in predicted]
            smoothed = {'Power': predicted,'ratio': ti}
            return smoothed,quality
        else:
            # todo:check
            return pd.DataFrame(), -1
    def getBoundaries(self,df,end = False,col = 'ratio'):
        """
        Get col boundaries (min and max) and number of levels to create new smoothed dataframe
        :param df: input dataframe
        :param end: if true bring ratios to the end points.Else use minumum - maximum boundaries {boolean}
        :col: column to find. default 'ratio'
        :return:
        """
        lc_ratios = list(df[col])
        if end:
            lc_ratios = [i for i in lc_ratios if i>=0]
            min_lc_ratio,max_lc_ratio = min(lc_ratios),max(lc_ratios)
            bounds = np.searchsorted(self.time_Df[col],[min_lc_ratio,max_lc_ratio])
            extractedDf = self.time_Df[bounds[0]:bounds[-1]].drop_duplicates()
            ixs = extractedDf[col]
            levelsNum = len(ixs)
        else:
            min_lc_ratio,max_lc_ratio = min(lc_ratios),max(lc_ratios)
            levelsNum = (max_lc_ratio - min_lc_ratio) / self.interpStep
            ixs = np.linspace(min_lc_ratio,max_lc_ratio,levelsNum)
        return ixs,levelsNum
