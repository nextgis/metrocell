__author__ = 'Alex'

from sklearn import neighbors
from sklearn import cross_validation
from scipy.signal import medfilt

import pandas as pd
import numpy as np

class Filters:
    def __init__(self,unique_names,
                 nNeighbours,
                 minData,
                 test_size):
        self.unique_names =  unique_names
        self.nNeighbours =   nNeighbours
        self.minData =       minData
        self.test_size =     test_size

        self.min_lc_ratio =  None
        self.max_lc_ratio =  None
        self.levelsNum =     None

        # percent of noise data at each race
        self.minNoise = 0.3

    def prepareAndPredict(self,df,filt,PredictedDf,missedName = '',missedField = '',by = ''):
        unique_dict = self.addUniqueFields(df,list(by),missedField,missedName)
        if filt == 'kmeans':
            predictedDict,quality = self.kmeansRegressor(df)
            smoothed = self.combineColumns(predictedDict,unique_dict,quality)
        if filt == 'median':
            smoothed = self.medianFilter(df)
        PredictedDf = pd.concat([PredictedDf,smoothed])
        return PredictedDf
    def check_uniqueness(self,df,unique_names):
        unique_dict = dict.fromkeys(unique_names)
        for name in unique_names:
            unique_values = list(df[name].drop_duplicates())
            if len(unique_values)==1:
                unique_dict[name] = unique_values[0]
            else:
                raise ValueError("To many other unique values at field %s : %s"%(name,unique_values))
        return unique_dict
    def addUniqueFields(self,df,add_names,missedField,_name):
        unique_dict = self.check_uniqueness(df,self.unique_names + add_names)
        _count = len(df['race_id'].unique())
        missed_dict = {_name:_count}
        unique_dict.update(missed_dict)
        return unique_dict
    def medianFilter(self,df,window = 9):
        Filtered = pd.DataFrame()
        grouped = df.groupby(['User'])
        for user,gr in grouped:
            _gr = gr.copy()
            _gr.loc[:,'rawPower'] = _gr.loc[:,'Power']
            _gr.loc[:,'Power'] = medfilt(_gr['Power'],window)
            #_gr.loc[:,'Power'] = pd.rolling_mean(_gr['Power'],window)
            Filtered = pd.concat([Filtered,_gr])
        return Filtered
    def kmeansRegressor(self,df,
                    algorithm = 'auto',
                    w = 'default'):
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
        model = neighbors.KNeighborsRegressor(n_neighbors=n_count,algorithm = algorithm,weights = weight)
        #model = AdaBoostRegressor(n_estimators=n_estimators)
        model = model.fit(t_train, y_train)

        ti = np.linspace(self.min_lc_ratio, self.max_lc_ratio, self.levelsNum)[:, np.newaxis]
        y_model = model.predict(ti)
        y_model = np.around(y_model)

        predictedCols = {'Power': y_model,'ratio': ti[:, 0]}
        return predictedCols,quality

    def combineColumns(self,predictedDict,uniqueDict,predictedQuality):
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
    def noisyUser(df,col = 'Power',window = 9):
        """
        Define user with maximum signal noise.
        :param df: pd.DataFrame contains several user's and per one laccid  {pd.DataFrame}
        :param col: column to compute {'str'}
        :param window: rolling window length {'int'}
        :return: name of user {'str'}
        """
        maxNoise = 0
        noises = {}
        grouped = df.groupby(['User'])
        for user,gr in grouped:
            user_fltrd = pd.rolling_median(gr[col],window,center = True)
            noisy_part = gr[user_fltrd != gr[col]].shape[0]/float(gr.shape[0])
            if noisy_part > maxNoise:
                noisyUser = user
                maxNoise = noisy_part
            noises.update({user:noisy_part})
        return noisyUser,noises

    def rollingMean(self,df,col,noises,window = 6):
        FilteredDf = pd.DataFrame()
        grouped = df.groupby('User')
        for user,gr in grouped:
            _gr = gr.copy()
            if noises[user]>self.minNoise:
                _gr.loc[:,col] = pd.rolling_mean(_gr[col].interpolate(),window,center = True)
            FilteredDf = pd.concat([FilteredDf,_gr])
        return FilteredDf