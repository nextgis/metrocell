from sklearn.ensemble import AdaBoostRegressor
from sklearn import neighbors
from sklearn import cross_validation
import pandas as pd
import numpy as np
import paths
from utils import Utils
import os
from preproc import Preproc
class Smooth():
    def __init__(self,unique_names):
        self.unique_names = unique_names
        self.algorithm = 'auto'
        self.weights = ['uniform','distance']
        self.ut = Utils()
        return
    def check_uniqueness(self,df,unique_names):
        unique_dict = dict.fromkeys(unique_names)
        for name in unique_names:
            unique_values = list(df[name].drop_duplicates())
            if len(unique_values)==1:
                unique_dict[name] = unique_values[0]
            else:
                raise ValueError("To many other unique values at field %s : %s"%(name,unique_values))
        return unique_dict

    def computeRaces(self,df):
        dict = {'NumRaces':None,'NumUsers':None}
        dict['NumRaces'] = len(self.ut.unique(df,'race_id'))
        dict['NumUsers'] = len(self.ut.unique(df,'User'))
        return dict
    def smooth(self,df,
            func = 'Power',
            min_lc_ratio = 0,
            max_lc_ratio = 1,
            levels_num = 10,
            test_size = 0.4):
        t = np.array(df['ratio'])
        y = np.array(df[func])
        size = len(t)
        t.shape = (-1, 1)

        if size<10:
            neigh = neighbors.KNeighborsRegressor(n_neighbors = size,algorithm = self.algorithm,weights = 'uniform')
            model = neigh.fit(t,y)
            quality = None
            weight = 'uniform'
        else:
            t_train, t_test, y_train, y_test = cross_validation.train_test_split(t, y, test_size=test_size, random_state=1)
            quality = best_q = -1000000
            best_params = (None, None)
            max_iter = min(len(t_train), 15)
            for n_count in range(1, max_iter):
                for w in self.weights:
                    model = neighbors.KNeighborsRegressor(n_neighbors = n_count,algorithm = self.algorithm,weights = w)
                    #model = AdaBoostRegressor(n_estimators = n_estimators)
                    model = model.fit(t_train, y_train)
                    quality = model.score(t_test, y_test)
                    if quality > best_q:
                        best_q = quality
                        best_params = n_count,w
            if best_params == (None, None):
                #print t_train, max_iter
                raise ValueError("No best parameters!")
            n_count,weight = best_params
            #print weight
            model = neighbors.KNeighborsRegressor(n_neighbors=n_count,algorithm = self.algorithm,weights = weight)
            #model = AdaBoostRegressor(n_estimators=n_estimators)
            model = model.fit(t_train, y_train)

        ti = np.linspace(min_lc_ratio, max_lc_ratio, levels_num)[:, np.newaxis]
        y_model = model.predict(ti)
        y_model = np.around(y_model)

        newCols = {'Power': y_model,'ratio': ti[:, 0]}
        unique_dict = self.check_uniqueness(df,self.unique_names)
        races_dict = self.computeRaces(df)
        unique_dict.update(races_dict)
        if quality!=None:
            quality = float("%.2f"%quality)

        unique_dict.update({'quality':quality,'weight':weight})
        for name in unique_dict.keys():
            unique_dict[name] = [unique_dict[name]]*levels_num
        newCols.update(unique_dict)
        df_aver = pd.DataFrame(newCols)
        return df_aver,quality

if __name__ == '__main__':

    _preproc = Preproc()
    ut = Utils()
    testDf = pd.io.parsers.read_csv(paths.preLogPointsPath)

    testDf = testDf[testDf['segment']=='119-118']

    unique_names = ['laccid','segment','NetworkType','NetworkGen']

    power = Smooth(unique_names = unique_names)
    aver_df = pd.DataFrame()

    quality = {}
    seg_laccid_list = list(testDf['laccid'].drop_duplicates())
    for laccid in seg_laccid_list:
        laccid_df = testDf[testDf['laccid']==laccid]
        netType_list = ut.unique(laccid_df,'NetworkType')
        for netType in netType_list:
            nettype_df = laccid_df[laccid_df['NetworkType'] == netType]

            smoothed_laccid_df,quality = power.smooth(nettype_df)
            aver_df = pd.concat([aver_df,smoothed_laccid_df])
            print (str(laccid) + "-" + str(netType) + ":" + str(quality))
    print (quality,aver_df)

