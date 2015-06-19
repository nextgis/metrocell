#coding=utf-8
__author__ = 'Alex'
from geopy.distance import vincenty
#import numpy as np
import pandas as pd
import numpy as np
#import paths
from posAlgorithm import PosAlgorithm

class Estimator():
    @staticmethod
    def byLatLong(predictedDf,truthPoint):
        test = pd.DataFrame()
        truth = zip(truthPoint['x'],truthPoint['y'])
        test['xy'] = zip(predictedDf['x'],predictedDf['y'])
        distances = test.apply(lambda df:vincenty(truth,df['xy']).meters,axis = 1)
        error = max(distances)
        return error
if __name__ =="__main__":
    #Algorithm 1 : Random
    iters = 50
    errors = []
    for i in range(1,iters):
        randomAlg = PosAlgorithm()
        randomAlg.randomSampling(randomAlg.SmoothedDf)
        #error
        error= Estimator.byLatLong(randomAlg.predicted_df,randomAlg.truthPoint)
        errors.append(error)
    print("###Random###")
    print ("Mean maxima-error is " + str(np.mean(errors)) + " meters")
    #Algorithm 2: By LACCID
    errors = []
    for i in range(1,iters):
        byLaccid = PosAlgorithm()
        byLaccid.byLacCid()
        #error
        error = Estimator.byLatLong(byLaccid.predicted_df,byLaccid.truthPoint)
        errors.append(error)
    print("###By LACCID###")
    print ("Mean maxima-error is " + str(np.mean(errors)) + " meters")
