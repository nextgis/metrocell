__author__ = 'Alex'
import random,string
import numpy as np
from datetime import datetime
import pandas as pd
class Utils():
    def __init__(self):
        return

    @staticmethod
    def uniqueUnsorted(ar,returnNA = False):
        if returnNA == False:
            ar = ar[~np.isnan(ar)]
        uniqueIxs = np.unique(ar,return_index = True)[1]
        uniqueVals = [ar[index] for index in sorted(uniqueIxs)]
        return uniqueVals

    @staticmethod
    def dropMultipleCols(frame,columns):
        for col in columns:
            if col in frame.columns.values:
                frame.drop([col],axis = 1,inplace=True)
        return frame
    @staticmethod
    def checkId(frame):
        if frame['ID'].dtypes == 'object':
            frame.loc[frame['ID'] == 'ServiceLog','ID'] = np.nan
            frame['ID'] = frame['ID'].astype('float64')
        return frame

    @staticmethod
    def floatToInt(frame,columns):
        f = lambda x: '%.0f' % x
        g = lambda x : x.replace('nan','')
        for col in columns:
            frame[col] = frame[col].apply(f)
            frame[col] = frame[col].apply(g)
        return frame
    @staticmethod
    def generateRandomId():
        return random.randint(1,999999999)
    @staticmethod
    def placeStrBeforeType(path,val):
        t = string.split(path,".")
        s2 = t[0] + "_" + val + "." + t[1]
        return s2
    @staticmethod
    def insertColumns(frame,columns,value = np.nan):
        for col in columns:
            frame[col] = value
        return frame
    @staticmethod
    def getLocalTime(frame):
        localTime = datetime.fromtimestamp(int(frame.irow(0)['TimeStamp']/1000)).strftime("%Y%m%d%H%M")
        return localTime
    @staticmethod
    def dfTempate(marksFrame,columns = []):
        """
        Add columns
        :param marksFrame: frame with columns to copy
        :param columns: columns to add
        :return:
        """
        df = pd.DataFrame(columns = list(marksFrame.columns.values)+columns)
        return df