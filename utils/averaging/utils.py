import os
from datetime import datetime
import numpy as np
import pandas as pd
class Utils():
    def __init__(self):
        return
    def unique(self,df,param,type = "all"):
        l = list(df[param].drop_duplicates())
        if type == "one":
            l = l[0]
        return l

    @staticmethod
    def fromCurrentTime(mark,ext):
        t = datetime.now()
        timename = str(t.year) + "-" + str(t.month) + "-" + str(t.day) + "-" + str(t.hour) + "-" + str(t.minute) + "-" + str(t.second)
        timename = timename + "-" + mark + ext
        return timename
    @staticmethod
    def initOutFld(flds,outDir):
        """
        make folders output folders
        :return:
        """
        # create output folder devided by device
        f = lambda fld: os.mkdir(outDir + "\\" + fld)
        # check if folder already exist
        e = lambda fld: os.path.isdir(outDir + "\\" + fld)
        # create folders if need
        [f(d) for d in flds if e(d) == False]
    @staticmethod
    def computeInterpStep(df):
        steps = []
        grouped = df.groupby(['race_id','User','laccid'])
        for ix,group in grouped:
            step = np.mean(pd.rolling_apply(group['ratio'],2,np.diff))
            steps.append(step)
        interpStep = np.mean(steps)
        return interpStep
