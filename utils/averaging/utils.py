import os
from datetime import datetime
class Utils():
    def __init__(self):
        return
    def unique(self,df,param,type = "all"):
        l = list(df[param].drop_duplicates())
        if type == "one":
            l = l[0]
        return l
    def getBoundaries(self,df,numOfPts):
        """

        :param df:
        :param numOfPts:
        :return:
        """
        dict = {}
        lc_ratios = list(df['ratio'])
        min_lc_ratio,max_lc_ratio = min(lc_ratios),max(lc_ratios)
        delta_ratios = max_lc_ratio - min_lc_ratio
        levels_num = int(delta_ratios*numOfPts)

        return min_lc_ratio,max_lc_ratio,levels_num
    @staticmethod
    def fromCurrentTime(ext):
        t = datetime.now()
        timename = str(t.year) + "-" + str(t.month) + "-" + str(t.day) + "-" + str(t.hour) + "-" + str(t.minute) + "-" + str(t.second)
        timename = timename + ext
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
