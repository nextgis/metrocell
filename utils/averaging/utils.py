import pandas as pd
class Utils():
    def __init__(self):
        return
    def unique(self,df,param):
        return list(df[param].drop_duplicates())
    def getBoundaries(self,df,numOfPts):
        lc_ratios = list(df['ratio'])
        min_lc_ratio,max_lc_ratio = min(lc_ratios),max(lc_ratios)
        levels_num = int(max_lc_ratio*numOfPts)
        return min_lc_ratio,max_lc_ratio,levels_num
