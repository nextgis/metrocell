import pandas as pd
import geopy
class Utils():
    def __init__(self):
        return
    def unique(self,df,param,type = "all"):
        l = list(df[param].drop_duplicates())
        if type == "one":
            l = l[0]
        return l
    def getBoundaries(self,df,numOfPts):
        dict = {}
        lc_ratios = list(df['ratio'])
        min_lc_ratio,max_lc_ratio = min(lc_ratios),max(lc_ratios)
        delta_ratios = max_lc_ratio - min_lc_ratio
        if delta_ratios>0.2:
            dict['min_lc_ratio'] = min_lc_ratio
            dict['max_lc_ratio'] = max_lc_ratio
            dict['levels_num'] = int(max_lc_ratio*numOfPts)
        return dict
