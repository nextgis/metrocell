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
    def computeRaces(self,df):
        dict = {'NumRaces':None,'NumUsers':None}
        dict['NumRaces'] = len(self.ut.unique(df,'race_id'))
        dict['NumUsers'] = len(self.ut.unique(df,'User'))
        return dict
    def check_uniqueness(self,df,unique_names):
        i=0
        unique_dict = dict.fromkeys(unique_names)
        unique_dicts  = {0:unique_dict}
        ununique_dict = {}
        for name in unique_names:
            unique_values = list(df[name].drop_duplicates())
            if len(unique_values)!=1:

                ununique_dict[name] = unique_values
        return ununique_dict