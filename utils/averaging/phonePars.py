import paths
import pandas as pd
import numpy as np
import math
from preproc import Preproc
from utils import Utils
from os import path
from plot import Plot
class PhonePars():
    def __init__(self):
        self.ut = Utils()
        return

    def phonePars(self,userFrame):
        nCount = self.neighboursCount(userFrame)
        unusuals = self.unusuals(userFrame)

    def neighboursCount(self,userFrame):
        """
        The number of neighbours at the same moment

        :param userFrame:
        :return:
        """
        return 1

    def stats(self,Df,param = 'Power'):
        """
        Generating simple statistics(mean power by station:laccid:phone:race_id) for each phone in the table Df.
        :param Df: "Stop" dataframe represented logs taken at the stations
        :param param: paramenter for analysis
        :return: stats-dataframe
        """
        segments = self.ut.unique(Df,'segment')
        SegDict = dict.fromkeys(segments)
        for seg in segments:
            #loop by segments
            segDf = Df[Df['segment'] == seg]
            laccids = self.ut.unique(segDf,'laccid')
            LcDict = dict.fromkeys(laccids)
            SegDict[seg] = LcDict
            for lc in laccids:
                #loop by laccids
                SegDict[seg][lc] = {}
                lcDf = segDf[segDf['laccid'] == lc]
                #ListOfLaccids = list(segDf.loc[segDf['laccid'] == lc,param])
                #stdev = sqrt(1/n*sum(Xi - Xmean)), i:1..n
                #SegDict[seg][lc]['stdev'] = np.std(ListOfLaccids)
                phones = self.ut.unique(lcDf,'User')
                goodPhones = dict.fromkeys(phones)
                badPhones = dict.fromkeys(phones)
                for phone in phones:
                    #loop by phones
                    #list of parameters
                    stats = ['stdev','mean','weight','system','meas']
                    PhoneDf = lcDf[lcDf['User'] == phone]
                    race_ids = self.ut.unique(PhoneDf,'race_id')
                    goodPhones[phone] = race_ids
                    badPhones[phone] = []
                    SegDict[seg][lc][phone] = {}
                    #SegDict[seg][lc][phone] = dict.fromkeys(race_ids)
                    for id in race_ids:
                        #loop by race_ids. Race_id is the identifier of each race of user
                        SegDict[seg][lc][phone].update({id:dict.fromkeys(stats)})
                        RaceDf = PhoneDf[PhoneDf['race_id'] == id]
                        valsByParam = list(RaceDf[param])
                        stdev = np.std(valsByParam)
                        meas = len(valsByParam)
                        mean = np.mean(valsByParam)
                        SegDict[seg][lc][phone][id]['meas'] = meas
                        SegDict[seg][lc][phone][id]['stdev'] = stdev
                        SegDict[seg][lc][phone][id]['mean'] = mean
                        # if the standart deviation equal to zero -->the lack of measurements
                        if stdev == 0.0:
                            badPhones[phone].append(id)
                            weight = float('Inf')
                        else:
                            weight = 1.0/math.pow(stdev,2)
                        SegDict[seg][lc][phone][id]['weight'] = weight
                # extraction id's with stdev == 0.0 from the next calculations
                [[ goodPhones[phone].remove(id) for id in badPhones[phone]]for phone in badPhones]
                # calculation of summary weight for each laccid. P = sum(p(phone,id)).
                # TRICK: sum([1,23,[27]], []) --> [1,23,27]
                P_sum = sum(sum(([[SegDict[seg][lc][phone][id]['weight'] for id in goodPhones[phone]] for phone in goodPhones]),[]))

                if (P_sum!=0.0) and (len(goodPhones)>=2):
                    #calculating the averaging of parameter = (p1*A1+p2*A2+...+piAi)/P_sum.
                    lc_P_mean = sum(sum([[SegDict[seg][lc][phone][id]['weight']*SegDict[seg][lc][phone][id]['mean'] for id in goodPhones[phone]] for phone in goodPhones],[]))/P_sum
                    for phone in goodPhones:
                        # calculation of systematic error. Note that it would really be only if the measurements
                        # were executed at the same points of station, at the same
                        systems = {id:(lc_P_mean - SegDict[seg][lc][phone][id]['mean']) for id in goodPhones[phone]}
                        for id in goodPhones[phone]:
                            SegDict[seg][lc][phone][id]['system'] = systems[id]
                else:
                    SegDict[seg].pop(lc)

        # saving the output daraframe. Dictionary to dataframe conversion
        df = pd.DataFrame([[col1,col2,col3,col4,col5,col6] for col1,a in SegDict.items()
                           for col2,b in a.items()
                           for col3,c in b.items()
                           for col4,e in c.items()
                           for col5,col6 in e.items()],columns = ['Segment','lc','user','race_id','parameter','value'])
        # extraction values from "parameter" column and write them into the header.
        ndf = pd.tools.pivot.pivot_table(df,values = 'value', index = ['Segment','lc','user','race_id'], columns = ['parameter'])
        ndf.to_csv(path.join(paths.saveStatsFolder,str(param)+".csv"))
        return ndf


    def pulledMeasures(self,userFrame):
        """

        :return:
        """
        constantPower = self.constantPower(userFrame)

    def unusuals(self,userFrame):
        """
        :return:
        """
        return
    def constantPower(self,userFrame):
        return
    def evaluation(self):
        return

if __name__ =='__main__':
    stopDf = pd.io.parsers.read_csv(paths.inputStopsPath)
    preproc_ = Preproc()
    #plot_ = Plot()
    stopDf = preproc_.proc_cell_df(stopDf,users = ['sasfeat'])
    phonePars = PhonePars()
    ndf = phonePars.stats(stopDf,param = 'Power')
    #plot_.stats(ndf)





