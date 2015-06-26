__author__ = 'Alex'
import pandas as pd
import numpy as np
import paths
#postprocessing of powerCorrelation algorithm
Df = pd.io.parsers.read_csv(paths.CorrResultsDf)

byAbs = Df[Df['type'] =='byAbs']
byCorr = Df[Df['type'] =='byCorr']
corrcoeffs = byCorr.groupby(['iter','predSegment','lc','index']).apply(lambda x: np.corrcoef(x['predPowers'],x['truePowers'])[0,1])
deltaPowers = byAbs.groupby(['iter','predSegment','lc','index']).apply(lambda x: abs(np.mean(x['predPowers']) - np.mean(x['truePowers'])))
predDeltas = deltaPowers[deltaPowers<0.001]