# The content of tables
##### Note! Smoothed means that data was processed with smoothed database. No postfix raw data was used.  
### _estimDf.csv_

_alg_ : prediction algorithm [ 'lc','lcM','pc-union','pc-intersection','pc-maxCorrMinDelta' ]

__'lc'__  - by LAC-CID-NetworkType-NetworkGen

__'lcM'__ - by LAC-CID-NetworkType-NetworkGen and neighbours

__'pc'__ - by Power's Correlation. "union,intersection,maxCorrMinDelta" mean postprocessing method.
 
_error_ : maximum distance from current point to the most remote predicted point [0,...)

_falseSeg_: the number of false predicted segments [0,...)

_trueSeg_ : the number of true predicted segments [0,1]

_iter_ : iteration number. One iteration - one imitation of prediction [1,...)

_unpr_ : __1__ - the algorithm has not worked. __0__ - the algorithm has worked [0,1]

#### _PredictedD.csv_

###### The structure is the same as at the georeferenced DataFrame. But some columns were added:

_union, intersection, maxCorrMinDelta_  : __1__ - if row were processed by appropriate algorithm. [1]. 

_iter_ : iteration number. One iteration - one imitation of prediction [1,...)

_method_ : method of data process. ['byAbs','byCorr']

__'byAbs'__ - if variance of values of Power is about equal to zero. 

__'byCorr'__ - the coefficient of correlation

_controls_ : the point is "control". The maxima of correlation coefficient or minimum delta Powers 
among this LAC-CID-NetworkType-NetworkGen [0,1]

_coeffs_ : coefficent of correlation[0,1] or deltaPower [0,....)

