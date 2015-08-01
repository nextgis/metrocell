__author__ = 'Alex'

 def byPowerCorr(self,
                    useSmoothed = False):
        """
        The input segment should contains varying of signal. Only in this case
        Suppose that user's telephone grabbed not only the base station but neighbours too
        it is possible to identify truth position
        :return: predicted data frame.
        """
        self.unpredicted = 0
        self.resultsDf = pd.DataFrame()
        predictedDf = pd.DataFrame()
        # dataFrame contained control Rows.

        ReducingTypes = {'byAbs':'maxLimit','byCorr':'localMaxima'}
        resIndex = 0

        # 1. Split phone data on base step's sections.

        if useSmoothed  ==True:
            self.interpPowers = self.grabbedDf.groupby(['laccid'])['Power'].apply(list).to_dict()
        else:
            self.interpolateByTimeStep()
        # 2. Compare powers of grabbed log with powers from database

        # a) If the variance of grabbed log close to zero --> compare Mean by list of absolute Power values.
        # b) Else --> compare the coefficients of correlation
        #       If corrCoeff < 0 : extract this indexes from predicted dataFrame
        #       If corrCoeff > 0 : find local maximums at the list of corrCoeffs and
        #                          extract all of the others from predicted dataFrame

        absMeans = self.analyzeLC()
        # Extract indexes iteratively
        powersDf = self.predictedDf.groupby(['segment','laccid'])
        for ((seg,lc),SegLcGroup) in powersDf:
            predictedIndexes = {'byAbs':[],'byCorr':[]}
            i = 0
            # window wide

            if lc in absMeans.keys():
                method = 'byAbs'
            else:
                method = 'byCorr'
            powers = self.interpPowers[lc]
            self.movingAverage(powers,)
            ww = len(self.interpPowers[lc])
            while (i+ww)<=SegLcGroup.shape[0]:
                resIndex+=1
                # move along indexes at the database and compare section from it
                # with grabbed section
                grabSection = np.array(SegLcGroup[i:i+ww]['Power'])
                grabIndexes = np.array(SegLcGroup[i:i+ww].index)
                # Check if the signal is in list of constant signals
                # if yes --> compute deltaPower
                if method =='byAbs':
                    # check the variance
                    # if variance close to zero --> compare absolute Powers. Else --> step over
                    fQua = np.percentile(grabSection,q = self.fQua)
                    lQua = np.percentile(grabSection,q = self.lQua)
                    deltaQua = lQua-fQua
                    if deltaQua < self.deltaQua:
                    #lcVariance = np.var(grabSection)
                    #if lcVariance< self.minVariance:
                        # delta Power
                        coeff = np.median(grabSection)
                        # coeff = abs(absMeans[lc] - np.mean(grabSection))
                        predictedIndexes['byAbs'].append(([grabIndexes[0],grabIndexes[-1]],coeff))
                # if no --> compute the correlation
                else:
                    coeff = np.corrcoef(grabSection,self.interpPowers[lc])[0,1]
                    # around 0 - 0.5 else --> no correlation.Else --> append to the list
                    if coeff > self.minCorrcoeff:
                        predictedIndexes['byCorr'].append(([grabIndexes[0],grabIndexes[-1]],coeff))

                resDict = {'index'          :   resIndex,
                           'trueSegment'    :   self.trueSegment,
                           'predSegment'    :   seg,
                           'lc'             :   lc,
                           'predPowers'     :   grabSection,
                           'truePowers'     :   self.interpPowers[lc],
                           'type'           :   method
                            }
                resRow = pd.DataFrame.from_dict(resDict)
                self.resultsDf = pd.concat([self.resultsDf,resRow])
                # step forward
                i+=self.windowStep
            # extract indexes and append them to the main list
            extractedInfo = self.processPC(predictedIndexes[method],method = method,redType = ReducingTypes[method])
            if extractedInfo.empty!=True:
                extractedIxs = np.array(extractedInfo.index)
                predictedGroup = SegLcGroup[SegLcGroup.index.isin(extractedIxs)]
                predictedGroup = pd.concat([predictedGroup,extractedInfo],axis = 1)
                predictedDf = pd.concat([predictedDf,predictedGroup])
                #print predictedGroup
        if predictedDf.empty != True:
            #print predictedDf.columns.values
            controlCheck = 'controls' not in predictedDf.columns.values
            if controlCheck == True:
                print ""

            self.predictedDf = predictedDf
            #self.controlDf = controlDf
        else:
            self.unpredicted = 1
                # write result into the table
