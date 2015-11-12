from utils import Utils
import pandas as pd
from numpy import unique,array
import os
class GenerateRaces():
    def __init__(self,interGraphPath,moveGraphPath,moveTypes,moveGraphClosedPath = None):
        self.moveGraph = self.getGraphs(moveGraphPath)
        if moveGraphClosedPath:
            self.moveGraph_closed = self.getGraphs(moveGraphClosedPath)
        else:
            self.moveGraph_closed = None
        self.interGraph = self.getGraphs(interGraphPath,
                                         id_from = 'station_from',
                                         id_to = 'station_to',
                                         reverse= False)

        self.rightSeq = [3,4,1,2]
        self.errors = ['sequence','stationId']
        self.errorsDfCols = ['ID','Name','inter','move','sequence','stationId','errorIndex']
        self.moveTypes = moveTypes
        return

    def getIdInfo(self,row):
        nextId,nextMark = None,None
        fullId = row.ID.values[0]
        idCheck = self.idCheck(fullId)
        if idCheck:
            nextId = int(str(fullId)[:-1])
            nextMark = int(str(fullId)[-1])

        return idCheck,nextId,nextMark,fullId
    def fullIdCheck(self,fullIdsSeq):
        check = True
        curSeq = Utils.uniqueUnsorted(fullIdsSeq,True)
        #if len(curSeq) == 4:
        seq = [(curSeq[0:2]),(curSeq[2:4])]
        try:
            ids = [[int(str(fullId[i])[:-1]) for i in range(0,2)] for fullId in seq]
        except:
            print ""
        uniqueStations = [len(unique(array(part))) for part in ids]
        if not uniqueStations == [1,1]:
            check = False

        #else:
        #    print ""
        return check
    def getGraphs(self,name,
                  id_from = 'id_from',
                  id_to = 'id_to',
                  reverse = True):
        df = pd.io.parsers.read_csv(name,sep = ';')
        graphs = df[id_from].apply(str) + "-" + df[id_to].apply(str)
        graphs = list(graphs)
        if reverse:
            graphsReverse = df[id_to].apply(str) + "-" + df[id_from].apply(str)
            graphs = graphs + list(graphsReverse)
        return graphs

    def splitByRace(self,marksFrame,indexes,pushErrors):
        """
        Split the markFile into the segments and write 'race_id' column with corresponding index into it
        :param marksFrame: raw markFile {pd.DataFrame}
        :param indexes: empty list if user is main(raw dirs contain zip-files, not sessions with several users)
                        or list containing the indexes , has been written into the marks column 'race_id' of main user frame {list}
        :return: marks   : source markFrame containing race_id column, checked for errors and write corresponding fields
                           of 'stop' and 'inter' into the "SomeID1[2]-SomeID1(or 2)[3]" parts. {pd.DataFrame}
                 indexes : list containing the indexes , has been written into the marks column 'race_id' of main user frame {list}
                 errorsDf: dataFrame containing error sections
        """
        marksFrame = Utils.insertColumns(marksFrame,self.moveTypes + ['stationId','sequence','race_id'])
        Stations = []
        stations = []
        errors = dict.fromkeys(self.errors,[])
        #if pushErrors:
        errorsDf = Utils.dfTempate(marksFrame)
        firstIter = False
        if len(indexes) ==0:
            firstIter = True
            indexes = []
        curId = -1
        numId = 0
        firstIx = 0
        step = 0
        seq = []
        fullIds = []
        falseIxs = []
        marks = marksFrame.copy()
        LastIx = marks.index[-1]
        for ix in marks.index:
            row = marks[marks.index == ix]
            idCheck,nextId,nextMark,fullId = self.getIdInfo(row)
            if not idCheck:
                errors['stationId'].append(ix)
                falseIxs.append(ix)
                continue
            if curId!=nextId:
                curId = nextId
                numId+=1
            if numId >=2:
                # if nextMark id the next segment (the user was moving forward)  or
                # it is the last index -- we have no stop section or interchange or
                if nextMark == 3 or ix == LastIx:
                    lastIx = ix -1
                    # if the last stamp
                    if ix == LastIx:
                        lastIx = ix
                        seq.append(nextMark)
                        fullIds.append(fullId)


                    # check a sequence errors
                    falseIxs,errors = self.checkSequences(errors,seq,fullIds,firstIx,ix,falseIxs)
                    truthIxs = range(firstIx,lastIx+1)
                    if errors['stationId']:
                        errors['stationId'] = truthIxs
                        falseIxs = truthIxs
                    truthIxs = self.excludeFalseVals(truthIxs,falseIxs)

                    if truthIxs:
                    #if not errors:
                        # process cell mark - generate random indexes
                        if firstIter:
                            # index as first TimeStamp
                            stamp = int(str(marks['TimeStamp'][firstIx])[:-3])
                            indexes.append(stamp)
                            marks.loc[truthIxs,'race_id'] = stamp
                        # use already generated indexes
                        else:
                            marks.loc[truthIxs,'race_id'] = self.generateId(ix = indexes[step])

                        step+=1
                        Stations.append((truthIxs,stations))
                    if sum(errors.values(),[]):
                        if pushErrors:
                            errors = {key:errors[key] for key in errors.keys() if errors[key]}
                            for error in errors.keys():
                                errorsDf = self.concatErrorSlice(marks,errorsDf,errors[error][0],errors[error][-1],error)
                    seq = []
                    stations = []
                    fullIds = []
                    numId = 1
                    firstIx = ix
                    falseIxs = []
                    errors = dict.fromkeys(self.errors,[])
            seq.append(nextMark)
            #fullCheck = self.checkFullSeq(seq,nextMark)
            stations.append(nextId)
            fullIds.append(fullId)

        # drop error indexes
        if pushErrors:
            if not errorsDf.empty:
                marks = marks.drop(errorsDf.index)
            # after removing error-rows need to mark as -1 all of the slices by template
            #  st1[2] -- ... removed slice...  -- st3[3]
            marks,moveErrorsDf,interErrorsDf = self.compareWithDict(marks,Stations)
            marks,errorsDf = self.concatErrorsDropRows(marks,errorsDf,moveErrorsDf,interErrorsDf)
        marks = Utils.dropMultipleCols(marks,['stationId','sequence'])
        return marks,indexes,errorsDf
    def excludeFalseVals(self,y,x):
        y_excluded_x = [y[i] for i in range(0,len(y)) if y[i] not in x]
        return y_excluded_x
    def concatErrorsDropRows(self,marks,errorsDf,moveErrorsDf,interErrorsDf):
        if not moveErrorsDf.empty:
            marks = marks.drop(moveErrorsDf.index)
            errorsDf = pd.concat([errorsDf,moveErrorsDf])
        if not interErrorsDf.empty:
            errorsDf = pd.concat([errorsDf,interErrorsDf])
        if not errorsDf.empty:
            errorsDf = errorsDf.drop_duplicates()
            errorsDf = errorsDf.loc[:,self.errorsDfCols]
            errorsDf = Utils.floatToInt(errorsDf,['ID','inter','move','sequence','stationId','errorIndex'])
        return marks,errorsDf
    def concatErrorSlice(self,marksFrame,errorsFrame,ix_from,ix_to,errorField):
         marksFrame.loc[ix_from:ix_to,errorField] = -1
         section = marksFrame.loc[ix_from:ix_to,:].copy()
         section.loc[:,'errorIndex'] = self.generateId()
         errorsFrame = pd.concat([errorsFrame,section])
         return errorsFrame
    def compareWithDict(self,marks,listOfIds):
        """
        compare sections with graphs from dictionary-frame.
        :param marks: dataFrame containing the 'race_id'  {pd.DataFrame}
        :param listOfIds: list contains tuples by template ([ix_from,ix_to],[mark-I[3],mark-I[4],mark-II[1],mark-II[2]])
        :return:marsDf :  updated marksFrame
                errorsDf: dataFrame containing error sections
        """
        marksDf = marks.copy()
        moveErrorsDf = Utils.dfTempate(marksDf,columns = ['errorIndex'])
        interErrorsDf = Utils.dfTempate(marksDf,columns = ['errorIndex'])
        # check errors of move-part
        lOfUniqueIds = [(listOfIds[i][0],Utils.uniqueUnsorted(listOfIds[i][1],returnNA = True)) for i in range(0,len(listOfIds))]
        moveErrorsIxs = [(lOfUniqueIds[i][0]) for i in range(0,len(lOfUniqueIds)) if
                         not self.compareWithGraph(lOfUniqueIds[i][1],self.moveGraph,closedGraph= self.moveGraph_closed)]
        # write them into the errors-dataFrame
        if moveErrorsIxs:
            for ixs in moveErrorsIxs:
                moveErrorsDf = self.concatErrorSlice(marksDf,moveErrorsDf,ixs[0],ixs[-1],'move')
        # check interchanges-part and write '1' value into the corresponding column
        for i in range (1,len(lOfUniqueIds)):
            stopOrInterIds = (lOfUniqueIds[i-1][1][1],lOfUniqueIds[i][1][0])
            stopOrInterIxs = (lOfUniqueIds[i-1][0][-1],lOfUniqueIds[i][0][0])
            if not stopOrInterIds[0] == stopOrInterIds[1]:
                check = self.compareWithGraph(stopOrInterIds,self.interGraph)
                if not check:
                    interErrorsDf = self.concatErrorSlice(marksDf, interErrorsDf, stopOrInterIxs[0], stopOrInterIxs[1], 'inter')
                else:
                    #something strange!check!
                    if (stopOrInterIxs[1]-stopOrInterIxs[0]==1):
                        marksDf.loc[stopOrInterIxs[0]:stopOrInterIxs[1],'inter'] = 1
            else:
                marksDf.loc[stopOrInterIxs[0]:stopOrInterIxs[1],'stop'] = 1
        return marksDf,moveErrorsDf,interErrorsDf
    def addErrorFields(self,frame,dict = {}):
        """
        add error-fields into the frame
        :param frame: {pd.DataFrame}
        :param errors: {list}
        :return:
        """
        #if not 'errorIndex' in frame.columns.values:
        #    frame = Utils.insertColumns(frame,['errorIndex'])
        for error in dict.keys():
            frame[error] = dict[error]
            frame.loc[:,'errorIndex'] = self.generateId()
        return frame
    def idCheck(self,fullId):
        """
        check if id is valid
        :param fullId: stationID + mark {int}
        :return: {boolean}
        """
        check = True
        if fullId == -1:
            check = False
        return  check
    def checkSequences(self,errors,seq,fullIds,firstIx,lastIx,falseIxs):
        seqFullCheck,errors = self.seqCheck(seq,firstIx,lastIx,errors)
        if seqFullCheck:
            fullIdCheck = self.fullIdCheck(fullIds)
            if not fullIdCheck:
                errors['stationId'] = range(firstIx,firstIx+4)
        falseIxs = sorted(list(set(falseIxs+sum(errors.values(),[]))))
        return falseIxs,errors

    def seqCheck(self,_seq,firstIx,lastIx,errors):
        """
        compare the equal order between current and truth sequences
        :param seq: sequence to check  {list}
        :return: true if equal {boolean}
        """

        fullCheck = True
        seq = [None] + _seq
        curSeq = [seq[i] for i in range(1,len(seq)) if seq[i]!=seq[i-1]]
        #curSeq = Utils.uniqueUnsorted(seq,True)

        if not curSeq == self.rightSeq:
            errors['sequence'] = range(firstIx,lastIx)
            fullCheck = False
        return fullCheck,errors
    def compareWithGraph(self,stationsId,graph,closedGraph = None):
        """
        find local graph into the dictionary-dataFrame
        :param stationsId: station-from ,station-to identifiers
        :param graph: graph dataFrame {pd.DataFrame}
        :return: true if founded {boolean}
        """
        # len(stationsId) might be equal to 1 if it is an error on the start or ont the end of marksFrame
        if len(stationsId) == 2:
            check = True
            stGraph = str(stationsId[0]) + "-" + str(stationsId[1])
            if stGraph not in graph:
                if closedGraph:
                    if stGraph not in closedGraph:
                        check = False

        else:
            check = False
        return check
    def generateId(self,**kwargs):
        """
        generate identifier (random or pull it from the list)
        :param kwargs:
        :return: identifier{int}
        """
        if not kwargs:

            id = Utils.generateRandomId()
        else:
            id = kwargs['ix']
        return id
