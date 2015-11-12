__author__ = 'Alex'
import os,sys
from argparse import ArgumentParser
from dirProcesser import DirProcesser
import pandas as pd
import numpy as np
from generateRaces import GenerateRaces
from bringing_to_single_time import BringingToSingleTime
from utils import Utils

class PrepareAndSplit():
    def __init__(self,bring,
                 inputRawDir,
                 outputProcDir,
                 interchangesPath,
                 moveGraphPath,
                 moveGraphClosedPath,
                 mainUser = None
                 ):
        #self.availableDevices = ['cell','sensor','external']
        self.availableDevices = ['cell','sensor']
        self.moveTypes = ['move','stop','inter']

        self.inputRawDir = inputRawDir


        self.bring = bring
        self.generator = GenerateRaces(interchangesPath,moveGraphPath,self.moveTypes,moveGraphClosedPath)
        # use mainUser variable to initialize folder which contains the marks
        self.dirProcesser = DirProcesser(mainUser=mainUser,moveTypes=self.moveTypes)
        #self.toParse = toParse
        self.outputProcDir= outputProcDir
        self.errorsDir = self.outputProcDir + "\\" + 'errors'
        if self.bring:
            self.bringing = BringingToSingleTime()
        self.initOutFlds()
        # let's go!
        self.loopRaws()

        return

    def updateMarks(self,marksPath,indexes,pushErrors = True):
        """
        Read,process, write into and save the markFile
        :param marksPath: path tot the markFile
        :param indexes: empty list if user is main(raw dirs contain zip-files, not sessions with several users)
        or list containing the indexes , has been written into the marks field 'race_id' of main user {list}

        :return: marksFrameIx : updated marksFrame {pd.DataFrame}
                 indexes: list containing the indexes , has been written into the marks field 'race_id'
                 errorsDf: dataFrame containing errors segments
        """
        marksFrame = pd.io.parsers.read_csv(marksPath,sep = ";")
        marksFrameIx,indexes,errorsDf = self.generator.splitByRace(marksFrame,indexes,pushErrors)
        # if the last marks have no race_id its will be excluded from the output dataFrame.
        # It may be if user has been forgotten to mark the station he pulled in.
        marksFrameIx = marksFrameIx[~np.isnan(marksFrameIx['race_id'])]
        marksFrameIx.loc[:,'race_id'] = marksFrameIx.loc[:,'race_id'].astype('int64')
        marksFrameIx.to_csv(marksPath,sep = ';',index = False)
        return marksFrameIx,indexes,errorsDf

    def concatAndCut(self,logPath,marksFrameIx):
        """
        Concatenate log and marks dataFrames and cut sections between moments when user has been pressed button "start logger"
        and written into the markFile the first mark.
        :param logPath: path to the log {pd.DataFrame}
        :param marksFrameIx: marksFrame containing 'race_id' identifier{pd.DataFrame}
        :return:
        """
        logFrame = pd.io.parsers.read_csv(logPath,sep = ";")
        logFrame = Utils.insertColumns(logFrame,columns = self.moveTypes + ['race_id'] )
        fullFrame = pd.concat([logFrame,marksFrameIx])
        # fullFrame.ID = fullFrame.ID.astype('int64')
        fullFrame = fullFrame.sort(['TimeStamp','inter','stop'])
        fullFrame = fullFrame.set_index([range(0,len(fullFrame))])
        s = pd.Series(list(fullFrame['ID']))
        firstIx = s.first_valid_index()
        lastIx = s.last_valid_index()
        fullFrame = fullFrame.loc[firstIx:lastIx,:]
        return fullFrame
    def initOutFlds(self):
        """
        make folders output folders
        :return:
        """
        # create output folder devided by device
        f = lambda fld: os.mkdir(self.outputProcDir + "\\" + fld)
        # check if folder already exist
        e = lambda fld: os.path.isdir(self.outputProcDir + "\\" + fld)
        # create folders if need
        flds = self.availableDevices + ['errors']
        [f(d) for d in flds if e(d) == False]
    def breakOnSections(self,fullFrame,device):
        """
        Split full DataFrame on the move,stop (or inter) sections
        :param fullFrame: dataFrame containing binded and sorted marks and log frames{pd.DataFrame}
        :param device: what type of device need to parse {str}
        :return:
        """
        marksInLog = fullFrame[~fullFrame['race_id'].isnull()]
        marksIxs = marksInLog.groupby('race_id').apply(lambda x : list(x.index))
        for i in range(1,marksIxs.shape[0]):
            prev = marksIxs.irow(i-1)
            next = marksIxs.irow(i)
            fullFrame.loc[prev[0]:prev[-1],'move'] = 2
            fullFrame.loc[prev[0]:next[0],'race_id'] = marksIxs.index[i-1]
            sectionFrame = fullFrame.loc[prev[0]:next[0],:].copy()
            self.parseSectionAndDrop(sectionFrame,device)

    def extractStops(self,section):
        """

        :param section: dataFrame containing one race with marks : stI(3-4)-stII(1-2-3) {pd.DataFrame}
        :return:sectionFrame : dataFrame containing "2" value at the corresponding "stop" or "inter" column  {pd.DataFrame}
        """
        sectionFrame = section.copy()
        for stopType in ['stop','inter']:
            na = self.isempty(sectionFrame,stopType)
            if not na:
                ix_to = sectionFrame.loc[:,stopType].last_valid_index()
                ix_from = sectionFrame.loc[:ix_to-1,stopType].last_valid_index()
                sectionFrame.loc[ix_from:ix_to,stopType] = 2
        return sectionFrame
    def checkFullSlice(self,ixs):
        check = True
        for ix in ixs:
            if ix in [None,-1]:
                check = False
        return check

    def parseSectionAndDrop(self,sectionFrame,device):
        """
        Parse sectionFrame and drop parsed files into the end directory
        :param sectionFrame:  dataFrame containing one race with marks : stI(3-4)-stII(1-2-3) {pd.DataFrame}
        :param device: what type of device need to parse {str}
        :return:
        """
        sectionFrameUpdated = self.extractStops(sectionFrame)
        for moveType in self.moveTypes:
            typeDf = sectionFrameUpdated[sectionFrameUpdated[moveType] == 2]
            if not typeDf.empty:
                Ids = self.stationsId(typeDf)
                self.dirProcesser.put_in_tidy(typeDf,device,Ids,moveType,self.outputProcDir)

    def isempty(self,frame,col):
        """
        check if column of given frame contains only NA-values
        :param frame: {pd.DataFrame}
        :param col: column {str}
        :return:
        """
        check = False
        valsNA = frame[col].unique()
        vals = valsNA[~np.isnan(valsNA)]
        uniqueRows = frame[frame[col].isin(vals)]
        uniqueRows = uniqueRows[~uniqueRows[col].isin([-1])]
        if not uniqueRows.shape[0]>1:
            check = True
        return check
    def stationsId(self,sectionFrame):
        """
        extract the first and the last identifiers of stations from given sectionFrame
        :param sectionFrame: section of segment(move(3-4-1-2),stop or inter(3-4) ) with { pd.DataFrame}
        :return: identifiers {dict}
        """
        firstIx = sectionFrame.ID.first_valid_index()
        lastIx = sectionFrame.ID.last_valid_index()
        # .zfill(3)
        id_from=str(int(sectionFrame['ID'][firstIx]))[:-1]
        id_to=str(int(sectionFrame['ID'][lastIx]))[:-1]
        ids = {'from':id_from,'to':id_to}
        return ids

    def loopMarks(self,row,devices,indexes = [],pushErrors = False):
        """
        Loop through the marks into the user's folder
        :param row: row from the dataFrame, containing paths to all of the marks {pd.DataFrame}
        :param devices: what type of devices folder is contains {list}
        :param indexes: empty list if user is main(raw dirs contain zip-files, not sessions with several users)
        or list containing the indexes , has been written into the marks of main user {list}
        :param pushErrors: True if need to write the errors into the dictionary {Boolean}
        :return:
        """
        errors = {}
        # check what type of devices(cell,sensor,external) has been grabbed
        userSession = DirProcesser.lastFld(row['cellLog'],pos = -2)

        for device in devices:
            sys.stdout.write("\t"+ userSession + " : " + device)
            sys.stdout.flush()

            logPath = row[device + "Log"]
            marksPath = row[device + "Marks"]
            marksFrameIx,indexes,errorsDf = self.updateMarks(marksPath,indexes,pushErrors)
            sortedFullFrame = self.concatAndCut(logPath,marksFrameIx)
            self.breakOnSections(sortedFullFrame,device)
            if pushErrors:
                if not errorsDf.empty:
                    errorsName = userSession + ".csv"
                    errors.update({errorsName:errorsDf})
                #pushErrors = False

        return indexes,errors

    def loopRaws(self):
        """
        loop through the raw zip files.
        :return:
        """
        step = 0
        files = os.listdir(self.inputRawDir)
        length = len(files)
        for f in files:
            step+=1
            unpacked = []
            sys.stdout.write("\r" + str(step) + "/" + str(length) + " : ")
            sys.stdout.flush()
            sessionFolder = self.inputRawDir + "\\" + f

            # check if number of phones > 1(if user was grabbing logs from more then 1 phone)
            if self.bring:
                sessionUsersData = {}
                usersSessions = os.listdir(sessionFolder)
                sessionId = Utils.generateRandomId()
                # loop through users
                for userSessionName in usersSessions:
                    userSessionName = self.dirProcesser.unzipUserSession(sessionFolder,userSessionName)
                    unpacked.append(userSessionName)
                    sessionUserData = self.dirProcesser.getSessionData(userSessionName)
                    sessionUsersData.update(sessionUserData)
                usersFrame = pd.DataFrame(sessionUsersData).transpose()
                userRow = usersFrame[usersFrame['mainUser'] == 1].irow(0)
                devices = DirProcesser.ifExists(userRow)
                self.bringing.writeSessionSaveFrames(userRow,sessionId,devices)
            else:
                sessionName = self.dirProcesser.unzipUserSession(self.inputRawDir,f)
                unpacked.append(sessionName)
                sessionData = self.dirProcesser.getSessionData(sessionName)
                userRow = pd.DataFrame(sessionData).transpose().irow(0)
                devices = DirProcesser.ifExists(userRow)
            # only after this step we can start copy marks
            mainIds,errors = self.loopMarks(userRow,devices,pushErrors= True)

            if self.bring:
                # usersFrame contains only the paths. let's start to copy the marks from main User folder.
                usersFrameApdated = self.bringing.processFrames(usersFrame,userRow)
                # split other users on sections
                otherUsersFrame = usersFrameApdated[usersFrameApdated['mainUser']==0]

                for index,row in otherUsersFrame.iterrows():
                    devices = eval(row['devices'])
                    self.loopMarks(row,devices,mainIds)
            # save errors in the markFiles
            self.dirProcesser.pushErrors(errors,self.errorsDir)
            # remove unzipped folders
            self.dirProcesser.removeFlds(unpacked)

def main():

    parser = ArgumentParser(description='')
    parser.add_argument('-i', '--interchangesPath',type = str,required = True,help = 'Path to interchanges dataFrame')
    parser.add_argument('-m', '--moveGraphPath',type = str,required = True,help = 'Path to moves graph dataFrame')
    parser.add_argument('-mC','--moveGraphClosedPath',type = str,required = False,help = 'Path to graph with closed stations')
    parser.add_argument('-u', '--mainUser', type = str, help = 'The user whose logs contain markFiles')
    parser.add_argument('-b', '--bring',action = 'store_true',help = 'bring to single time or not')
    parser.add_argument('inputRawDir', help = 'input directory containing zip-files of sessions or session-folders ')
    parser.add_argument('outputProcDir',type = str, help = "The output directory to save adapted zip-files.")
    args = parser.parse_args()
    return args
if __name__ =='__main__':
    args = main()
    PrepareAndSplit(inputRawDir = args.inputRawDir,
                    bring = args.bring,
                    mainUser = args.mainUser,
                    outputProcDir = args.outputProcDir,
                    interchangesPath = args.interchangesPath,
                    moveGraphPath = args.moveGraphPath,
                    moveGraphClosedPath = args.moveGraphClosedPath
                    )