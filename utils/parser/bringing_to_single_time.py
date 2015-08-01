#coding=utf-8
import random
import pandas as pd
from dirProcesser import DirProcesser
class BringingToSingleTime():
    def __init__(self):
        return
    def writeSessionSaveFrames(self,row,sessionId,devices,**kwargs):
        """
        write session_id into the marks- and log- files and rewrite them.
        :param row: row from the dataFrame, containing paths to all of the marks {pd.DataFrame}
        :param sessionId: identifier of current session {int}
        :param devices: what type of devices folder is contains {list}
        :param kwargs:
        :return:
        """
        for device in devices:
            userFrames = {}
            logPath = row[device + "Log"]
            marksPath = row[device + "Marks"]
            userFrames[logPath] = pd.io.parsers.read_csv(logPath,sep = ";")
            if not kwargs:
                userFrames[marksPath] = pd.io.parsers.read_csv(marksPath,sep = ";")
            else:
                userFrames = self.generateMarks(logPath,marksPath,kwargs['mainUserFirstStamp'],kwargs['mainCellMarkFile'])
            userFramesIdent = self.writeId(sessionId, Dfs = userFrames)
            self.saveFrames(userFramesIdent)

    def processFrames(self,usersFrame,mainRow):
        """
        :sessionId: identifier of session
        :param usersFrame: frame containing paths to all of the files of user session
        :return: CSV markFiles
        """
        # init  log and mark frames

        mainCellLogFilePath = mainRow['cellLog']
        mainCellMarkFilePath = mainRow['cellMarks']
        mainCellLogFile = pd.io.parsers.read_csv(mainCellLogFilePath,sep = ";")
        mainCellMarkFile = pd.io.parsers.read_csv(mainCellMarkFilePath,sep = ";")

        # use only the "base station" marks!
        mainCellMarkFile = mainCellMarkFile[mainCellMarkFile['Active'] == 1]
        #self.writeSessionSaveFrames(mainRow,sessionId)
        mainUserFirstStamp = mainCellLogFile['TimeStamp'][0]
        otherUsers = usersFrame[usersFrame['mainUser'] == 0].index
        sessionId = mainCellMarkFile.session_id.unique()[0]
        for user in otherUsers:
            userRow = usersFrame[usersFrame.index == user].irow(0)
            # what type of devices the user has been grabbed
            devices = DirProcesser.ifExists(userRow)
            usersFrame.loc[user,'devices'] = str(devices)
            self.writeSessionSaveFrames(userRow,sessionId,devices,
                            mainUserFirstStamp = mainUserFirstStamp,
                            mainCellMarkFile = mainCellMarkFile)
        return usersFrame

    def generateMarks(self,logPath,marksPath,mainUserFirstStamp,mainCellMarkFile):
        """
        Generation of marks base on the fact that user started grabbing the data from several phones at ones.
         So, if we know the delta times between 'main' and any other phone then we can create marks for them using
         the simple math.
        :param logPath: user's logPath to read || {str}
        :param marksPath: user's marksPath to write || {str}
        :param mainUserFirstStamp: main user's first TimeStamp from log  {int}
        :param mainCellMarkFile:  main user's markFile {pd.DataFrame}
        :return: userFrames: key = path{str}: value = dataFrame {pd.DataFrame} || {dict}
        """
        userFrames = {}
        userFrames[logPath] = pd.io.parsers.read_csv(logPath,sep = ";")
        # compute the delta time
        userFirstStamp = userFrames[logPath]['TimeStamp'][0]
        deltaStamps = mainUserFirstStamp - userFirstStamp
        marksStamps = mainCellMarkFile[['ID','Name','User','session_id','race_id']].copy()
        marksStamps['TimeStamp'] = mainCellMarkFile['TimeStamp'] - deltaStamps
        # find corresponding stamps into the log frame
        userFrames[marksPath] = self.interpolateByStamps(marksStamps,userFrames[logPath])
        return userFrames
    def writeId(self,sessionId,Dfs):
        """
        Write unique identifier into the tables
        :param sessionId: identifier || {int}
        :param Dfs: dictionary with "path"{str} keys and table {pd.DataFrame} values || { dict }
        :return: identified tables || {dict}
        """
        for path in Dfs.keys():
            Dfs[path]['session_id'] = sessionId
        return Dfs
    def saveFrames(self,Dfs):
        """
        save updated dataFrames to CSV - files
        :param Dfs:dictionary with "path"{str} keys and table {pd.DataFrame} values || { dict }
        :return:
        """
        for path in Dfs.keys():
            Dfs[path].to_csv(path,sep = ';',index = False)
    def interpolateByStamps(self,marksStamps,logFrame):
        """
        Find the stamps, that are nearest to computed marks and take the data from them.
        :param marksStamps: computed marks {pd.DataFrame}
        :param Log: user's log file {pd.DataFrame}
        :return: user's markFile {pd.DataFrame}
        """
        marksFrame = pd.DataFrame()
        Log = logFrame.copy()

        toAdd = list(marksStamps.columns.values)
        toAdd.remove('TimeStamp')

        for rowIx in marksStamps.index:
            # series object is returned! so, use values[0]
            markStamp = marksStamps[marksStamps.index == rowIx]['TimeStamp'].values[0]
            minDelta =  abs(Log['TimeStamp'] - markStamp).min()
            nearRow = Log[abs(Log['TimeStamp'] -  markStamp) == minDelta].copy()
            nearRow.TimeStamp = nearRow.loc[:,'TimeStamp'].values[0] + random.randint(0,999)
            nearRow = self.addVals(nearRow,marksStamps,rowIx,toAdd)
            marksFrame = pd.concat([marksFrame,nearRow])
        marksFrame.User = Log['User'].unique()[0]
        marksFrame['ID'] = marksFrame['ID'].astype('int64')

        return marksFrame
    def addVals(self,row,frame,ix,columns):

        for col in columns:
            row.loc[:,col] = frame.loc[frame.index == ix,col].values[0]
        return row