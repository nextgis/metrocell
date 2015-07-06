#coding=utf-8
import os,string,zipfile,sys,random,shutil
import pandas as pd
from argparse import ArgumentParser
class BringingToSingleTime():
    def __init__(self,folderPath,mainUser,rawFldPath,toParse,
                 sensorMarksName = "sensor_time_marks.csv",
                 cellMarksName = "cell_time_marks.csv",
                 externalMarksName = "external_time_marks.csv",
                 sensorLogName = "sensor_time_log.csv",
                 cellLogName = "cell_time_log.csv",
                 externalLogName = "external_time_log.csv"
                    ):
        self.folderPath = folderPath
        self.rawFldPath = rawFldPath
        self.mainUser = mainUser
        self.toParse = toParse
        self.sensorMarksName = sensorMarksName
        self.cellMarksName = cellMarksName
        self.externalMarksName = externalMarksName
        self.sensorLogName = sensorLogName
        self.cellLogName = cellLogName
        self.externalLogName = externalLogName
        self.cellLogName = cellLogName

        # let's go!
        self.loopSessions()
    def generateMarks(self,usersFrame):
        """
        Generation of marks base on the fact that user started grabbing the data from several phones at ones.
         So, if we know the delta times between 'main' and any other phone then we can create marks for them using
         the simple math.
        :param usersFrame: frame containing paths to all of the files of user session
        :return: CSV markFiles
        """

        mainCellLogFilePath = usersFrame[usersFrame['mainUser'] == 1]['cellLog'].unique()[0]
        mainCellMarkFilePath = usersFrame[usersFrame['mainUser'] == 1]['cellMarks'].unique()[0]
        mainCellLogFile = pd.io.parsers.read_csv(mainCellLogFilePath,sep = ";")

        mainCellMarkFile = pd.io.parsers.read_csv(mainCellMarkFilePath,sep = ";")
        # using only the "base station" marks!
        mainCellMarkFile = mainCellMarkFile[mainCellMarkFile['Active'] == 1]
        mainUserFirstStamp = mainCellLogFile['TimeStamp'][0]
        otherUsers = usersFrame[usersFrame['mainUser'] == 0].index
        for user in otherUsers:
            userRow = usersFrame[usersFrame.index == user]
            logShortNames = self.ifExists(userRow)
            for logShortName in logShortNames:
                logPath = usersFrame[usersFrame.index == user][logShortName + "Log"].unique()[0]
                marksPath = usersFrame[usersFrame.index == user][logShortName + "Marks"].unique()[0]
                Log = pd.io.parsers.read_csv(logPath,sep = ";")
                userFirstStamp = Log['TimeStamp'][0]
                # compute the delta time
                deltaStamps = mainUserFirstStamp - userFirstStamp
                marksStamps = mainCellMarkFile[['ID','Name','User']].copy()
                marksStamps['TimeStamp'] = mainCellMarkFile['TimeStamp'] - deltaStamps
                marksFrame = self.interpolateByStamps(marksStamps,Log)
                marksFrame.to_csv(marksPath,sep = ";",index = False)

    def ifExists(self,row):
        """
        Check what files does the phone grabbed.
        :param row: paths of user's session in theory
        :return: only really existed file paths
        """
        truthPaths = []
        codes = ['cell','sensor','external']
        for code in codes:
            fname = code + "Log"
            path = row[fname][0]
            exists = os.path.isfile(path)
            if exists == True:
                truthPaths.append(code)
        return truthPaths
    def interpolateByStamps(self,marksStamps,Log):
        """
        Find the stamps, that are nearest to computed marks and take the data from them.
        :param marksStamps: computed marks {pd.DataFrame}
        :param Log: user's log file {pd.DataFrame}
        :return: user's markFile {pd.DataFrame}
        """
        marksFrame = pd.DataFrame()
        for rowIx in marksStamps.index:
            # series object is returned! so, use values[0]
            markStamp = marksStamps[marksStamps.index == rowIx]['TimeStamp'].values[0]
            minDelta =  abs(Log['TimeStamp'] - markStamp).min()
            nearRow = Log[abs(Log['TimeStamp'] -  markStamp) == minDelta]
            nearRow.TimeStamp = nearRow.loc[:,'TimeStamp'].values[0] + random.randint(0,999)
            nearRow.ID = marksStamps.loc[marksStamps.index == rowIx,'ID'].values[0]
            nearRow.Name = marksStamps.loc[marksStamps.index == rowIx,'Name'].values[0]
            marksFrame = pd.concat([marksFrame,nearRow])
        marksFrame.User = Log['User'].unique()[0]
        marksFrame['ID'] = marksFrame['ID'].astype('int64')
        return marksFrame

    def getSessionData(self,userSessionFldPath):
        """
        Write data about all possible file paths  to the table
        :param userSessionFldPath: path to the folder containing the sessions {str}
        :return: the data about users file paths {pd.DataFrame}
        """
        usersData = {}
        userName = string.split(userSessionFldPath, "--")[-1]
        if userName == self.mainUser:
            mainUser = 1
        else:
            mainUser = 0
        sensorMarks =   userSessionFldPath + "\\" + self.sensorMarksName
        cellMarks =         userSessionFldPath + "\\" + self.cellMarksName
        externalMarks =     userSessionFldPath + "\\" + self.externalMarksName
        sensorLog =         userSessionFldPath + "\\" + self.sensorLogName
        cellLog =           userSessionFldPath + "\\" + self.cellLogName
        externalLog =       userSessionFldPath + "\\" + self.externalLogName
        usersData.update({userName:{
                        "mainUser"            : mainUser,
                        "sensorMarks"     : sensorMarks,
                        "cellMarks"       : cellMarks,
                        "externalMarks"   : externalMarks,
                        "sensorLog"       : sensorLog,
                        "cellLog"         : cellLog,
                        "externalLog"     : externalLog
                                }
        })

        return usersData
    def unzipUserSession(self,pathToSession,userZip):
        """
        Unzipping files
        :param pathToSession: path to session {str}
        :param userZip: user's raw zip-file  {str}
        :return: name of the extracted folder {str}
        """
        userSessionFolder =   pathToSession + "\\" + userZip
        outSessionFld =  pathToSession + "\\" + string.split(userZip,".")[0]
        zip_ref = zipfile.ZipFile(userSessionFolder,'r')
        zip_ref.extractall(outSessionFld)
        zip_ref.close()
        newSessionName = string.split(userSessionFolder,".")[0]
        return newSessionName
    def grabAndDrop(self,fld):
        """
        Take the generated files and drop them to the folder
        :param fld: output folder {str}
        :return:
        """
        files = os.listdir(fld)
        userFld = string.split(fld,"\\")[-1]
        outPath = self.rawFldPath + "\\" + userFld + ".zip"
        zf = zipfile.ZipFile(outPath,"w")
        for file in files:
            fullname = fld + "\\" + file
            zf.write(fullname)
        zf.close()
        if self.toParse:
            dst = self.toParse + "\\" + userFld
            shutil.move(src = fld, dst = dst)
    def loopSessions(self):
        """
        Loop through the sessions at the input folder.
        :return:
        """
        #
        for session in os.listdir(self.folderPath):
            sys.stdout.write("\r" + session )
            sys.stdout.flush()
            sessionFolder = self.folderPath + "\\" + session
            sessionUsersData = {} 
            usersSessions = os.listdir(sessionFolder)
            numSessions = len(usersSessions)
            # check if number of phones > 1(if user was grabbing logs from more then 1 phone)
            if numSessions>1:
                # loop through users
                for userSessionName in usersSessions:
                    userSessionName = self.unzipUserSession(sessionFolder,userSessionName)
                    sessionUserData = self.getSessionData(userSessionName)
                    sessionUsersData.update(sessionUserData)
                usersFrame = pd.DataFrame(sessionUsersData).transpose()
                self.generateMarks(usersFrame)
                if self.rawFldPath:
                    for userSessionName in usersSessions:
                        fName,ext = os.path.splitext(userSessionName)
                        userSession = sessionFolder + "\\" + fName
                        self.grabAndDrop(userSession)
            else:
                print  ' || To generate marks folder must contain at least 2 sessions'


def main():
   parser = ArgumentParser(description = 'Generating of the NextGis Logger phone marks in the single time grabbing cases')
   parser.add_argument('-u', '--mainUser', type = str, required = True, help = 'The user whose logs contain markFiles')
   parser.add_argument('-p', '--pathToParse',type = str, default = False, help = "The output directory to save adapted folders."
                                                                                             "This may need for the next step of processing namely break each on sections "
                                                                                             "Modes: 'single' - to the one general directory"
                                                                                             "       'same'   - to the same session-directories")

   parser.add_argument('-o','--outputRawDir',type = str,default = False, help = "The output directory to save adapted zip-files."
                                                                                        "Modes: 'single' - to the one general directory"
                                                                                        "       'same'   - to the same session-directories."
                                                                                        "Note that in the second case zip-files will not be created")

   parser.add_argument('inputSessionsDir',type = str,help = 'Input directory contains sessions. '
                                                            'One session might contains several zip-archives where one archive represents one user session')
   args = parser.parse_args()
   return args
if __name__ == '__main__':
    args = main()
    BringingToSingleTime(mainUser = args.mainUser,
                                    toParse = args.pathToParse,
                                    folderPath = args.inputSessionsDir,
                                    rawFldPath= args.outputRawDir)


