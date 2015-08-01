__author__ = 'Alex'
import zipfile,shutil,string,os
from utils import Utils
class DirProcesser():
    def __init__(self,
                 moveTypes,
                 mainUser='',
                 sensorMarksName = "sensor_time_marks.csv",
                 cellMarksName = "cell_time_marks.csv",
                 externalMarksName = "external_time_marks.csv",
                 sensorLogName = "sensor_time_log.csv",
                 cellLogName = "cell_time_log.csv",
                 externalLogName = "external_time_log.csv"):
        self.sensorMarksName = sensorMarksName
        self.cellMarksName = cellMarksName
        self.externalMarksName = externalMarksName
        self.sensorLogName = sensorLogName
        self.cellLogName = cellLogName
        self.externalLogName = externalLogName
        self.cellLogName = cellLogName
        self.mainUser = mainUser
        self.moveTypes = moveTypes
    def put_in_tidy(self,sectionFrame,device,stationsId,moveType,outputDir):
        """
        Push processed sectionFrame into the output directory
        :param sectionFrame: frame to push {pd.DataFrame}
        :param device: 'cell','sensor' or 'external' {str}
        :param stationsId: station-from,station-to {list}
        :param moveType: 'stop','inter' or 'move' {str}
        :param outputDir: path to the output directory {str}
        :return:
        """
        localTime = Utils.getLocalTime(sectionFrame)
        username = sectionFrame.User.unique()[0]
        fileName=stationsId['from'].zfill(3) + "-" + \
                 stationsId['to'].zfill(3)   + "-" + \
                 localTime                   + "-" + \
                 username                    + "-" + \
                 moveType + ".csv"
        path = outputDir + "\\" + device + "\\" + fileName
        sectionFrameAdapted = Utils.dropMultipleCols(sectionFrame,self.moveTypes)
        #sectionFrameAdapted.loc[:,'race_id'] = sectionFrameAdapted.loc[:,'race_id'].astype('int64')
        sectionFrameAdapted = Utils.checkId(sectionFrameAdapted)
        sectionFrameAdapted = Utils.floatToInt(sectionFrameAdapted,['race_id','ID'])
        sectionFrameAdapted.to_csv(path,index=False,encoding='utf-8')
    def getSessionData(self,userSessionFldPath):
        """
        Write data of all possible file paths  to the table
        :param userSessionFldPath: path to the folder containing the sessions {str}
        :return: the data about users file paths {pd.DataFrame}
        """
        usersData = {}
        userName = string.split(userSessionFldPath, "--")[-1]
        if userName == self.mainUser:
            mainUser = 1
        else:
            mainUser = 0
        sensorMarks =       userSessionFldPath + "\\" + self.sensorMarksName
        cellMarks =         userSessionFldPath + "\\" + self.cellMarksName
        externalMarks =     userSessionFldPath + "\\" + self.externalMarksName
        sensorLog =         userSessionFldPath + "\\" + self.sensorLogName
        cellLog =           userSessionFldPath + "\\" + self.cellLogName
        externalLog =       userSessionFldPath + "\\" + self.externalLogName
        usersData.update({userName:{
                        "mainUser"        : mainUser,
                        "sensorMarks"     : sensorMarks,
                        "cellMarks"       : cellMarks,
                        "externalMarks"   : externalMarks,
                        "sensorLog"       : sensorLog,
                        "cellLog"         : cellLog,
                        "externalLog"     : externalLog
                                }
        })
        return usersData
    def removeFlds(self,localDirs):
        for lDir in localDirs:
            shutil.rmtree(lDir)
    def pushErrors(self,errorsDict,dir):
        for localPath in errorsDict.keys():
            errorsDict[localPath].to_csv(dir + "\\" +  localPath,sep = ';',index = False)
    @staticmethod
    def lastFld(path,pos):
        return string.split(path,"\\")[pos]
    @staticmethod
    def ifExists(row):
        """
        Check what files does the phone grabbed.
        :param row: paths of user's session in theory
        :return: only really existed file paths
        """
        truthPaths = []
        codes = ['cell','sensor','external']
        for code in codes:
            fname = code + "Log"
            path = row[fname]
            exists = os.path.isfile(path)
            if exists == True:
                truthPaths.append(code)
        return truthPaths

    @staticmethod
    def unzipUserSession(pathToSession,userZip):
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
    @staticmethod
    def grabAndDrop(fld,fldDst,zipDst):
        """
        Take the generated files and drop them to the folder
        :param fld: output folder {str}
        :return:
        """
        files = os.listdir(fld)
        userFld = string.split(fld,"\\")[-1]
        outPath = zipDst + "\\" + userFld + ".zip"
        zf = zipfile.ZipFile(outPath,"w")
        for file in files:
            fullname = fld + "\\" + file
            zf.write(fullname)
        zf.close()
        if fldDst:
            dst = fldDst + "\\" + userFld
            shutil.move(src = fld, dst = dst)
