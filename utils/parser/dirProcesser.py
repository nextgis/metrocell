__author__ = 'Alex'
import zipfile,shutil,string,os
import utilities
import variables
class DirProcesser():
    def __init__(self):
        self.moveTypes = variables.moveTypes
        self.mainUser = None
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
        localTime = utilities.getLocalTime(sectionFrame)
        username = sectionFrame.User.unique()[0]
        fileName=stationsId['from'].zfill(3) + "-" + \
                 stationsId['to'].zfill(3)   + "-" + \
                 localTime                   + "-" + \
                 username                    + "-" + \
                 moveType + ".csv"
        path = outputDir + "/" + device + "/" + fileName
        sectionFrameAdapted = utilities.dropMultipleCols(sectionFrame, self.moveTypes)
        #sectionFrameAdapted.loc[:,'race_id'] = sectionFrameAdapted.loc[:,'race_id'].astype('int64')
        sectionFrameAdapted = utilities.checkId(sectionFrameAdapted)
        sectionFrameAdapted = utilities.floatToInt(sectionFrameAdapted, ['race_id', 'ID'])
        sectionFrameAdapted.to_csv(path,index=False,encoding='utf-8')
    def getSessionData(self,user_session_fld_path,zip_id):
        """
        Write data of all possible file paths  to the table
        :param userSessionFldPath: path to the folder containing the sessions {str}
        :return: the data about users file paths {pd.DataFrame}
        """
        mainUser = 0
        usersData = {}
        userName = os.path.basename(user_session_fld_path).split("--")[-1]

        sensorMarks =      user_session_fld_path + "/" + variables.sensorMarksName
        cellMarks =        user_session_fld_path + "/" + variables.cellMarksName
        externalMarks =    user_session_fld_path + "/" + variables.externalMarksName
        sensorLog =        user_session_fld_path + "/" + variables.sensorLogName
        cellLog =          user_session_fld_path + "/" + variables.cellLogName
        externalLog =      user_session_fld_path + "/" + variables.externalLogName
        for mark in [sensorMarks,externalMarks,cellMarks]:
            if os.path.exists(mark):
                mainUser = 1
        usersData.update({userName:{
                        "zip_id"          : zip_id,
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
            errorsDict[localPath].to_csv(dir + "/" +  localPath,sep = ';',index = False)
    @staticmethod
    def lastFld(path,pos):
        return string.split(path,"/")[pos]
    @staticmethod
    def ifExists(row):
        """
        Check what files the phone has grabbed.
        :param row: paths of user's session in theory
        :return: only really existed file paths
        """
        truthPaths = []
        codes = ['cell','sensor','external']
        for code in codes:
            fname = code + "Log"
            path = row[fname]
            exists = os.path.isfile(path)
            if exists:
                truthPaths.append(code)
        return truthPaths

    @staticmethod
    def unzipUserSession(zip_path,out_path):
        """
        Unzipping files
        :param pathToSession: path to session {str}
        :param userZip: user's raw zip-file  {str}
        :return: name of the extracted folder {str}
        """
        zip_ref = zipfile.ZipFile(zip_path,'r')
        zip_ref.extractall(out_path)
        zip_ref.close()

        return
    @staticmethod
    def grabAndDrop(fld,fldDst,zipDst):
        """
        Take the generated files and drop them to the folder
        :param fld: output folder {str}
        :return:
        """
        files = os.listdir(fld)
        userFld = string.split(fld,"/")[-1]
        outPath = zipDst + "/" + userFld + ".zip"
        zf = zipfile.ZipFile(outPath,"w")
        for file in files:
            fullname = fld + "/" + file
            zf.write(fullname)
        zf.close()
        if fldDst:
            dst = fldDst + "/" + userFld
            shutil.move(src = fld, dst = dst)
