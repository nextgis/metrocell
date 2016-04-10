__author__ = 'Alex'
import os,sys
from dirProcesser import DirProcesser
import pandas as pd
import numpy as np
from generateRaces import GenerateRaces
from bringing_to_single_time import BringingToSingleTime
import utilities
from shutil import rmtree
import variables
import subprocess

class PrepareAndSplit():
    def __init__(self,
                 server_conn,
                 city,
                 interchanges_df,
                 graph_df,
                 raw_df,
                 plot=True
                 # moveGraphClosedPath,
                 # mainUser = None
                 ):
        self.server_conn = server_conn
        self.city = city
        self.interchanges_df = interchanges_df
        self.graph_df = graph_df
        self.raw_df = raw_df
        self.plot = plot
        self.moveTypes = variables.moveTypes
        self.device_id = None
        # self.generator = GenerateRaces(interchangesPath,moveGraphPath,self.moveTypes,moveGraphClosedPath)
        self.generator = GenerateRaces(self.interchanges_df,self.graph_df,self.moveTypes)
        # use mainUser variable to initialize folder which contains the marks
        self.dirProcesser = DirProcesser()
        #self.toParse = toParse
        #self.outputProcDir= outputProcDir
        self.bringing = BringingToSingleTime()

        # intialize temp directory if it doesn't exist
        self.initOutFlds()
        # let's go!


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
        logFrame = utilities.insertColumns(logFrame, columns =self.moveTypes + ['race_id'])
        fullFrame = pd.concat([logFrame,marksFrameIx])
        fullFrame = fullFrame.sort_values(by = ['TimeStamp','inter','stop'])
        fullFrame = fullFrame.set_index([range(0,len(fullFrame))])
        s = pd.Series(list(fullFrame['ID']))
        firstIx = s.first_valid_index()
        lastIx = s.last_valid_index()
        fullFrame = fullFrame.loc[firstIx:lastIx,:]
        return fullFrame
    def initOutFlds(self):
        """
        make output folders
        :return:
        """
        # create output folder devided by device
        f = lambda fld: os.mkdir(fld)
        # check if folder already exist
        e = lambda fld: os.path.isdir(fld)
        # create folders if need
        flds = [variables.TEMP_FLD]
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
        seg_len = marksIxs.shape[0]
        if seg_len>1:
            for i in range(1,seg_len):
                prev = marksIxs.iloc[i-1]
                next = marksIxs.iloc[i]
                fullFrame.loc[prev[0]:prev[-1],'move'] = 2
                fullFrame.loc[prev[0]:next[0],'race_id'] = marksIxs.index[i-1]
                sectionFrame = fullFrame.loc[prev[0]:next[0],:].copy()
                self.parseSectionAndDrop(sectionFrame,device)
        if seg_len == 1:
            fullFrame['move'] = 2
            fullFrame['race_id'] = marksIxs.index[0]
            sectionFrame = fullFrame.copy()
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
                _sectionFrameUpdated = utilities.dropMultipleCols(typeDf, self.moveTypes+['Name'])
                _sectionFrameUpdated.rename(columns = {'ID':'station_id'},inplace=True)
                _sectionFrameUpdated['move_type'] = moveType
                _sectionFrameUpdated['id_from'] = Ids['from']
                _sectionFrameUpdated['id_to'] = Ids['to']
                _sectionFrameUpdated = utilities.dropMultipleCols(_sectionFrameUpdated,variables.excluded_meta_cols + ['session_id'])
                _sectionFrameUpdated['city'] = self.city
                #_sectionFrameUpdated = utilities.floatToInt(_sectionFrameUpdated, ['race_id'])
                utilities.remove_slice_from_postgres2(self.server_conn,self.server_conn['tables']['parsed_'+device],
                                                      zip_id = _sectionFrameUpdated['zip_id'].iloc[0],
                                                      id_from = Ids['from'],
                                                      id_to = Ids['to'],
                                                      TimeStamp = round(_sectionFrameUpdated['TimeStamp'].iloc[0],0)
                                                      )
                utilities.insert_pd_to_postgres(_sectionFrameUpdated,self.server_conn,self.server_conn['tables']['parsed_'+device])

                #self.dirProcesser.put_in_tidy(typeDf,device,Ids,moveType)

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
        # check what type of devices(cell,sensor,external) has been grabbed
        userSession = DirProcesser.lastFld(row['cellLog'],pos = -2)
        errorsDf = pd.DataFrame()
        for device in devices:
            sys.stdout.write("\t"+ userSession + " : " + device)
            sys.stdout.flush()

            logPath = row[device + "Log"]
            marksPath = row[device + "Marks"]
            # write race_id and session_id at the mark files
            marksFrameIx,indexes,errorsDf = self.updateMarks(marksPath,indexes,pushErrors)
            if not marksFrameIx.empty:
                sortedFullFrame = self.concatAndCut(logPath,marksFrameIx)
                sortedFullFrame['zip_id'] = row['zip_id']
                self.breakOnSections(sortedFullFrame,device)
            if pushErrors:
                if not errorsDf.empty:
                    errorsDf['zip_id'] = row['zip_id']
                    errorsDf = utilities.dropMultipleCols(errorsDf,['ID'])
                #pushErrors = False

        return indexes,errorsDf

    def loopRaws(self):
        """
        loop through the raw zip files.
        :return:
        """
        print 'Parser starts'
        print 30*'-'
        #step = 0
        #length = len(self.raw_df)

        row = self.raw_df.iloc[0]
        zip_id = self.raw_df.first_valid_index()
        zip_ids = list(self.raw_df.index)


        #step+=1
        #sys.stdout.write("\r" + str(step) + "/" + str(length) + " : ")
        #sys.stdout.flush()
        # check if number of phones > 1(if user was grabbing the logs from more then 1 phone)
        if row['session_id']:
            session_rows = self.raw_df[self.raw_df['session_id'] == row['session_id']]
            # loop throuth the users
            sessionData = {}
            for zip_id,session_row in session_rows.iterrows():
                session_path = variables.INBOX + '/' + self.city + '/sessions/' + session_row['zip_basename']
                user_session_name = os.path.splitext(os.path.basename(session_path))[0]
                extracted_logs_path = variables.TEMP_FLD + '/' + os.path.basename(user_session_name)
                self.dirProcesser.unzipUserSession(session_path,extracted_logs_path)
                sessionUserData = self.dirProcesser.getSessionData(extracted_logs_path,zip_id)
                sessionUserData['zip_id'] = zip_id
                self.device_info_df = utilities.get_pd_df_from_sql(self.server_conn,self.server_conn['tables']['device_info'])
                if not session_row['device_id']:
                    self.extract_phone_info(extracted_logs_path)
                    utilities.update_postgre_rows(self.server_conn,self.server_conn['tables']['input_data'],zip_id,'device_id',self.device_id,index_col='zip_id')
                    self.device_id = None
                sessionData.update(sessionUserData)
            usersFrame = pd.DataFrame(sessionData).transpose()
            userRow = usersFrame[usersFrame['mainUser'] == 1].iloc[0]
            devices = DirProcesser.ifExists(userRow)
            self.bringing.writeSessionSaveFrames(userRow,row['session_id'],devices)
        else:
            session_path = variables.INBOX + '/' + self.city + '/' + row['zip_basename']
            session_name =  os.path.splitext(session_path)[0]
            extracted_logs_path = variables.TEMP_FLD + '/' + os.path.basename(session_name)
            self.dirProcesser.unzipUserSession(session_path,extracted_logs_path)
            self.device_info_df = utilities.get_pd_df_from_sql(self.server_conn,self.server_conn['tables']['device_info'])
            if (not row['device_id']) or (row['device_id']!=row['device_id']):
                self.extract_phone_info(extracted_logs_path)
                utilities.update_postgre_rows(self.server_conn,self.server_conn['tables']['input_data'],zip_id,'device_id',self.device_id,index_col='zip_id')
                self.device_id = None
            sessionData = self.dirProcesser.getSessionData(extracted_logs_path,zip_id)
            userRow = pd.DataFrame(sessionData).transpose().iloc[0]
            devices = DirProcesser.ifExists(userRow)

        # only after this step we can start copy marks
        # firstly process the main user (if there are several users at the session)
        mainIds,errors_df = self.loopMarks(userRow,devices,pushErrors= True)
        if (row['session_id'])and(len(mainIds)>0):
            # usersFrame contains only the paths. let's start to copy the marks from main User folder.
            usersFrameUpdated = self.bringing.processFrames(usersFrame,userRow)
            # split other users on sections
            otherUsersFrame = usersFrameUpdated[usersFrameUpdated['mainUser']==0]
            for index,row in otherUsersFrame.iterrows():
                devices = eval(row['devices'])
                self.loopMarks(row,devices,mainIds)
        # save errors in the markFiles
        print ' '
        if not errors_df.empty:
            utilities.insert_pd_to_postgres(errors_df,self.server_conn,self.server_conn['tables']['grab_errors'])
        for z_id in zip_ids:
            utilities.update_postgre_rows(self.server_conn,self.server_conn['tables']['processing_status'],z_id,'parsed',True,index_col = 'zip_id')
        [rmtree(variables.TEMP_FLD + '/' + fld) for fld in os.listdir(variables.TEMP_FLD) if os.path.isdir(variables.TEMP_FLD + '/' + fld)]
        [os.remove(variables.TEMP_FLD + '/' + file) for file in os.listdir(variables.TEMP_FLD) if os.path.isfile(variables.TEMP_FLD + '/' + file)]

    def extract_phone_info(self,session_path):
        device_path = session_path + '/' + variables.device_info
        if os.path.exists(device_path):
            fr = pd.io.parsers.read_table(device_path,header = None)
            fr[0] = fr[0].apply(lambda x: x[:-1])
            fr = fr.set_index(0)
            fr = fr.transpose()
            fr.rename(columns = {
                       'Kernel version':'Kernel_version',
                       'Radio firmware':'Radio_firmware',
                       'Logger version name':'Logger_version_name',
                       'Logger version code':'Logger_version_code',
                       },inplace = True)
            device_id = None
            for id,device in self.device_info_df.iterrows():
                try:
                    if all(device == fr.iloc[0]):
                        self.device_id = device_id = id
                        print "device = ",device['Brand'],"with id = " ,self.device_id ," is already exist at the database"
                        break
                except:
                    pass
            if not device_id:
                self.device_id = utilities.insert_pd_to_postgres(fr, self.server_conn,self.server_conn['tables']['device_info'],return_last_id=True)

        else:
            print 'oops!> device file does not exist!',device_path


