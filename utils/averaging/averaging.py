__author__ = 'Alex'

import variables
import pandas as pd
import numpy as np
import utilities
from preproc import Preproc
from smooth import Smooth
from scipy import interpolate
import sys
import psycopg2

from posAlgorithm import SignalCorrelator
from filters import Filters
class Averaging():
    def __init__(self,server_conn,averaged_tabname,zip_ids,city
                  ):
        self.server_conn = server_conn
        self.zip_ids = zip_ids
        self.city = city
        self.averaged_table_name =  self.server_conn['tables']['averaged_'+averaged_tabname+'_meta']
        self.connString = "host = %s user = %s password = %s dbname = %s port = %s" %\
                         (self.server_conn['host'],self.server_conn['user'],self.server_conn['password'],self.server_conn['dbname'],self.server_conn['postgres_port'])
        self.segments = {}
        self.move_df = utilities.merge_parsed_georeferenced_df(self.server_conn,'parsed_'+averaged_tabname)
        # extract the only rows which represent input zip ids
        self.move_df = self.move_df[self.move_df['zip_id'].isin(self.zip_ids)]

        self.subway_info_df = utilities.get_pd_df_from_sql(self.server_conn,self.server_conn['tables']['subway_info'])


        self.averaged_geo = utilities.get_pd_df_from_sql(self.server_conn,self.server_conn['tables']['averaged_geo'],index_col=['city','ratio','segment_id'])
        # make averaged point geometries for all segments
        if self.averaged_geo.empty:
            utilities.interpolate_averaged_points(self.server_conn,
                                                  self.server_conn['tables']['lines'],
                                                  self.server_conn['tables']['averaged_geo'],
                                                  step = variables.averaged_cell_pars['interp_step_meters']
                                                  )
            self.averaged_geo = utilities.get_pd_df_from_sql(self.server_conn,self.server_conn['tables']['averaged_geo'],index_col=['city','ratio','segment_id'])
        #instances
        self.filters = Filters()
        # save after filtration
        #self.filters.unique_names = ['segment','NetworkType','NetworkGen','LAC','CID','laccid','segment_start_id','segment_end_id']
        self.filters.unique_names = ['segment_id','NetworkGen','LAC','CID','laccid','quality']
        self.filters.nNeighbours = variables.averaged_cell_pars['nNeighbors']
        self.minData = variables.averaged_cell_pars['minData']
        self.filters.testsize = variables.averaged_cell_pars['testsize']
        self.aver_df = pd.DataFrame()
        # drop this names from database
        # self.dropnames = ['NumRaces','NumUsers','laccid','segment','quality','weight']
        self.dropnames = ['User','rawPower','Active','PSC','TimeStamp','race_id','zip_id','station_id','NumUsers','NumRaces','id_from','id_to','move_type','cell_id','quality','laccid','MCC','geom']
        # the minimum number of rows for each collected unique cell(for preprocessing)
        self.Lcdelta = variables.averaged_cell_pars['lcDelta'] # MUST BE IDENTICAL IN SMOOTH MODULE!

        self.correlator = SignalCorrelator()
        self.powerAveraging = Smooth()
        self.powerAveraging.correlator =self.correlator
        self.powerAveraging.filters = self.filters
        return

    def iterateBySegment(self):
        #self.segmentsStepsDf = pd.DataFrame()
        networkErrors = pd.DataFrame()

        grouped_move = self.move_df.groupby(['id_from','id_to'])
        seg_len = len(grouped_move.groups.keys())
        # set default quality
        i = 0
        for (id_from,id_to),segment_df in grouped_move:
            net_quality_segment = pd.DataFrame()
            smoothed_df_segment = pd.DataFrame()
            i+=1
            sys.stdout.write(str(i) + " / " + str(seg_len) + ' segment_id : ' + id_from + " - " + id_to)
            sys.stdout.flush()
            segment_info = self.subway_info_df[self.subway_info_df['segment_id'] == (id_from.zfill(3) + '-' + id_to.zfill(3))]
            if not segment_info.empty:
                time_median = segment_info['time_median'].iloc[0]
            else:
                raise Exception("Median time has not been computed!")
            # split data to equal time step(cut on one second steps)
            time_Df,interpStep = self.splitByTimeStep(time_median)
            # write ratio length to the DataFrame
            #segRow = pd.DataFrame({'segment':[seg],'interpStep':interpStep,'index':[step]})
            #self.segmentsStepsDf = pd.concat([self.segmentsStepsDf,segRow])

            self.filters.time_Df = time_Df
            self.powerAveraging.segmentTime = time_median
            segment_net_gen_gr = segment_df.groupby(['NetworkGen'])
            for NetGEN, net_gen_gr in segment_net_gen_gr:
                segment_laccid_gr = net_gen_gr.groupby(['LAC','CID'])
                for (LAC,CID),laccid_gr in segment_laccid_gr:
                    NumRaces = len(np.unique(laccid_gr['race_id']))
                    # loop through laccids
                    #try:
                    laccid_gr,MNC = self.extract_false_mncs(laccid_gr)
                    if not MNC:
                        continue
                    # check if frame contains error-rows(for ex. points which contains lac and cid from the next cell, but MNC from the last cell)
                    errorRows,laccid_gr = self.powerAveraging.splitFrameByMinLen(laccid_gr,by = 'NetworkType')
                    if len(laccid_gr) > self.minData:
                        # filter all parts of data with a few datasets.
                        self.powerAveraging.interpStep = interpStep
                        # initialization of smoothing algorithm
                        smoothed_df,fewData = self.powerAveraging.initCombinations(laccid_gr,combinations='RU')
                        if not smoothed_df.empty:
                            net_quality_slice = smoothed_df[['segment_id','laccid','NetworkGen','quality','ratio']]
                            net_quality_slice.loc[:,'MNC'] = MNC
                            net_quality_slice.loc[:,'NumRaces'] = NumRaces  # todo:check!
                            net_quality_slice.loc[:,'NumUsers'] = self.powerAveraging.num_users
                            net_quality_slice.loc[:,'city'] = self.city
                            smoothed_df.loc[:,'city'] = self.city
                            smoothed_df.loc[:,'MNC'] = MNC
                            #smoothed_df.loc[:,'NetworkGen'] = NetGEN
                            smoothed_df = utilities.dropMultipleCols(smoothed_df,self.dropnames)

                            smoothed_df_segment = pd.concat([smoothed_df_segment,smoothed_df])
                            net_quality_segment = pd.concat([net_quality_segment,net_quality_slice])
                # extract number of LACCIDs and apply it for line geometry
                #self.segments = list(np.unique(self.move_df['segment_id']))
                # - process averaged cell
                if not smoothed_df_segment.empty:
                    self.interpolate_to_equal_time_ratio_and_push(smoothed_df_segment)
                    utilities.plot_signal_power(self.server_conn,'georeferencing_averaged',id_from.zfill(3),id_to.zfill(3),self.city)
                if not net_quality_segment.empty:
                    self.push_cell_quality(net_quality_segment)
                    self.push_cell_grabbing_quality(net_quality_segment)
        # write into the appropriate table that process has been complited
        for z_id in self.zip_ids:
            utilities.update_postgre_rows(self.server_conn,self.server_conn['tables']['processing_status'],z_id,'averaged',True,index_col = 'zip_id')


        print networkErrors
    def extract_false_mncs(self,df,field = 'MNC'):
        false_mncs = []
        init_df = df.copy()
        unique_vals = list(df[field].unique())
        for f in unique_vals:
            if f!='-1':
                mnc_gr = df[df[field] == f]
                if not len(mnc_gr)> self.minData:
                    false_mncs.append(f)
        unique_vals.sort(reverse=True)
        if len(false_mncs)>0:
            df = df[~df[field].isin(false_mncs)]
            unique_vals = list(df[field].unique())
            if len(unique_vals)>1:
                print "There are many " + field + ' : ', unique_vals
                return df,None
            if len(unique_vals) == 0:
                print  "Too small slice: ",len(init_df)," rows"
                return df,None

        return df,unique_vals[0]
    #def postProc(self):
     #   print "Post georeferencing starts"

     #   self.segments = list(np.unique(self.move_df['segment_id']))
        # - process averaged cell
      #  self.interpolate_to_equal_time_ratio_and_push(self.aver_df)

        #self.aver_df['geom'] = self.aver_df.apply(lambda x:
         #                                      utilities.interpolator(self.server_conn,
          #                                                            self.server_conn['tables']['lines'] ,
          #                                                            x['id_from'],
          #                                                            x['id_to'],
            #                                                          x['ratio'],
            #                                                          self.city),axis = 1)

        # - process network quality

        #utilities.remove_slice_from_postgres(self.server_conn,self.server_conn['tables']['subway_cell_quality'],'segment_id',self.segments)

        # - process data quality

        # - ggplotting
      #  for seg in self.segments:
      #      utilities.plot_signal_power(self.server_conn,'georeferencing_averaged',seg.split('-')[0],seg.split('-')[1],self.city)

    def preprocData(self):
        """
        Preprocessing of input dataFrame

        """

        self.preproc_df = Preproc(df = self.move_df)
        # process move-data
        self.preproc_df.proc_cell_df()
        self.subway_info_df = self.preproc_df.computeAverTime()
        self.subway_info_df['city'] = self.city
        utilities.update_postgre_rows_cols(self.server_conn,self.server_conn['tables']['subway_info'],['segment_id'],self.subway_info_df)
        self.preproc_df.filterLackofData(self.Lcdelta)
        # todo : define if this operations is need
        #self.preproc_df.exclude_constant_signals()
        self.move_df = self.preproc_df.df
        self.move_df['quality'] = variables.averaged_cell_pars['default_quality']
        self.move_df['city'] = self.city
        # split data by Operators

    def processTestDf(self,df,subwayInfoDf):
        """
        Processing of input test frame.loop through the segments and interpolate each of them by equal steps.
        :param df: testing dataframe {pd.DataFrame}
        :param subwayInfoDf: dataframe contains info about passed time for each segment
        :return:
        """
        Interpolated = pd.DataFrame()
        segments = df.segment.unique()
        for seg in segments:
            segDf = df[df.segment == seg]
            pathTime = subwayInfoDf.loc[seg]['pathTime']
            time_Df,interpStep = self.splitByTimeStep(pathTime)
            grouped = segDf.groupby('laccid')
            for ix,gr in grouped:
                _gr = self.distToTimeRatio(gr,time_Df,toTime=True)
                Interpolated = pd.concat([Interpolated,_gr])
        return Interpolated
    def interpolate_to_equal_time_ratio_and_push(self,df):
        print "Init interpolate_to_equal_time_ratio_and_push"
        grouped_segments = df.groupby(['segment_id'])

        for seg_id,seg_gr in grouped_segments:
            cell_grouped = seg_gr.groupby(['LAC','CID','MNC','NetworkGen'])
            for (LAC,CID,MNC,NetGen),cell_gr in cell_grouped:
                utilities.remove_slice_from_postgres2(self.server_conn,self.server_conn['tables']['averaged_cell_meta'],
                                                     segment_id = seg_id,
                                                     LAC = LAC,
                                                     CID = CID,
                                                     MNC = MNC,
                                                     NetworkGen = NetGen
                                                     )
                interp_df = pd.DataFrame()
                _cell_gr = cell_gr.sort_values(by = ['ratio'])
                # get segment from database averaged_geo frame
                geo_fr = self.averaged_geo[(self.averaged_geo.index.get_level_values(2) == seg_id)&
                                           (self.averaged_geo.index.get_level_values(0) == self.city)]
                if not geo_fr.empty:
                    geo_ratios = list(geo_fr.index.get_level_values(1).astype(np.float))
                    f = interpolate.interp1d(_cell_gr['ratio'],_cell_gr['Power'],bounds_error = False)
                    for i,row in _cell_gr.iterrows():
                        _row = row.copy()
                        interp_ratio_ix = np.searchsorted(geo_ratios,[row['ratio']])[0]
                        try:
                            interp_ratio = geo_ratios[interp_ratio_ix]
                            _row['ratio'] = interp_ratio
                            # todo: correct!
                            _row['Power'] = f(interp_ratio).base[0][0]
                            interp_df = pd.concat([interp_df,pd.DataFrame(_row).transpose()])
                        except:
                            print "Point with ratio = ", row['ratio'], "could not be interpolated"
                    #power = np.interp(list(geo_ratios.astype(np.float)),list(_seg_gr['ratio'].astype(np.float)),list(_seg_gr['Power'].astype(np.float)))
                    utilities.insert_pd_to_postgres(interp_df,self.server_conn,self.server_conn['tables']['averaged_cell_meta'])
                else:
                    print "Segment = ",seg_id ," does not exist at the city = ",self.city



    def distToTimeRatio(self,distDf,timeDf,toTime = False):
        """
        Translate distance ratios to the time ratios
        :param distDf: origin df {pd.DataFrame}
        :param timeDf: end df splitted by ratios {pd.DataFrame}
        :param toTime: if true - append "TimeStamp" column to imitate real situation. {boolean,default False}
        :return: translated Df {pd.DataFrame}
        """
        LCDF = pd.DataFrame()
        grouped = distDf.groupby(['race_id','User'])
        for ix,gr in grouped:
            newDf = pd.DataFrame()
            boundaries = np.searchsorted(timeDf['ratio'],gr.loc[[gr.index[0],gr.index[-1]],'ratio'])
            _grDf = timeDf[boundaries[0]:boundaries[-1]]
            if not _grDf.empty:
                ixs = np.searchsorted(gr['ratio'],_grDf['ratio'])
                ixs = gr.iloc[ixs].index
                for i in range(0,len(ixs)):
                    row = gr.loc[ixs[i]:ixs[i]]
                    _row = row.copy()
                    _row.loc[row.index[0],'ratio'] = _grDf.loc[_grDf.index[i],'ratio']
                    if toTime:
                        _row.loc[row.index[0],'TimeStamp'] = _grDf.loc[_grDf.index[i],'time']
                    newDf = pd.concat([newDf,_row])
                newDf['Power'] = np.interp(newDf['ratio'].astype(np.float),gr['ratio'].astype(np.float),gr['Power'].astype(np.float))
                LCDF = pd.concat([LCDF,newDf])
        return LCDF
    def splitByTimeStep(self,pathTime,step = 1):
        """
        Split dataframe by equal time steps
        :param pathTime: mean time passed on a segment {float}
        :param step: length of the step (default 1 second) {int}
        :return: _segDf: dataFrame splitted on equal segments
             interpStep: interpolation step
        """
        nSteps = int(round(pathTime/step))+1
        timeRatio = np.linspace(0,1,nSteps)
        _segDf = pd.DataFrame({'ratio':timeRatio})
        interpStep = _segDf.loc[_segDf.index[1],'ratio'] - _segDf.loc[_segDf.index[0],'ratio']
        times = np.linspace(0,len(_segDf)-1,len(_segDf))
        _segDf.loc[:,'time'] = times
        return _segDf,interpStep
    def dbTestSplitter(self,df):
        """
        random choose and split data on main and test DF.
        :param df: origin dataframe
        :return: MainDf: dataframe will be bushed to smoothing algorithm
                 TestDf: testing parts
        """
        MainDf = pd.DataFrame()
        TestDf = pd.DataFrame()
        grouped = df.groupby(['segment_id'])
        for seg,gr in grouped:
            _gr = gr.copy()
            races = _gr.race_id.unique()
            testRace = np.random.choice(races,1)[0]
            testGroup = _gr[_gr.race_id == testRace]
            users = testGroup.User.unique()
            if len(list(users) + list(races)) >2:
                testUser = np.random.choice(users,1)[0]
                testFrame = testGroup[testGroup.User == testUser]
                mainFrame = _gr.drop(testFrame.index)
                MainDf = pd.concat([MainDf,mainFrame])
                TestDf = pd.concat([TestDf,testFrame])
        return MainDf,TestDf
    def push_cell_grabbing_quality(self,pts_df):
        print "Init push_cell_grabbing_quality"
        conn = psycopg2.connect(self.connString)
        conn.rollback()
        cur = conn.cursor()
        sql = """INSERT INTO """ + self.server_conn['tables']['subway_cell_grabbing_quality'] + """(segment_id,"NetworkGen","MNC",geom,quality,"NumRaces","NumUsers",city)
        VALUES(%(segment_id)s,%(NetworkGen)s,%(MNC)s,ST_GeomFromText(%(geom)s,3857),%(quality)s,%(NumRaces)s,%(NumUsers)s,%(city)s)"""
        pts_df['id_from'] = pts_df['segment_id'].apply(lambda x : x.split('-')[0])
        pts_df['id_to'] = pts_df['segment_id'].apply(lambda x : x.split('-')[1])
        pts_df['geom'] = pts_df.apply(lambda x:
                                              utilities.interpolator(self.server_conn,
                                                                      self.server_conn['tables']['lines'] ,
                                                                      x['id_from'],
                                                                      x['id_to'],
                                                                      x['ratio'],
                                                                      self.city),axis = 1)
        pts_df_grouped = pts_df.groupby(['MNC','NetworkGen'])
        for i,mnc_gr in pts_df_grouped:
            _mnc_gr = mnc_gr.sort_values(by=['ratio'])

            # todo: think about better algorithm
            max_quality = max(_mnc_gr['quality'])
            _mnc_gr['quality'] = max_quality

            line_wkt = utilities.wkbpts_to_wkbline(list(_mnc_gr['geom']),wkt=True)
            lc_row = _mnc_gr[['segment_id','NetworkGen','MNC','city','NetworkGen','quality','NumUsers','NumRaces']].iloc[0]
            lc_row['geom'] = line_wkt
            utilities.remove_slice_from_postgres2(self.server_conn, self.server_conn['tables']['subway_cell_quality'],
                                                          segment_id = [lc_row['segment_id']],
                                                          city = lc_row['city'],
                                                          MNC = lc_row['MNC'],
                                                          NetworkGen = lc_row['NetworkGen']
                                                          )
            cur.execute(sql,lc_row.to_dict())
            conn.commit()
        conn.close()
    def push_cell_quality(self,pts_df):
        print "Init push_cell_quality"
        conn = psycopg2.connect(self.connString)
        conn.rollback()
        cur = conn.cursor()
        sql = """INSERT INTO """ + self.server_conn['tables']['subway_cell_quality'] + """(segment_id,"NetworkGen","MNC",geom,cell_num,city)
        VALUES(%(segment_id)s,%(NetworkGen)s,%(MNC)s,ST_GeomFromText(%(geom)s,3857),%(cell_num)s,%(city)s)"""

        pdlines_df = pd.DataFrame()
        pts_df['id_from'] = pts_df['segment_id'].apply(lambda x : x.split('-')[0])
        pts_df['id_to'] = pts_df['segment_id'].apply(lambda x : x.split('-')[1])
        # reference each point of input dataframe
        pts_df['geom'] = pts_df.apply(lambda x:
                                              utilities.interpolator(self.server_conn,
                                                                      self.server_conn['tables']['lines'] ,
                                                                      x['id_from'],
                                                                      x['id_to'],
                                                                      x['ratio'],
                                                                      self.city),axis = 1)
        qua_pts = pts_df.groupby(['segment_id','NetworkGen','MNC','ratio','geom','city'])['laccid'].apply(lambda x : len(np.unique(x)))
        qua_pts = qua_pts.reset_index()
        qua_pts.columns = ['segment_id','NetworkGen','MNC','ratio','geom','city','cell_num']
        #qua_pts = qua_pts.sort_values(by = ['ratio'])

        cell_gr = pd.DataFrame()
        qua_pts_grouped = qua_pts.groupby(['city','segment_id','NetworkGen','MNC'])
        for i,qua_pts_gr in qua_pts_grouped:
            _qua_pts_gr = qua_pts_gr.sort_values(by = ['ratio'])
            prev_cell_num = _qua_pts_gr['cell_num'].iloc[0]
            last_ratio = qua_pts_gr['ratio'].iloc[0]
            ratio_split = False
            for i,row in _qua_pts_gr.iterrows():
                if row['MNC'] =='99':
                    pass
                if prev_cell_num == row['cell_num']:
                    # todo: experement
                    delta_ratio = row['ratio']-last_ratio
                    # print last_ratio,row['ratio'],delta_ratio
                    if delta_ratio< 0.2:
                        cell_gr = pd.concat([cell_gr,pd.DataFrame(row).transpose()])
                        prev_cell_num = row['cell_num']
                        last_ratio = row['ratio']
                    else:
                        print "High delta ratio!",delta_ratio,row['MNC']
                        ratio_split = True
                if (prev_cell_num!=row['cell_num'])or(i == _qua_pts_gr.last_valid_index())or(ratio_split):
                    if len(cell_gr)>1:
                        cell_gr = cell_gr.sort_values(by=['ratio'])
                        line_wkt = utilities.wkbpts_to_wkbline(list(cell_gr['geom']),wkt=True)
                        lc_row = cell_gr[['segment_id','NetworkGen','MNC','cell_num','city']].iloc[0]
                        lc_row['geom'] = line_wkt
                        lc_row['srid'] = '3857'

                        utilities.remove_slice_from_postgres2(self.server_conn, self.server_conn['tables']['subway_cell_quality'],
                                                              segment_id = [lc_row['segment_id']],
                                                              city = lc_row['city'],
                                                              MNC = lc_row['MNC'],
                                                              NetworkGen = lc_row['NetworkGen']
                                                              )
                        cur.execute(sql,lc_row.to_dict())
                        conn.commit()
                    #pdlines_df = pd.concat([pdlines_df,lc_row])
                    # cell_gr = pd.DataFrame(row).transpose()
                    cell_gr = pd.DataFrame()
                    prev_cell_num = row['cell_num']
                    last_ratio = row['ratio']
                    ratio_split = False

        conn.close()
        return pdlines_df



