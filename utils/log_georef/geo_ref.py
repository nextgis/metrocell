#!/usr/bin/python
# coding=utf-8
__author__ = 'sasfeat'

import sys
import ogr
import math
import string
import psycopg2

from shapely.geometry import shape
from shapely.wkt import loads


from sqlalchemy import create_engine,Column,Integer,Text,Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import utilities,variables


Base  =  declarative_base()
class Parsed_data(Base):
    __tablename__ = 'parsed_cell'
    id = Column(Integer,primary_key=True)
    TimeStamp = Column(Numeric)
    User = Column(Text)
    ratio = Column(Numeric)
    station_id = Column(Integer)
    id_from = Column(Text)
    id_to = Column(Text)
    city = Column(Text)
    move_type = Column(Text)
    zip_id = Column(Integer)
    def __init__(self,id,TimeStamp,User,ratio,station_id,id_from,id_to,city,move_type,zip_id):

        self.id = id
        self.TimeStamp = TimeStamp
        self.User = User
        self.ratio = ratio
        self.station_id = station_id
        self.id_from = id_from
        self.id_to = id_to
        self.city = city
        self.move_type = move_type
        self.zip_id = zip_id

class Geo_ref():
    def __init__(self,server_conn,zip_ids,city):
        self.server_conn = server_conn
        self.zip_ids = zip_ids
        self.city = city
        self.extractor = GeoArgumentsExtractor()

        self.connString = "host = %s user = %s password = %s dbname = %s port = %s" %\
                         (self.server_conn['host'],self.server_conn['user'],self.server_conn['password'],self.server_conn['dbname'],self.server_conn['postgres_port'])

        self.session = None
        #self.projection_4326 = partial(
        #        pyproj.transform,
        #        pyproj.Proj(init = 'epsg:3857'),
        #        pyproj.Proj(init = 'epsg:4326')
        #    )
    def geo_ref(self):
        """
        Extract parsed logs from DB, iterate through them(and by device),split data on
        groups, which contains 1 user, 1 segment, and 1 movement type(stop, move or interchanges),
        and initialize the processing algorithm of each group. Morover at the end of th function
        there is a visualizing of georeferenced result (currently only for cells, not for another devices
        such as accelerometr,gyroscope and etc)
        :return:
        """

        print 'Georeferencer starts!'
        print 30*"-"
        # use sqlalchemy sessions to faster commiting of changes
        # look at https://ru.wikibooks.org/wiki/SQLAlchemy
        metrocell_engine = create_engine("postgresql://"+self.server_conn['user']+':'+utilities.if_none_to_str(self.server_conn['password'])+'@'+self.server_conn['host']+':'+ utilities.if_none_to_str(self.server_conn['postgres_port'])+'/' + self.server_conn['dbname'])
        Metrocell_session = sessionmaker(bind = metrocell_engine)
        self.session = Metrocell_session()
        # iterate through devices
        # todo: georeference another sensors
        #for device in variables.available_devices:
        for device in ['cell']:
            Parsed_data.__table__ = 'parsed_'+device
            try:
                parsed_df = utilities.get_pd_df_from_sql(self.server_conn,self.server_conn['tables']['parsed_' + device])
            except:
                pass
            parsed_zip_df = parsed_df[parsed_df['zip_id'].isin(self.zip_ids)]
            _parsed_zip_df = parsed_zip_df.sort_values(by = ['TimeStamp'])
            _parsed_zip_df = _parsed_zip_df[parsed_zip_df['ratio'].isnull()]
            segment_slices = _parsed_zip_df.groupby(['id_from','id_to','User','move_type'])
            self.proc_len = len(parsed_zip_df)
            self.step = 0
            # iterate thtough the groups
            for (id_from,id_to,User,move_type),segment_df in segment_slices:
                self.id_from = id_from
                self.id_to = id_to
                self.device = device
                self.key_rows = segment_df[segment_df['station_id'].notnull()]

                _segment_df = self.session.query(Parsed_data)\
                    .filter_by(id_from = id_from,id_to=id_to,User = User,move_type = move_type,ratio = None)\
                    .order_by(Parsed_data.TimeStamp.asc())
                if move_type == 'inter':
                    self.step+=1
                    #todo: no algorithm to reference interchanges!
                    continue
                segment_graph = str(id_from).zfill(3) + '-' + str(id_to).zfill(3)
                try:
                    self.segment_georef(segment_graph,_segment_df, self.extractor)
                except:
                    self.step+=1
                    print sys.exc_info()[0],sys.exc_info()[1]
                    continue
                if move_type == 'move':
                    utilities.plot_signal_power(self.server_conn,'georeferencing_raw',id_from,id_to,self.city)


        for z_id in self.zip_ids:
            utilities.update_postgre_rows(self.server_conn,self.server_conn['tables']['processing_status'],z_id,'georeferenced',True,index_col = 'zip_id')


    def segment_georef(self,segment_graph, segment_slice_fr, extractor):
        """

        :param segment_graph: e.g. 070-071
        :param segment_slice_fr: data for 1 user 1 segment 1 movement type {pd.DataFrame}
        :param extractor: instance of extractor
        :return:
        """
        alg = -1
        psycopg_conn = psycopg2.connect(self.connString)
        cur = psycopg_conn.cursor()
        psycopg_conn.rollback()
        sql_georef_insert = """INSERT INTO """ + self.server_conn['tables']['georeferenced']  + """ (geom,ratio,id_from,id_to,city,zip_id) VALUES(ST_SetSRID(%(geom)s::geometry,%(srid)s),%(ratio)s,%(id_from)s,%(id_to)s,%(city)s,%(zip_id)s)"""

        move_type = segment_slice_fr.first().move_type
        if move_type == 'move':
            filter_query = "code = '%s' AND city = '%s'"% (segment_graph,self.city)
            conn = ogr.Open("PG:"+self.connString)
            lines = conn.GetLayer(self.server_conn['tables']['lines'])
            lines.SetAttributeFilter(filter_query)
            feat_count = lines.GetFeatureCount()
            line_feature = lines.GetNextFeature()
            if feat_count!=1:

                old_stdout = sys.stdout
                log_file = open(variables.LOGSPATH + 'segment_selection_errors.log',"a")
                sys.stdout = log_file
                print ">oops! segment selection error !"+ segment_graph + "Feature count = "+str(feat_count)
                sys.stdout = old_stdout
                log_file.close()
                raise Exception(">oops! segment selection error !"+ segment_graph + "Feature count = "+str(feat_count))
            key_stamps = list(self.key_rows['TimeStamp']/1e3)
            if (len(key_stamps)<4):
                raise IndexError("The log was collected incorrect. The lack of marks!")
            key_pts = sorted(list(set(key_stamps)))
            #get segment length
            segment_dist = line_feature.geometry().Length()
            #get parameters of acceleration,constant speed and decceleration.
            segment_pars = extractor.segment_pars(key_pts, segment_dist)
        for log_entry in segment_slice_fr:
            self.step+=1
            sys.stdout.write("\r" + str(self.step) + "/" + str(self.proc_len) + " : ")
            sys.stdout.flush()
            if log_entry.move_type == 'move':
                log_entry_ts = float(log_entry.TimeStamp)/1e3
                try:
                    st_id = str(int(float(log_entry.station_id)))
                    alg = int(st_id[-1])
                except:
                    pass
                start_dist_delta = extractor.get_dist_delta(alg,log_entry_ts,key_pts,segment_pars)
                dist_ratio = start_dist_delta/segment_dist
            else:

                #segment_dist = 1
                if alg == 4:
                    dist_ratio = 1
                if alg == -1:
                    dist_ratio = 0
            if move_type!='move':
                stationFrom = segment_graph.split('-')[0]
                try:
                    conn = ogr.Open("PG:"+self.connString)
                    stations = conn.GetLayer(self.server_conn['tables']['stations'])
                    stations.SetAttributeFilter("id_station = %s AND city = '%s'"%(stationFrom,self.city))
                    station_feature = stations.GetNextFeature()
                    point_str = station_feature.geometry().ExportToWkt()
                    point = loads(point_str)
                    conn.Destroy()
                except:
                    raise Exception( "There is no station with such ID: %s" % stationFrom)
            else:
                point_linear_offset = segment_dist * dist_ratio
                point = shape(loads(line_feature.geometry().ExportToWkt())).interpolate(point_linear_offset)
            #point_linear_offset = segment_dist * dist_ratio
            #point = shape(loads(line_feature.geometry().ExportToWkt())).interpolate(point_linear_offset)

            new_entry = {'id_from':self.id_from,
                         'id_to':self.id_to,
                         'city':self.city,
                         'geom':point.wkb_hex,'srid':3857,
                         'ratio':round(dist_ratio,6),
                         'TimeStamp':int(log_entry.TimeStamp),
                         'zip_id':log_entry.zip_id
                         }
            try:
                self.session.rollback()
                log_entry.ratio = new_entry['ratio']
                log_entry.zip_id = new_entry['zip_id']
                self.session.commit()
                psycopg_conn.rollback()
                cur.execute(sql_georef_insert,new_entry)
                psycopg_conn.commit()

            except:
                #print sys.exc_info()[0],sys.exc_info()[1],"Point update : ",new_entry
                pass
class GeoArgumentsExtractor():

    def __init__(self):

        return
    def extract_key_pts(self):
        """
        return:list of timestamps of changing type of movement:
        0 - accel_start,1-accel_max,2-deccel_start,3-deccel_end
        """
        key_stamps = list(self.key_rows['TimeStamp']/1e3)
        if (len(key_stamps)<4):
            raise IndexError("The log was collected incorrect. The lack of marks!")
        return sorted(list(set(key_stamps)))

    def segment_pars(self,key_pts,segment_length):
        """
        return: movement parameters for each of three parts:acceleration, constant speed and decceleration.
        simple algorithm use track distance, key timestamps of changing movement type and
        """
        segment_pars = {'a1':None,'a2':None,'v_const':None}
        segment_pars['v_const'] = 2 * segment_length/(key_pts[2] + key_pts[3] - key_pts[1] - key_pts[0])
        segment_pars['a1'] = segment_pars['v_const']/(key_pts[1] - key_pts[0])
        segment_pars['a2'] = - segment_pars['v_const']/(key_pts[3] - key_pts[2])
        return segment_pars
    def stationsId(self,name):
        """
        extract stations Identifiers by the filename
        :param fname: filename {str}
        :return:stations: dictionary with keys '_from' (station from) and '_to' ( station to ){dict}
        """
        stations = {}
        l = string.split(name,'-')
        stations['_from'] = l[0]
        stations['_to'] = l[1]
        return stations
    def get_dist_delta(self,alg_key,log_entrty_ts,key_pts,segment_pars):
        """
        the values represent the next type of movements:
            3: the train starts moving forward
            4: the train moves with constant speed
            1: the train starts stopping
        """
        if alg_key == 3:
            time_delta = log_entrty_ts - key_pts[0]
            self.dist_delta = segment_pars['a1'] * math.pow(time_delta,2)/2
            self.passed_accel = self.dist_delta
        elif alg_key == 4:
            time_delta = log_entrty_ts - key_pts[1]
            self.dist_delta = segment_pars['v_const'] * (time_delta) + self.passed_accel
            self.passed_const = self.dist_delta
        elif (alg_key == 1):
            time_delta = log_entrty_ts - key_pts[2]
            self.dist_delta = self.passed_const + segment_pars['v_const']*(time_delta) + segment_pars['a2']*math.pow((time_delta),2)/2
        return self.dist_delta
