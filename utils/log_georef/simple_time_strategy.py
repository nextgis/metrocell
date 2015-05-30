# coding=utf-8
import copy
from datetime import datetime


__author__ = 'yellow'


class SimpleTimeStrategy():
    """
    Class for georeferencing log entries to 'real' metro lines
    with simple linear time strategy
    """



    @staticmethod
    def georeferencing(line_id, line_log_entries, point_interpolator, database):
        """
        Georeferencing log rows !for one segment! on any layer using interpolator
        :param line_id: id of segment
        :param line_log_entries: log rows for segment
        :param point_interpolator: interpolator for any layer
        :return:
        """
        # get start time, finish time and total time for segment
        start_time_ts = float(line_log_entries[0]['TimeStamp'])/1e3
        #start_time = datetime.fromtimestamp(start_time_ts)

        finish_time_ts = float(line_log_entries[-1]['TimeStamp'])/1e3
        finish_time = datetime.fromtimestamp(finish_time_ts)

        #segment_time_period = finish_time - start_time
        new_entries = []

        #    segment_info = SegmentInfo()
        #get key points
        
        key_pts = point_interpolator.extract_key_pts(line_log_entries)
       
        #get parameters of acceleration,constant speed and decceleration.
        segment_pars = point_interpolator.segment_pars(key_pts, line_id)
        #get segment length
        segment_dist = point_interpolator.get_segment_length(line_id)
        # interpolate log entries
        for log_entry in line_log_entries:
            log_entry_ts = float(log_entry['TimeStamp'])/1e3
            start_time_delta = log_entry_ts - start_time_ts
            #define entry agorithm(accel,const speed or deccel)
            if (log_entry['ID']  <> ''):
                alg = int(log_entry['ID'][-1] )

            start_dist_delta = point_interpolator.get_dist_delta(alg,log_entry_ts,key_pts,segment_pars)
            dist_ratio = start_dist_delta/segment_dist
      
            log_entry_point = point_interpolator.interpolate_by_ratio(line_id, dist_ratio)

            new_entry = copy.copy(log_entry)
            new_entry['x'] = log_entry_point.x
            new_entry['y'] = log_entry_point.y

            new_entry['segment_start_id'] = line_id.split('-')[0]
            new_entry['segment_end_id'] = line_id.split('-')[1]
            new_entry['ration'] = dist_ratio

            new_entries.append(new_entry)

            database.connection.cursor().execute('INSERT INTO log_points(x,y,active,power,ration,mcc,mnc,lac,cid,psc,seg_begin,seg_end) '
                                                 'VALUES(:x,:y,:Active,:Power,:ration,:MCC,:MNC,:LAC,:CID,:PSC,:segment_start_id,:segment_end_id)',
                                                 new_entry)
            database.connection.commit()

        return new_entries