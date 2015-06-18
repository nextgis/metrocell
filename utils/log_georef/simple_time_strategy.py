# coding=utf-8
import copy
import random

__author__ = 'yellow'

class SimpleTimeStrategy():
    """
    Class for georeferencing log entries to 'real' metro lines
    with simple linear time strategy
    """
    @staticmethod
    def georeferencing(line_id, line_log_entries, extractor,geojsonConn,pointInterpolator,stopkey):
        """
        Georeferencing log rows !for one segment! on any layer using interpolator
        :param line_id: id of segment
        :param line_log_entries: log rows for segment
        :param point_interpolator: interpolator for any layer
        :return:
        """
        # get start time, finish time and total time for segment
        new_entries = []
        race_id = random.randint(1,999999999)
        if stopkey ==0:
            key_pts = extractor.extract_key_pts(line_log_entries)
            #get parameters of acceleration,constant speed and decceleration.
            segment_pars = extractor.segment_pars(key_pts, line_id)
            #get segment length
            segment_dist = geojsonConn.get_segment_length(line_id)
            # interpolate log entries
        for log_entry in line_log_entries:
            new_entry = copy.copy(log_entry)
            if stopkey == 0:
                log_entry_ts = float(log_entry['TimeStamp'])/1e3
                #define entry agorithm(accel,const speed or deccel)
                if (log_entry['ID']  <> ''):
                    alg = int(log_entry['ID'][-1])

                start_dist_delta = extractor.get_dist_delta(alg,log_entry_ts,key_pts,segment_pars)
                dist_ratio = start_dist_delta/segment_dist
            else:
                dist_ratio = 0
            log_entry_point = pointInterpolator.interpolate_by_ratio(line_id, dist_ratio,stopkey)

            new_entry['race_id'] = race_id
            new_entry['x'] = log_entry_point.x
            new_entry['y'] = log_entry_point.y
            new_entry['segment_start_id'] = "$"+str(line_id.split('-')[0])
            new_entry['segment_end_id'] = line_id.split('-')[1] +"$"
            new_entry['ratio'] = dist_ratio

            new_entries.append(new_entry)
            #if stopkey ==0:

                #database.connection.cursor().execute('INSERT INTO log_points(race_id,x,y,active,power,ration,mcc,mnc,lac,cid,psc,seg_begin,seg_end) '
                                                    # 'VALUES(:race_id,:x,:y,:Active,:Power,:ration,:MCC,:MNC,:LAC,:CID,:PSC,:segment_start_id,:segment_end_id)',
                                                    # new_entry)
                #database.connection.commit()

        return new_entries

