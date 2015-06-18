# coding=utf-8
__author__ = 'yellow'
import math
from geojsonC import GeojsonConn

class GeoArgumentsExtractor():

    def __init__(self, geojsonConn):
        self.geojsonConn = geojsonConn
        return
    def extract_key_pts(self,log_entries):
        """
        return:list of timestamps of changing type of movement:
        0 - accel_start,1-accel_max,2-deccel_start,3-deccel_end
        """
        key_pts = [row for row in log_entries if row['ID'] != '']
        key_stamps = [float(row['TimeStamp'])/1e3 for row in key_pts]
        if (len(key_stamps)<4):
            raise IndexError("The log was collected incorrect. The lack of marks!")
        return sorted(list(set(key_stamps)))
 
    def segment_pars(self,key_pts,line_id):
        """
        return: movement parameters for each of three parts:acceleration, constant speed and decceleration.
        simple algorithm use track distance, key timestamps of changing movement type and 
        """
        segment_pars = {'a1':None,'a2':None,'v_const':None}
        segment_length = self.geojsonConn.get_segment_length(line_id)
        segment_pars['v_const'] = 2 * segment_length/(key_pts[2] + key_pts[3] - key_pts[1] - key_pts[0])
        segment_pars['a1'] = segment_pars['v_const']/(key_pts[1] - key_pts[0])
        segment_pars['a2'] = - segment_pars['v_const']/(key_pts[3] - key_pts[2])
        return segment_pars
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
        if alg_key == 4:
            time_delta = log_entrty_ts - key_pts[1]
            self.dist_delta = segment_pars['v_const'] * (time_delta) + self.passed_accel
            self.passed_const = self.dist_delta
        if (alg_key == 1):
            time_delta = log_entrty_ts - key_pts[2]
            self.dist_delta = self.passed_const + segment_pars['v_const']*(time_delta) + segment_pars['a2']*math.pow((time_delta),2)/2
        return self.dist_delta

