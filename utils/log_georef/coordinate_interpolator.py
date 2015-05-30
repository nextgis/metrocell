# coding=utf-8
__author__ = 'yellow'

import json
from shapely.geometry import shape
from shapely.ops import transform
import math
from functools import partial
import pyproj

class CoordinateInterpolator():

    def __init__(self, segments_geojson_path, id_field_name):
        """
        Инициализация интерполятора слоем линейных объектов.
        Каждый сегмент должен быть polyline и иметь ID.
        Например:
            Слой сегментов тунелей метро, от станции А до станции Б.
        :param segments_geojson_path: путь к файлу с динейными сегментами в формате GeoJSON
        :param id_field_name: название поля с идентификаторами
        :return:
        """
        self._lines_feats = {}
        self.dist_delta = 0
        self.passed_accel = 0
        self.passed_const = 0
        self.project = partial(
            pyproj.transform,
            pyproj.Proj(init = 'epsg:3857'),
            pyproj.Proj(init = 'epsg:4326')
        )
        # read lines
        with open(segments_geojson_path, 'r') as lines_file:
            lines_json = json.load(lines_file)

            for feature in lines_json['features']:
                feat_id = feature['properties'][id_field_name]
                geom = shape(feature['geometry'])
                # check data
                if feat_id in self._lines_feats.keys():
                    raise Exception("Input layer contains conflicting data: row with same id %s" % feat_id)
                self._lines_feats[feat_id] = geom
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
        segment_length = self.get_segment_length(line_id)
        segment_pars['v_const'] = 2 * segment_length/(key_pts[2] + key_pts[3] - key_pts[1] - key_pts[0])
        segment_pars['a1'] = segment_pars['v_const']/(key_pts[1] - key_pts[0])
        segment_pars['a2'] = - segment_pars['v_const']/(key_pts[3] - key_pts[2])
        return segment_pars
    def get_segment_length(self,line_id):
        return self._lines_feats[line_id].length
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
	
    def interpolate_by_ratio(self, line_id, ratio):
        """
        Интерполяция точки по линии, по отношению процента её положения к общей длине линии
        Например:
            длина линии = 100м
            ratio = 0.33
            положение = точка на линии, отстоящая на 33 метра от начала
        :param line_id: Идентификатор линии, на которой интерполируем
        :param ratio: процент положения
        :return: Point(x,y) - shapely point в координатах исходного слоя
        """
        if line_id not in self._lines_feats.keys():
            raise IndexError("Where is no object with such ID: %s" % line_id)

        line_length = self.get_segment_length(line_id)
		
        point_linear_offset = line_length * ratio
        point = self._lines_feats[line_id].interpolate(point_linear_offset)
        #transformation to wgs-84
        point2 = transform(self.project,point)

        return point2
