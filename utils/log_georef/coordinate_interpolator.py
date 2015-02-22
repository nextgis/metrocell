# coding=utf-8
__author__ = 'yellow'

import json
from shapely.geometry import shape


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

        line_length = self._lines_feats[line_id].length
        point_linear_offset = line_length * ratio
        point = self._lines_feats[line_id].interpolate(point_linear_offset)
        return point
