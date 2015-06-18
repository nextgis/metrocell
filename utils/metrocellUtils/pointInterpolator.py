#coding=utf-8
__author__ = 'Alex'

import pyproj
from shapely.geometry import shape,Point
from shapely.ops import transform
from functools import partial

class PointInterpolator():

    def __init__(self,geojson_conn):
        """
        Инициализация интерполятора слоем линейных объектов.
        Каждый сегмент должен быть polyline и иметь ID.
        Например:
            Слой сегментов тунелей метро, от станции А до станции Б.
        :param segments_geojson_path: путь к файлу с динейными сегментами в формате GeoJSON
        :param id_field_name: название поля с идентификаторами
        :return:
        """
        self.dist_delta = 0
        self.passed_accel = 0
        self.passed_const = 0
        self.project = partial(
            pyproj.transform,
            pyproj.Proj(init = 'epsg:3857'),
            pyproj.Proj(init = 'epsg:4326')
        )
        self.geojson_conn = geojson_conn
    def interpolate_by_ratio(self, line_id, ratio,stopkey):
        """
        Интерполяция точки по линии, по отношению процента её положения к общей длине линии
        Например:
            длина линии = 100м
            ratio = 0.33
            положение = точка на линии, отстоящая на 33 метра от начала
        :param line_id: Идентификатор линии, на которой интерполируем
        :param ratio: процент положения
        :param stopkey: 1 - привязываем точку на станции. 0 - на сегменте.
        :return: Point(x,y) - shapely point в координатах исходного слоя
        """
        if line_id not in self.geojson_conn._lines_feats.keys():
            raise IndexError("There is no object with such ID: %s" % line_id)

        if stopkey==1:
            point = Point(self.geojson_conn._lines_feats[line_id][0].coords[-1])
        else:
            line_length = self.geojson_conn.get_segment_length(line_id)
            point_linear_offset = line_length * ratio
            point = self.geojson_conn._lines_feats[line_id].interpolate(point_linear_offset)
        #transformation to wgs-84
        point2 = transform(self.project,point)

        return point2
