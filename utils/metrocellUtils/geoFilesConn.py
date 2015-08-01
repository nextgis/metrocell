#coding=utf-8
__author__ = 'Alex'

import json
from shapely.geometry import shape
import pandas as pd

class GeoFilesConn():
    def __init__(self,linesPath,lines_id_field,stationsPath = ''):
        self._lines_feats = self.geojsonReader(linesPath,lines_id_field)
        if stationsPath:
            self._stations = self.stationsReader(stationsPath,sep = ';')
        #self._stations_feats = {}

    def get_segment_length(self,_id):
        return self._lines_feats[_id].length
    def stationsReader(self,csvPath,sep = ';'):
        stations = pd.io.parsers.read_csv(csvPath,sep = sep)
        return stations
    def getStationCoordinates(self,station):
        y = self._stations[self._stations['id_station'] == station]['lat']
        x = self._stations[self._stations['id_station'] == station]['lon']
        return x,y
    def geojsonReader(self,geojson_path,id_field):
        _feats = {}
        with open(geojson_path, 'r') as _file:
            _json = json.load(_file)

            for feature in _json['features']:
                feat_id = feature['properties'][id_field]
                geom = shape(feature['geometry'])
                # check data
                if feat_id in _feats.keys():
                    raise Exception("Input layer contains conflicting data: row with same id %s" % feat_id)
                _feats[feat_id] = geom
        return _feats
