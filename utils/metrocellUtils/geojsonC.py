#coding=utf-8
__author__ = 'Alex'

import json
from shapely.geometry import shape


class GeojsonConn():
    def __init__(self,segments_geojson_path,segment_id_field):
        self._lines_feats = {}
        with open(segments_geojson_path, 'r') as lines_file:
            lines_json = json.load(lines_file)

            for feature in lines_json['features']:
                feat_id = feature['properties'][segment_id_field]
                geom = shape(feature['geometry'])
                # check data
                if feat_id in self._lines_feats.keys():
                    raise Exception("Input layer contains conflicting data: row with same id %s" % feat_id)
                self._lines_feats[feat_id] = geom
    def get_segment_length(self,line_id):
        return self._lines_feats[line_id].length