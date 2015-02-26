# coding=utf-8
__author__ = 'yellow'

import json
import csv
from shapely.geometry import Point, mapping



class NglLogHandler():
    """
    NextGIS Logger's log parser
    """

    @staticmethod
    def get_log_entries(log_file_path):
        """
        Read log ant return all entries as [] of dicts for every row
        :return:
        [ log_entry_dict, log_entry_dict, .... ]
        """
        with open(log_file_path, 'rb') as log_file:
            log_reader = csv.DictReader(log_file)
            log_lines = [row for row in log_reader]
            return log_lines

    @staticmethod
    def save_as_csv(out_log_file_path, log_entries):
        """
        Save log entries to CSV file
        :param out_log_file_path: Path to file for saving
        :param log_entries: [] of dicts with log entries
        :return: None
        """
        if len(log_entries) < 1:
            return
        field_names = log_entries[0].keys()  # need more right version (fields position!!!)
        with open(out_log_file_path, 'w') as log_file:
            out_writer = csv.DictWriter(log_file, fieldnames=field_names)
            out_writer.writeheader()
            out_writer.writerows(log_entries)

    @staticmethod
    def save_as_geojson(out_file_path, log_entries):
        """
        Save extended log entries to GeoJson file
        :param out_file_path: Path to file for saving
        :param log_entries: [] of dicts with log entries and x, y fields
        :return: None
        """
        if len(log_entries) < 1:
            return

        json_stub = {
            'type': 'FeatureCollection',
            'crs': {'type': 'name', 'properties': {'name': 'urn:ogc:def:crs:EPSG::3857'}},
            'features': []
        }

        for row in log_entries:
            point = Point(row['x'], row['y'])

            json_stub.features.append({
                'type': 'Feature',
                'geometry': mapping(point),
                'properties': row
            })

            json.dumps(json_stub)