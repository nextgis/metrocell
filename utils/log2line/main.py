import json
import datetime
from shapely.geometry import shape

__author__ = 'yellow'
import csv
from sys import argv
from argparse import ArgumentParser

def main(args):
    # params
    parser = ArgumentParser(description='Process NextGISLogger logs')
    parser.add_argument('log_file', type=str, help='log file in CVS format ')
    parser.add_argument('metro_lines', type=str, help='geojson file in EPSG:3857 dissolved by stations')
    parser.add_argument('output_csv', type=str, help='result file with coordinates in CSV format')
    args = parser.parse_args()

    #read log file
    log_file = open(args.log_file, 'rb')
    log_reader = csv.DictReader(log_file)
    log_lines = [row for row in log_reader]

    #read metro lines
    lines_file = open(args.metro_lines, 'r')
    lines_json = json.load(lines_file)
    lines_feats = []
    for feature in lines_json['features']:
        geom = shape(feature['geometry'])
        feature['shape'] = geom
        lines_feats.append(feature)

    #time stuff
    min_time_ts = float(log_lines[0]['TimeStamp'])/1e3
    min_time = datetime.datetime.fromtimestamp(min_time_ts)

    max_time_ts = float(log_lines[-1]['TimeStamp'])/1e3
    max_time = datetime.datetime.fromtimestamp(max_time_ts)

    total_line_time = max_time - min_time

    #search line
    metro_line_name = args.log_file.split('-')
    metro_line_name = '{0}-{1}'.format(metro_line_name[0], metro_line_name[1])
    metro_line = None
    for line in lines_feats:
        if line['properties']['CODE'] == metro_line_name:
            metro_line = line

    if not metro_line:
        raise Exception('Line not found!')

    #get line lenght
    metro_line_length = metro_line['shape'].length

    #interpolate log lines
    for log_entry in log_lines:
        log_entry_ts = float(log_entry['TimeStamp'])/1e3
        log_entry_dt = datetime.datetime.fromtimestamp(log_entry_ts)

        start_line_delta = log_entry_dt - min_time
        start_line_offset = metro_line_length * (start_line_delta.total_seconds() / total_line_time.total_seconds())

        log_entry_point = metro_line['shape'].interpolate(start_line_offset)

        log_entry['x'] = log_entry_point.x
        log_entry['y'] = log_entry_point.y

    #write out
    out_file = open(args.output_csv, 'w')
    out_writer = csv.DictWriter(out_file, fieldnames=log_lines[0].keys())

    out_writer.writeheader()
    out_writer.writerows(log_lines)

if __name__ == '__main__':
    main(argv)
