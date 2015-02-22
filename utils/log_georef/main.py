#!/usr/bin/python
# coding=utf-8
__author__ = 'yellow'

from argparse import ArgumentParser

from ngl_log_handler import NglLogHandler
from simple_time_strategy import SimpleTimeStrategy
from coordinate_interpolator import CoordinateInterpolator


def main():
    # run arguments
    parser = ArgumentParser(description='Process NextGISLogger logs')
    parser.add_argument('log_file', type=str, help='log file in CVS format for one segment')
    parser.add_argument('metro_lines', type=str, help='geojson file in EPSG:3857 dissolved by stations')
    parser.add_argument('output_csv', type=str, help='result file with coordinates in CSV format')
    args = parser.parse_args()

    interpolator = CoordinateInterpolator(args.metro_lines, 'CODE')

    # get log rows
    log_entries = NglLogHandler.get_log_entries(args.log_file)

    # get line id by name of file
    metro_line_name = args.log_file.split('-')
    metro_line_name = '{0}-{1}'.format(metro_line_name[0], metro_line_name[1])

    # georeferencing log rows
    interpol_entries = SimpleTimeStrategy.georeferencing(metro_line_name, log_entries, interpolator)

    # write out
    NglLogHandler.save_as_csv(args.output_csv, interpol_entries)

if __name__ == '__main__':
    main()
