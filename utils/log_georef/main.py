#!/usr/bin/python
# coding=utf-8
__author__ = 'yellow'

from os import listdir, path
from argparse import ArgumentParser

from ngl_log_handler import NglLogHandler
from simple_time_strategy import SimpleTimeStrategy
from coordinate_interpolator import CoordinateInterpolator


def main():
    # Processing Modes
    SINGLE = 'single'
    BATCH = 'batch'
    MODES = [SINGLE, BATCH]

    # run arguments
    parser = ArgumentParser(description='Process NextGISLogger logs')
    parser.add_argument('-l', '--lines', type=str, required=True, help='GeoJSON file in EPSG:3857 dissolved by stations')
    parser.add_argument('-m', '--mode', type=str, choices=MODES, default=SINGLE, help='Process one file or dir of files')
    parser.add_argument('input_log', type=str, help='DIR or single Log file in CVS format for one segment')
    parser.add_argument('output_csv', type=str, help='DIR or single Result file with coordinates in CSV format')
    args = parser.parse_args()

    interpolator = CoordinateInterpolator(args.lines, 'CODE')

    # get input files for processing
    input_files = []
    if args.mode == SINGLE:
        input_files.append(args.input_log)
    else:
        for fname in listdir(args.input_log):
            if fname.endswith(".csv"):
                input_files.append(path.join(args.input_log, fname))

    # iterate input files
    for fname in input_files:
        # get log rows
        log_entries = NglLogHandler.get_log_entries(fname)

        # get line id by name of file
        metro_line_name = path.basename(fname).split('-')
        metro_line_name = '{0}-{1}'.format(metro_line_name[0], metro_line_name[1])

        # georeferencing log rows
        interpol_entries = SimpleTimeStrategy.georeferencing(metro_line_name, log_entries, interpolator)

        # write out
        if args.mode == SINGLE:
            output_path = args.output_csv
        else:
            output_path = path.join(args.output_csv, path.basename(fname))
        NglLogHandler.save_as_csv(output_path, interpol_entries)


if __name__ == '__main__':
    main()
