#!/usr/bin/python
# coding=utf-8
__author__ = 'yellow'

from os import listdir, path,mkdir
from argparse import ArgumentParser
import sys

from ngl_log_handler import NglLogHandler
from simple_time_strategy import SimpleTimeStrategy
from geoArgumentsExtractor import GeoArgumentsExtractor
from geoFilesConn import GeoFilesConn
from pointInterpolator import PointInterpolator
from utils import Utils
def main():
    # Processing Modes
    SINGLE = 'single'
    BATCH = 'batch'
    MODES = [SINGLE, BATCH]
    # run arguments
    parser = ArgumentParser(description='Process NextGISLogger logs')
    parser.add_argument('-l', '--lines', type=str, required=True, help='GeoJSON file in EPSG:3857 dissolved by stations')
    parser.add_argument('-m', '--mode', type=str, choices=MODES, default=SINGLE, help='Process one file or dir of files')
    parser.add_argument('-g', '--geojson', action='store_true', help='Write out GeoJSON files with all rows from input CSV files')
    parser.add_argument('-e', '--exclude-stops', action='store_true', help='Exclude "-stop" files')
    parser.add_argument('-b', '--bind-to-csv', action = 'store_true',help = 'Write out to one csv- dataFrame')
    parser.add_argument('-s', '--stations', type = str,help = 'Stations CSV file')
    #parser.add_argument('-sC','--stationsClosed',type = str,help = 'Stations CSV file which were closed')
    parser.add_argument('input_log', type=str, help='DIR or single Log file in CVS format for one segment')
    parser.add_argument('output_csv', type=str, help='DIR or single Result file with coordinates in CSV format')

    args = parser.parse_args()

    geoFilesConn = GeoFilesConn(args.lines, 'CODE',args.stations)
    extractor = GeoArgumentsExtractor(geoFilesConn)
    interpolator = PointInterpolator(geoFilesConn)

    if not args.exclude_stops:
        # create stop-dir
        stop_path = args.output_csv + '\\stop\\'
        Utils.initOutFld(['stop'],args.output_csv)
    #dbase = Dbase(args.output_csv)
    all_rows = []
    stop_rows =  []
    step = 0
    # get input files for processing
    input_files = []
    if args.mode == SINGLE:
        input_files.append(args.input_log)
    else:
        for fname in listdir(args.input_log):
            if fname.endswith(".csv"):
                if args.exclude_stops and ('-stop' in fname or '-inter' in fname):
                    continue
                input_files.append(path.join(args.input_log, fname))

    # iterate input files
    length = len(input_files)

    for fname in input_files:
        step+=1
        sys.stdout.write("\r" + str(step) + "/" + str(length))
        sys.stdout.flush()
        stopkey = 0
        if '-stop'  in fname or '-inter' in fname:
            stopkey = 1
        # get log rows
        log_entries = NglLogHandler.get_log_entries(fname)
        
        # get line id by name of file
        metro_line_name = path.basename(fname).split('-')
        metro_line_name = '{0}-{1}'.format(metro_line_name[0], metro_line_name[1])
       
        try:
            # georeferencing log rows
            interpol_entries = SimpleTimeStrategy.georeferencing(metro_line_name, log_entries, extractor, geoFilesConn, interpolator, stopkey)

        except:
            print '>Oops!', metro_line_name, sys.exc_value
            continue
   
        # write out
        if args.mode == SINGLE:
            output_path = args.output_csv

        else:
            if stopkey == 1:
                output_path = path.join(stop_path, path.basename(fname))
            else:
                output_path = path.join(args.output_csv, path.basename(fname))

        NglLogHandler.save_as_csv(output_path, interpol_entries)


        # store for geojson
        src_file = path.basename(fname)
        for entry in interpol_entries:
            entry['source_file'] = src_file
        if stopkey==0:
            all_rows.extend(interpol_entries)
        else:
            stop_rows.extend(interpol_entries)

    # write geojson
    if args.geojson:
        if args.mode == SINGLE:
            output_geojson_path = args.output_csv+'.geojson'
        else:
            output_geojson_path = path.join(args.output_csv, 'out.geojson')
        NglLogHandler.save_as_geojson(output_geojson_path, all_rows)
    #bind referenced rows into the one dataframe
    if args.bind_to_csv:
        output_move_path = path.join(args.output_csv, 'log_points.csv')
        output_stop_path = path.join(args.output_csv, 'log_points-stops.csv')
        NglLogHandler.save_as_csv(output_move_path, all_rows)
        if not args.exclude_stops:
            NglLogHandler.save_as_csv(output_stop_path, stop_rows)


if __name__ == '__main__':
    main()
