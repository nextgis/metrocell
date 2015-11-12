log_georef
=========

Utility for georeferencing metro logs  

```bash
usage: main.py [-h] -l LINES [-m {single,batch}] [-g] [-e]
               input_log output_csv

Process NextGISLogger logs

positional arguments:
  input_log             DIR or single Log file in CVS format for one segment
  output_csv            DIR or single Result file with coordinates in CSV
                        format

optional arguments:
  -h, --help            show this help message and exit
  -l LINES, --lines LINES
                        GeoJSON file in EPSG:3857 dissolved by stations
  -m {single,batch}, --mode {single,batch}
                        Process one file or dir of files
  -g --geojson         Write out GeoJSON files with all rows from input CSV
                        files
  -e --exclude-stops   Exclude "-stop" files
  -b --bind-to-csv     Write out to one csv - dataFrame
  -s --stations        Stations CSV file

Example:
/geo_ref.py -b -e -g -m batch -l ../../segments/raw/msk/metro_lines_disolved_3857.geojson -s .../metro4all/data/msk/stations.csv  ../../data/proc/msk/cell/ ../../data/referenced/msk/cell
```