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
  -g, --geojson         Write out GeoJSON files with all rows from input CSV
                        files
  -e, --exclude-stops   Exclude "-stop" files


Example:
/main.py -e -g -m batch -l ../../segments/raw/msk/metro_lines_disolved_3857.geojson  ../../data/proc/msk/cell/ ~/GIS/metro/out_data/
```