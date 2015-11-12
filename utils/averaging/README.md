Averaging
=====
Averaging of georeferenced data(now only by Power)

```bash

positional arguments:
    input               Path to the referenced data

optional arguments:
    -p  --preproc       Preprocess referenced data
    -a  --average       Average referenced data
    -g  --georef        Post-georeferencing of input data
    -d  --pushToDb      push result to the database
    -m  --mkey          Mark key. For example key for stops-data is 'stop',whereas for interchanges-data is 'inter'. That keyword will be passed to the output name
    -s  --subwayInfoDf  DataFrame containing subway information
    -o  --output        Path to the DIR to write out processed data

Example:

One by one:

1 step:(if you haven't pre-processed your data)
...\metrocell\utils\averaging\averaging.py 
-p
-g
-d
-o
C:\temp\metrocell\averaged\spb
-l
C:\Users\Alex\Documents\GitHub\metrocell\segments\spb\lines_3857_dissolved.geojson
C:\Users\Alex\Documents\GitHub\metrocell\data\referenced\spb\cell\log_points.csv

2 step: (if you already have pre-processed data)
...\metrocell\utils\averaging\averaging.py
-a
-g
-s
C:\temp\metrocell\averaged\spb\subwayInfo.csv
-o
C:\temp\metrocell\averaged\spb
-l
C:\Users\Alex\Documents\GitHub\metrocell\segments\spb\lines_3857_dissolved.geojson
C:\temp\metrocell\averaged\spb\MegaFon\pre_log_points-2G.csv

All at once
...\metrocell\utils\averaging\averaging.py
-a
-g
-p
-d
-o
C:\temp\metrocell\averaged\spb
-l
C:\Users\Alex\Documents\GitHub\metrocell\segments\spb\lines_3857_dissolved.geojson
C:\Users\Alex\Documents\GitHub\metrocell\data\referenced\spb\cell\log_points.csv
```

Estimator
=========
Estimation of prediction algorithms

```bash

-a    --algorithm        Prediction algorithm.
                                lc : by laccids 
                                lcM : laccid modified(with neighbours)
                                pc : power correlation
-t    --testDf           path to test sets CSV file
-m    --mainDf           path to CSV file with smoothed signals
-st   --segmentsStepsDf  Df contains interpolations steps per segment
-c    --console          writing result into the console or not
-i    --iterations       number of iterations. one iteration - one imitation of prediction
-s    --spread           number of rows(seconds) received from the user\'s phone
-o    --output           DIR to write out output dataFrames

Example:
estimator.py -st ...\segmentsStepsDf.csv -t ...\testSets.csv -m ...\Cells_smoothed_ref-200-sm2.csv  -i 30 -s 15 -o ...\predicted
```

