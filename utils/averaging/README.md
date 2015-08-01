Averaging
=====
Averaging of gereferenced data(now only by Power)

```bash

positional arguments:
    input               Path to the referenced data

optional arguments:
    -p  --preproc       Preprocess referenced data
    -a  --average       Average referenced data
    -g  --georef        Post-georeferencing of input data
    -d  --pushToDb      push result to the database
    -m  --mkey'         Mark key. For example key for stops-data is 'stop',whereas for interchanges-data is 'inter'. That keyword will be passed to the output name
    -s  --subwayInfoDf  DataFrame containing subway information
    -o  --output        Path to the DIR to write out processed data

Example:

One by one:

1 step:(if you do not pre-process data)
-p -o ...\test -g ...\referenced\cell\log_points.csv
if you already have 
2 step: (if you already have pre-processed data)
-a -g -s ...\subwayInfo.csv -o C:\temp\test  ...\preproc\MegaPhone\pre_log_points-2G.csv

All at once
-p -a -o ...\test -g ...\referenced\cell\log_points.csv
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
-s    --spread           number of rows(seconds) received from the user's phone
-o    --output           DIR to write out output dataFrames

Example:
estimator.py -st ...\segmentsStepsDf.csv -t ...\testSets.csv -m ...\Cells_smoothed_ref-200-sm2.csv  -i 30 -s 15 -o ...\predicted
```

