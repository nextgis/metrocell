parser
=======
Utility for parsing and errors checking of raw logs.
```bash
usage: prepareAndSplit.py [-h] -i INTERCHANGES -m MOVEGRAPHPATH [-b] [-u]
        inputRawDir outputProcDir

positional arguments:
    inputRawDir         DIR containing users zip-files or session-folders
    outputProcDir       DIR to save adapted zip-files 
        
optional arguments:
-h      --help
-i      --interchanges          Path to 'interchanges' dataFrame       
-m      --moveGraphPath         Path to 'moves graph' dataFrame
-mC     --moveGraphClosedPath   Path to graph with closed stations
-u      --mainUser              The user whose logs contain markFiles(required if '-b' argument is initialized)
-b      --bring                 Bring to a single time or not(need to store if input directory contains session-folders)
  
Example:
/prepareAndSplit.py -i ...\...\data\msk\interchanges.csv -m ...\...\data\msk\graph.csv -mC ...\...\closed\msk\graph.csv ...\...\data\raw\msk ...\...\data\splitterOutput
```

After finish you will have 3 folders ['cell','sensor','external'] with parsed logs 
and 'error' folder containing errors dataFrames. each dataFrame will have the next template
(analyze each group with identical errorIndex separately):
ID          [int]    identifier of the station
Name        [str]    name of the station
inter       [-1]     if this and the previous( or the next) stations have not been founded at the interchanges graph dictionary 
move        [-1]     if station from - station to graph have not been founded at the move graph dictionary      
sequence    [-1]     sequence error. truth sequence has [StationI[3]-StationI[4]-StationII[1]-StationII[2]] sequence
stationId   [-1]     if mark has been entered incorrectly. for example instead of 'Новокузнецкая' 'новокуз' has been written. Then identifier of station will be equal to -1. 
errorIndex  [random] identifier of error. for each section it will be different. 