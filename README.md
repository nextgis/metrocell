Metrocell
=========

Metro navigation experiment using cell data

To achieve the goal we should resolve 2 tasks step by step:

1. positioning in the linear systems

2. positioning in the free moving space


Now we are working on the first task.

Current dev version is based on using suggestion that there is a way to identify current position by 2 main attributes of cell network: identifier of station and it signal power. We have already developed an algorithm of transformation from raw data(to collect it we use https://github.com/nextgis/nextgislogger mobile application.) to spatial database.

It contains several steps:

Theoretically there will be the least errors if we use the idea that the train moving equation contains 3 states:acceleration,constant speed,deceleration

If so, we could figure out the coordinates of each log point.

We could use the combination of moving window and correlation algorithm to shift to bring each signal function to the mean value.
We could apply “mean” and k-means regression methods of filtration to avoid a noise.
We have also developed the algorithm of position prediction. It based on the comparison between current form of signal and early