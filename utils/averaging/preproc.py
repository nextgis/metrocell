import pandas as pd
import numpy as np
import paths
class Preproc():
    def __init__(self):
        return
    @staticmethod
    def proc_cell_df(df,users = []):
        """
        :param dfPath: 'path to georeferenced -csv file'
        :param users: list of users with 'bad phones'
        :return: the dataframe from which was removed rows with:
            1)"NA" LAC or CID
            2) Users who have 'bad phones'
            ...and was added columns:
            1) 'LAC-CID'
            2) 'segment'
        """
        #create new column 'laccid' to identify cell-station referencing
        df['laccid'] =    df['NetworkType'].astype(str) + "-" +\
                          df['NetworkGen'].astype(str)  + "-" +\
                          df['LAC'].astype(str)         + "-" +\
                          df['CID'].astype(str)
        #create new column 'segment' to identify subway-station referencing
        df['segment_start_id'] = df['segment_start_id'].str.replace("$","")
        df['segment_end_id'] = df['segment_end_id'].str.replace("$","")
        df['segment'] =  df['segment_start_id'] + '-' + df['segment_end_id']
        #exclude phones with bad parameters
        if len(users)!=0:
            for user in users:
                df = df[df['User']!= user]
        #exclude cells that have no lac,cid (fields contain "-1") and Power > 0(this means NoSignal)
        procdf = df[(df['LAC']!=-1)
                    &(df['CID']!=-1)
                    &(df['Power']<0)
                    &(df['Power']>-113)
                    &(df['NetworkType']!='unknown')
                    &(df['NetworkGen']!='unknown')
                    #&(df['MCC']!=-1)
                    #&(df['MNC']!=-1)
                    #&(df['PSC']!=-1)
                      ]

        return(procdf)
    @staticmethod
    def computeAverTime(df,push = False):
        """
        Find a mean path time for each segment.
        :param df: input georeferenced dataframe
        :param push: if True push result to CSV
        :return: dataframe with columns : segment | pathTime | stdev
        """
        pathTime = pd.DataFrame()
        # find minimum and maximum times for each segment by each race
        r_b = df.groupby(['segment','race_id'])['TimeStamp'].max()
        l_b = df.groupby(['segment','race_id'])['TimeStamp'].min()
        # compute path times for each race
        delta_ts = (r_b-l_b)/1000
        # compute the means of path times for each segment.
        # Note! "level == 0" means that mean function applied to segments,
        # whereas level = 1 - applied to race_id
        pathTime['pathTime'] = delta_ts.groupby(level = 0).mean()
        # compute standart deviation
        pathTime['stdev'] = delta_ts.groupby(level = 0).apply(np.std)

        pathTime['stdev'] = pathTime['stdev'].apply(lambda x: float("%.2f"%x))
        pathTime['pathTime'] = pathTime['pathTime'].apply(lambda x: float("%.1f"%x))

        if push == True:
            pathTime.to_csv(paths.subwayInfo)
        return pathTime
if __name__ == "__main__":
    Df = pd.io.parsers.read_csv(paths.preLogPointsPath)
    delta_ts = Preproc.computeAverTime(Df)
    print delta_ts
