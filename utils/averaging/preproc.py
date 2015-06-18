import pandas as pd
class Preproc():
    def __init__(self):
        return
    def proc_cell_df(self,df,users = []):
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
