import os,sys
import utilities
import variables
import pandas as pd
import subprocess
from log_processer import log_processer
def walker(aver):
    """
    Walk through the files at the directory and initialize log_processer
    if there was no this file at the DB
    :return:
    """

    #old_stdout = sys.stdout
    #log_file = open(variables.LOGPATH,"w")
    #sys.stdout = log_file

    SERVER = variables.DB_CONN[[hostname for hostname in variables.DB_CONN.keys() if variables.DB_CONN[hostname]['main']][0]]
    dirnames = os.listdir(variables.INBOX)
    dirs = [os.path.join(variables.INBOX,file) for file in dirnames if os.path.isdir(os.path.join(variables.INBOX,file))]

    ava_files_Df = pd.DataFrame()
    existed_Df = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['input_data'],index_col='zip_id')
    session_dirs = list(existed_Df['zip_basename'])
    session_dirs = [os.path.dirname(f) for f in session_dirs]

    processing_status_Df = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['processing_status'],index_col = 'zip_id')
    indexes = list(processing_status_Df.index)
    print('Walker starts searching')
    for input_dir in dirs:
        city = os.path.basename(input_dir)
        if city in variables.CITIES:
            for dir,subdir,fnames in os.walk(input_dir):
                for fname in fnames:
                    session = False
                    to_process = False
                    fpath = os.path.join(dir,fname)
                    if fname in list(existed_Df['zip_basename']):
                        ex_zip_id = existed_Df[existed_Df['zip_basename'] == fname].index[0]

                        if ex_zip_id in indexes:
                            pr_status_row = processing_status_Df[processing_status_Df.index == ex_zip_id].iloc[0]
                            if aver:
                                cols = ['parsed','georeferenced','averaged']
                            else:
                                cols = ['parsed','georeferenced']
                            statuses = [pr_status_row[col] for col in cols if pr_status_row[col] == False]

                            if len(statuses)>0:
                                to_process = True
                    if (fname not in list(existed_Df['zip_basename']))|\
                            (to_process):
                        fname = os.path.basename(fpath)
                        # session_id = None
                        if 'sessions' in fpath:
                            session = True
                            L = fpath.split('/')
                            session_ix = [i for i,x in enumerate(L) if x=='sessions'][0]
                            l = L[session_ix+1:session_ix+2]
                            fname = '/'.join(l)
                            l2 = L[session_ix+1:session_ix+3]
                            fname_full = '/'.join(l2)
                            if fname_full in list(existed_Df['zip_basename']):
                                ex_zip_id = existed_Df[existed_Df['zip_basename'] == fname_full].index[0]

                                if ex_zip_id in indexes:
                                    pr_status_row = processing_status_Df[processing_status_Df.index == ex_zip_id].iloc[0]
                                    if aver:
                                        cols = ['parsed','georeferenced','averaged']
                                    else:
                                        cols = ['parsed','georeferenced']
                                    statuses = [pr_status_row[col] for col in cols if pr_status_row[col] == False]

                                if len(statuses)>0:
                                    to_process = True


                            if (fname not in session_dirs)|(to_process):
                                row = pd.DataFrame({'fname':[fname],'city':city,'session':[session]})
                                ava_files_Df = pd.concat([ava_files_Df,row])
                        else:
                            row = pd.DataFrame({'fname':[fname],'city':city,'session':[session]})
                            ava_files_Df = pd.concat([ava_files_Df,row])
    connString = "host = %s user = %s password = %s dbname = %s port = %s" %\
                 (SERVER['host'],SERVER['user'],SERVER['password'],SERVER['dbname'],SERVER['postgres_port'])
    print('Walker has finished searching! Log processor initialization has started...')
    new_files_total = len(ava_files_Df)

    #conn = psycopg2.connect(connString)
    #cur = conn.cursor()
    # insert new files to the input_data table

    indexer = 0
    for i,row in ava_files_Df.iterrows():
        indexer+=1
        sys.stdout.write('\n' + str(indexer) + " / " + str(new_files_total) )
        sys.stdout.flush()
        try:
            log_processer(row['city'],row['fname'],row['session'],aver)
        except:
            print sys.exc_info()[0],sys.exc_info()[1]
            continue
    #sys.stdout = old_stdout
    #log_file.close()
    return
if __name__ =='__main__':
    aver = True
    walker(aver)