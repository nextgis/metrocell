from parser import prepareAndSplit
from log_georef import geo_ref
from averaging import averaging
import pandas as pd
import numpy as np
import os,sys
from argparse import ArgumentParser
import variables
import utilities

def main():

    arg_parser = ArgumentParser(description="Desktop service of Metrocell database updating")
    arg_parser.add_argument('-c', '--city', type=str,required = True,choices = variables.CITIES, help = 'City where logs has been collected')
    #arg_parser.add_argument('-s', '--server',type=str,required = True,choices = variables.DB_CONN.keys(),help = 'utils')
    arg_parser.add_argument('-s', '--session',action='store_true',help = "Does file is session(folder which contains single time grabbing by multiple devices.zip files) instead of just filename.zip file")
    arg_parser.add_argument('-f', '--inbox_filename',type=str,help = 'Path to the 1-user .zip of session folder which contains "n" - users .zip')
    arg_parser.add_argument('-a', '--aver',action='store_true',help = 'Does it need to execute averaging block')
    #args = arg_parser.parse_args()
    return arg_parser
def log_processer(city,inbox_filename,session,aver):
    #print "Database updating starts"
    print 30*"-"
    global SERVER
    global CITY
    global SESSION
    #SERVER = variables.DB_CONN[args.server]
    SERVER = variables.DB_CONN[[hostname for hostname in variables.DB_CONN.keys() if variables.DB_CONN[hostname]['main']][0]]
    SESSION = session
    CITY = city
    # table that will be averaged
    AVERAGEDTABLE = 'cell'

    # define interchanges and graph
    interchanges_df = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['interchanges'])
    interchanges_df = interchanges_df[interchanges_df['city'] == CITY]
    graph_df = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['graph'])
    graph_df = graph_df[graph_df['city'] == CITY]
    # insert the file at the DataBase with corresponded city if it has not been already inserted
    input_ids,input_rows = inbox_file_inserter(inbox_filename)

    # if the data has been accepted
    processing_status = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['processing_status'],index_col='zip_id')
    raw_status_df = processing_status[processing_status.index.isin(input_ids)]
    if not (True in np.unique(raw_status_df['parsed'])):
        # 2. Parse and split raw logs.
        # - iteratively read all existed logs(.zip)
        # - extract info file and write phone info into the database
        # - define segments where user has collected the logs(station_from - station_to)
        # - define the errors
        # - write the results into the DataBase
        splitter = prepareAndSplit.PrepareAndSplit(SERVER,CITY,interchanges_df,graph_df,input_rows,plot=True)
        splitter.loopRaws()


    if not (True in np.unique(raw_status_df['georeferenced'])):
        # 3. Georeference parsed logs
        _geo_ref = geo_ref.Geo_ref(SERVER,input_ids,CITY)
        _geo_ref.geo_ref()

    if (aver)&(not (True in np.unique(raw_status_df['averaged']))):
        # 4. Average georeferenced logs
        _averager = averaging.Averaging(SERVER,AVERAGEDTABLE,input_ids,CITY)
        if not _averager.move_df.empty:
            _averager.preprocData()
            _averager.iterateBySegment()
        else:
            old_stdout = sys.stdout
            log_file = open(variables.LOGSPATH + 'zip_averaging_errors.log',"a")
            sys.stdout = log_file
            print "zip_ids = ",input_ids
            sys.stdout = old_stdout
            log_file.close()



def inbox_file_inserter(inbox_filename):
    print('Inbox file inserting starts')
    if not SESSION:
        inbox_filepath = os.path.join(variables.INBOX,CITY,inbox_filename)
    else:
        inbox_filepath = os.path.join(variables.INBOX,CITY,'sessions',inbox_filename)
        #inbox_filename = inbox_filename.split('/')[-1]
    existed_Df = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['input_data'],index_col='zip_id')
    city_Df = existed_Df[existed_Df['city'] == CITY]
    existed_b = list(city_Df['zip_basename'])

    if os.path.isdir(inbox_filepath):
        while True:
            session_id = utilities.generateRandomId()
            if session_id not in list(existed_Df['session_id']):
                break
        fld_files = [ os.path.join(variables.INBOX , CITY, 'sessions', inbox_filename , file) for file in os.listdir(inbox_filepath) if os.path.splitext(file)[1] == '.zip']
        base_inboxs = ['/'.join(fld_file.split('/')[-2:]) for fld_file in fld_files]
        for zipfile in fld_files:

            base_inbox = '/'.join(zipfile.split('/')[-2:])
            #base_inbox = os.path.basename(zipfile)

            if base_inbox not in existed_b:

                # out_path = variables.TIDY_FLD + '/' + CITY + '/' + os.path.basename(zipfile)
                # shutil.copyfile(zipfile,out_path)
                fr = pd.DataFrame({'session_id':[session_id],'zip_basename':[base_inbox],'city':[CITY]})
                utilities.insert_pd_to_postgres(fr,SERVER,SERVER['tables']['input_data'])
                updated_fr = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['input_data'],index_col = 'zip_id')
                updated_fr = updated_fr.sort_values(by= ['zip_id'])
                last_inserted_zip_id = updated_fr.last_valid_index()
                status_row = pd.DataFrame({'zip_id':[last_inserted_zip_id]})
                utilities.insert_pd_to_postgres(status_row,SERVER,SERVER['tables']['processing_status'])
                #os.remove(zipfile)
            else:
                print "Zip ", base_inbox ," is already exists in database!"

                #raise Exception('The file is already exists at the database!Check the filename and change the time if nessesary.')
    elif os.path.isfile(inbox_filepath):


        base_inboxs = [inbox_filename]
        if inbox_filename not in existed_b:
            #out_path = variables.TIDY_FLD + '/' + CITY + '/' + os.path.basename(inbox_filepath)
            #shutil.copyfile(inbox_filepath,out_path)
            fr = pd.DataFrame({'session_id':[None],'zip_basename':[inbox_filename],'city':[CITY]})
            utilities.insert_pd_to_postgres(fr,SERVER,SERVER['tables']['input_data'])
            updated_fr = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['input_data'],index_col = 'zip_id')
            updated_fr = updated_fr.sort_values(by= ['zip_id'])
            last_inserted_zip_id = updated_fr.last_valid_index()
            status_row = pd.DataFrame({'zip_id':[last_inserted_zip_id]})
            utilities.insert_pd_to_postgres(status_row,SERVER,SERVER['tables']['processing_status'])
            #os.remove(inbox_filepath)
        #else:
            #print("Zip is already exists in database!")

            #raise Exception('The file is already exists at the database!Check the filename and change the time if nessesary.')

    else:
        raise Exception("The file does not exist or has inappropriate format : ",inbox_filepath)

    fr = utilities.get_pd_df_from_sql(SERVER,SERVER['tables']['input_data'],index_col = 'zip_id')
    input_rows = fr[fr['zip_basename'].isin(base_inboxs)]
    input_ids = list(input_rows.index)

    return input_ids,input_rows


if __name__ == '__main__':
    parser = main()
    args = parser.parse_args()
    log_processer(args.city,args.inbox_filename,args.session,args.aver)