# required libs
import sqlite3

# dev's libs
import variables
import utilities

# additional libs
import pandas as pd
from argparse import ArgumentParser

def main():

    arg_parser = ArgumentParser(description="Database migrator")
    arg_parser.add_argument('-t', '--tables',type=str,nargs = '+',help = 'List of tables to migrate')

    args = arg_parser.parse_args()
    db_conn_pars = variables.DB_CONN[[hostname for hostname in variables.DB_CONN.keys() if variables.DB_CONN[hostname]['main']][0]]
    postgre_to_sqlite(db_conn_pars,args.tables)
    return arg_parser

def postgre_to_sqlite(db_conn_pars,tablenames):

    """
    Simple table migrator from postgresql to sqlite
    :return:
    """

    conn = sqlite3.connect(variables.SQLITEDBPATH)
    for table in tablenames:
        fr = utilities.get_pd_df_from_sql(db_conn_pars,table)
        #csvpath = os.path.join(variables.TEMP_FLD,table)
        #fr.to_csv(csvpath)
        fr.to_sql(table,conn,if_exists='replace')
    return

if __name__ == '__main__':
    main()