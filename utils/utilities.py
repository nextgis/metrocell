__author__ = 'Alex'
import os,re,shutil,sys
import random,string
from datetime import datetime

import ogr
import variables
from shapely.geometry import shape,LineString
from shapely.wkt import loads
from shapely.wkb import loads as loads_wkb
import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import subprocess

def if_none_to_str(s):
    if s is None:
        return ''
    return str(s)
def uniqueUnsorted(ar,returnNA = False):
    if returnNA == False:
        ar = ar[~np.isnan(ar)]
    uniqueIxs = np.unique(ar,return_index = True)[1]
    uniqueVals = [ar[index] for index in sorted(uniqueIxs)]
    return uniqueVals

def dropMultipleCols(frame,columns):
    for col in columns:
        if col in frame.columns.values:
            frame.drop([col],axis = 1,inplace=True)
    return frame

def checkId(frame):
    if frame['ID'].dtypes == 'object':
        frame.loc[frame['ID'] == 'ServiceLog','ID'] = np.nan
        frame['ID'] = frame['ID'].astype('float64')
    return frame

def floatToInt(frame,columns):
    f = lambda x: '%.0f' % x
    g = lambda x : x.replace('nan','')
    for col in columns:
        frame[col] = frame[col].apply(f)
        frame[col] = frame[col].apply(g)
    return frame

def generateRandomId():
    return random.randint(1,999999999)

def placeStrBeforeType(path,val):
    t = string.split(path,".")
    s2 = t[0] + "_" + val + "." + t[1]
    return s2

def insertColumns(frame,columns,value = np.nan):
    for col in columns:
        frame[col] = value
    return frame

def getLocalTime(frame):
    localTime = datetime.fromtimestamp(int(frame.iloc[0]['TimeStamp']/1000)).strftime("%Y%m%d%H%M")
    return localTime

def dfTempate(marksFrame,columns = []):
    """
    Add columns
    :param marksFrame: frame with columns to copy
    :param columns: columns to add
    :return:
    """
    df = pd.DataFrame(columns = list(marksFrame.columns.values)+columns)
    return df


def replaceFormulaVars(formula):
    """
    replace formula long variables (BLABLA,TRATATA,...) to (A,B...)
    :return: names dictionary and updated formula {BLABLA:A,TRATATA:B ... }, (A-B)/(A+B) instead of ('5'-'4')/('5'+'4')
    """
    names = {}
    alphabet = string.ascii_uppercase
    opers = ["(","+","-","/","*",")"]
    parsed = re.split("([(+-/*)])",formula)
    bandNames = list(set([var for var in parsed if var not in opers+[""]]))
    for i in range(0,len(bandNames)):
        names.update({alphabet[i]:bandNames[i]})
    for bandName in names.keys():
        formula = formula.replace(names[bandName],bandName)
    return names,formula



def getCurrentTime():
    return datetime.now()

def filterByExt(DIR,ext,return_count = False):
    files = []
    f_count = 0
    for file in os.listdir(DIR):
        if file.endswith(ext):
            # exlude e.g. <fname>.tiff.aux.xml
            if len(file.split('.'))==2:
                f_count+=1
                fullPath = DIR + "\\" + file
                files.append(fullPath)
    if not return_count:
        return files
    else:
        return f_count

def mkDirs(*args):
    for fld in args:
        if not os.path.exists(fld):
            os.makedirs(fld)
def rmDirs(*args):
    for fld in args:
        if os.path.exists(fld):
            shutil.rmtree(fld,ignore_errors=True)

def set_in_set(s_main,s,reverse=False):
    if ((s[0]<=s_main[0])&(s[1]<=s_main[0]))|((s[0]>=s_main[1])&(s[1]>=s_main[1])):
        if not reverse:
            return 0
    #    else:
      #      return 0,100
    if (s[0]<=s_main[0])&(s[1]<=s_main[1]):
        if not reverse:
            return (s[1]-s_main[0])
    #    else:
     #       return (s[1]-s_main[0])/(s[1]-s[0])*100 , (s[1]-s_main[0])/(s_main[1]-s_main[0])*100
    if (s[0]>=s_main[0])&(s[1]<=s_main[1]):
        if not reverse:
            return s[1]-s[0]
     #   else:
     #       return 100,(s[1]-s[0])/(s_main[1]-s_main[0])*100
    if (s[0]>=s_main[0])&(s[1]>=s_main[1]):
        if not reverse:
            return (s_main[1]-s[0])
     #   else:
     #       return (s_main[1]-s[0])/(s[1]-s[0])*100,(s_main[1]-s[0])/(s_main[1]-s_main[0])*100
    if (s[0]<=s_main[0])&(s[1]>=s_main[1]):
        if not reverse:
            #return (s_main[1]-s_main[0])/(s[1]-s[0])*100
            return s_main[1]-s_main[0]
     #   else:
    #        return 100,0
def get_bound_nums_in_DIR(rawDIR,ext = '.TIF'):
    files = filterByExt(rawDIR,ext=ext)
    bandNums = [get_bNum_l8(file) for file in files]
    return bandNums
def get_bNum_l8(file_path):
    return file_path.split('.')[0].split('_B')[1]
def compare_geoms(wkbs,wkb):
    res = None
    geom = ogr.CreateGeometryFromWkb(wkb)
    for basewkb in wkbs:
        basegeom = ogr.CreateGeometryFromWkb(basewkb)
        if geom.Equals(basegeom):
            res = basewkb
            break
    return res

def execute_sql2(server,sql):
    connString = "host = %s user = %s password = %s dbname = %s port = %s" % (server['host'],server['user'],server['password'],server['dbname'],server['postgres_port'])

    conn = psycopg2.connect(connString)
    conn.rollback()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except:
        print('oops!> resulting image insertion error!',sys.exc_info()[0],sys.exc_info()[1])

def execute_sql(server,sql,data):
    connString = "host = %s user = %s password = %s dbname = %s port = %s" % (server['host'],server['user'],server['password'],server['dbname'],server['postgres_port'])

    conn = psycopg2.connect(connString)
    conn.rollback()
    cur = conn.cursor()
    try:
        cur.execute(sql,data)
    except:
        print('oops!>',sys.exc_info()[0],sys.exc_info()[1])
    conn.commit()

def insert_pd_to_postgres(fr,db_conn,table,if_exists = 'append',index=False,return_last_id = False):
    engine = create_engine('postgresql://%(user)s:%(password)s@%(host)s:%(postgres_port)s/%(dbname)s' % db_conn)
    try:
        fr.to_sql(table,engine,if_exists = if_exists,index=index)
        if return_last_id:
            ins_fr = get_pd_df_from_sql(db_conn,table)
            ins_ix = ins_fr.last_valid_index()
            return ins_ix
    except:
        print "Pd dataframe inserting error", sys.exc_info()[0],sys.exc_info()[1]
def set_postgis_epsg(server,table):
    connString = "host = %s user = %s password = %s dbname = %s port = %s" % (server['host'],server['user'],server['password'],server['dbname'],server['postgres_port'])
    conn = psycopg2.connect(connString)
    sql = "UPDATE " + table + " SET geom=ST_SetSRID(geom,3857);"
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except:
        print "oops!> Postgis epsg setting error!", sys.exc_info()[0],sys.exc_info()[1]
#def get_pd_df_from_sql(utils,tab_name,index_col = 'id'):
#    connString = "host = %s user = %s password = %s dbname = %s port = %s" % (utils['host'],utils['user'],utils['password'],utils['dbname'],utils['postgres_port'])
#    conn = psycopg2.connect(connString)
#    sql = "SELECT * FROM %s" % tab_name
#    fr = pd.read_sql_query(sql,conn,index_col)
#    return fr
def update_postgre_rows(server,table,row_id,field,rows_val,index_col = 'id'):
    connString = "host = %s user = %s password = %s dbname = %s port = %s" % (server['host'],server['user'],server['password'],server['dbname'],server['postgres_port'])
    conn = psycopg2.connect(connString)
    if type(row_id) == tuple:
        sql = "UPDASTE %s SET %s = %s WHERE %s in %s" % (table,field,rows_val,index_col,row_id)
    else:
        sql = "UPDATE %s SET %s=%s WHERE %s = %s"% (table,field,rows_val,index_col,row_id)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
def interpolator(server_conn,table,id_from,id_to,ratio,city):
    connString = "PG:host = %s user = %s password = %s dbname = %s port = %s" %\
                         (server_conn['host'],
                          server_conn['user'],
                          server_conn['password'],
                          server_conn['dbname'],
                          server_conn['postgres_port'])
    conn = ogr.Open(connString)
    lines = conn.GetLayer(server_conn['tables'][table])
    code = str(id_from).zfill(3) + '-' + str(id_to).zfill(3)
    lines.SetAttributeFilter("code = '%s' AND city = '%s'"%(code,city))
    line_feature = lines.GetNextFeature()
    segment_dist = line_feature.geometry().Length()
    point_linear_offset = segment_dist * ratio
    point = shape(loads(line_feature.geometry().ExportToWkt())).interpolate(point_linear_offset)
    conn.Destroy()
    return point.wkb_hex
def fromCurrentTime(mark,ext):
    t = datetime.now()
    timename = str(t.year) + "-" + str(t.month) + "-" + str(t.day) + "-" + str(t.hour) + "-" + str(t.minute) + "-" + str(t.second)
    timename = timename + "-" + mark + ext
    return timename
def computeInterpStep(df):
    steps = []
    grouped = df.groupby(['race_id','User','laccid'])
    for ix,group in grouped:
        step = np.mean(pd.rolling_apply(group['ratio'],2,np.diff))
        steps.append(step)
    interpStep = np.mean(steps)
    return interpStep

def get_pd_df_from_sql(db_conn,tab_name,index_col = 'id',**wheres):
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)

    sql = "SELECT * FROM %s" % tab_name
    if wheres:
        sql +=" WHERE "
        for key in wheres:
            sql +=" %s= '%s' AND"%(key,wheres[key])
    if sql[-3:] == 'AND':
        sql = sql[:-3]
    fr = pd.read_sql_query(sql,conn,index_col)
    return fr
def get_pd_df_from_sql2(db_conn,tab_name,index_col = 'id',**wheres):
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)

    sql = "SELECT * FROM %s" % tab_name
    if wheres:
        sql +=" WHERE "
        for key in wheres:
            sql += " %s in %s AND"%(key,wheres[key])
    if sql[-3:] == 'AND':
        sql = sql[:-3]
    fr = pd.read_sql_query(sql,conn,index_col)
    return fr
def merge_parsed_georeferenced_df(db_conn,table):
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    sql = """SELECT g.id_from,g.id_to,g.ratio,g.city,g.geom,g."zip_id",
             p.id,p."TimeStamp",p.move_type,p."LAC",p."CID",p."User",p."NetworkGen",p."NetworkType",p."Active",p."MCC",p."MNC",p."race_id",p."Power"
             FROM georeferenced as g, parsed_cell as p WHERE
             p.move_type = 'move' AND
             p.ratio = g.ratio AND
             p.id_from = g.id_from AND
             p.id_to = g.id_to AND
             p.city = g.city;"""
    conn = psycopg2.connect(connString)
    fr = pd.read_sql_query(sql,conn,index_col = 'id')

    #df = utilities.execute_sql(self.server_conn,sql=sql,data = {})
    return fr
def update_postgre_rows_cols(db_conn,postgre_fr_name,group_keys,pd_df,index_col = 'id'):
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)
    cur = conn.cursor()
    postgre_fr = get_pd_df_from_sql(db_conn,postgre_fr_name)
    for i,row in pd_df.iterrows():
        if len(group_keys) == 1:
            ex_row = postgre_fr[(postgre_fr[group_keys[0]] == row[group_keys[0]])]
        elif len(group_keys) == 2:
            ex_row = postgre_fr[(postgre_fr[group_keys[0]] == row[group_keys[0]])&(postgre_fr[group_keys[1]] == row[group_keys[1]])]
        if not ex_row.empty:
            for col in pd_df.columns.values:
                sql = "UPDATE %s SET %s = '%s' WHERE %s = %s"%(postgre_fr_name,col,row[col],index_col,i)
                cur.execute(sql)
                conn.commit()
        else:
            insert_pd_to_postgres(pd.DataFrame(row).transpose(),db_conn,postgre_fr_name)
def get_pd_df_from_sql3(db_conn,tabname,whereas_keys,whereas_vals):
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)
    fr = pd.DataFrame()
    for where in whereas_vals:
        sql = "SELECT * FROM %s WHERE %s = %s AND %s = %s"%(tabname,whereas_keys[0], where[0],whereas_keys[1],where[1])
        fr_slice = pd.read_sql_query(sql,conn)
        fr = pd.concat([fr,fr_slice])
    return fr
def remove_slice_from_postgres(db_conn,tabname,whereas_key,whereas_vals):
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)
    conn.rollback()
    cur = conn.cursor()
    for where in whereas_vals:
        sql = "DELETE FROM %s WHERE %s = '%s'"%(tabname,whereas_key, where)
        try:
            cur.execute(sql)
            conn.commit()
        except:
            pass
    conn.close()
    return
def interpolate_averaged_points(db_conn,lines_table,averaged_pts_table,step=40):
    Seg_fr = pd.DataFrame()
    connString = "host = %s user = %s password = %s dbname = %s port = %s" % (db_conn['host'],
                                                                              db_conn['user'],
                                                                              db_conn['password'],
                                                                              db_conn['dbname'],
                                                                              db_conn['postgres_port'])
    for city in variables.CITIES:
        lines_fr = get_pd_df_from_sql(db_conn,lines_table,index_col='ogc_fid',city=city)
        segments = list(lines_fr['code'])
        conn = ogr.Open("PG:"+connString)
        lines = conn.GetLayer(db_conn['tables'][lines_table])
        for segment in segments:
            lines.SetAttributeFilter("code = '%s' AND city = '%s'"%(segment,city))
            line_feature = lines.GetNextFeature()
            line_geom = line_feature.geometry()
            segment_dist = line_geom.Length()
            seg_ratios = list(np.linspace(0,1,segment_dist/step+1))
            seg_fr = pd.DataFrame({'ratio':seg_ratios,'segment_id':segment,'city':city},columns=['ratio','segment_id','geom','city'])
            seg_fr['geom'] = seg_fr['ratio'].apply(lambda x : shape(loads(line_geom.ExportToWkt())).interpolate(x,normalized=True).wkb_hex)
            Seg_fr = pd.concat([Seg_fr,seg_fr])
            #insert_pd_to_postgres(seg_fr,db_conn,averaged_pts_table)
    conn.Destroy()
    conn = psycopg2.connect(connString)
    cur = conn.cursor()
    for i,row in Seg_fr.iterrows():
        sql = """ INSERT INTO """+ averaged_pts_table + """(geom,ratio,segment_id,city)  VALUES(ST_SetSRID(%(geom)s::geometry,3857),%(ratio)s,%(segment_id)s,%(city)s)  """
        cur.execute(sql,row.to_dict())
        conn.commit()
    conn.close()
    #sql_set_srid = """ALTER TABLE %s
    #                ALTER COLUMN geom
    #                TYPE geometry(Point,3857)
    #                USING ST_SetSRID(geom,3857)"""%averaged_pts_table
    #cur.execute(sql_set_srid)

    return
def plot_signal_power(db_conn,plot_func,id_from,id_to,city):
    #try:

    if db_conn['password'] is None:
        password = ''
    else:
        password = db_conn['password']
    ps = subprocess.Popen([
            "Rscript",variables.r_plot_pars['scripts_fld']+'/plot_georeferenced.R',plot_func,
             db_conn['host'],str(db_conn['postgres_port']),db_conn['user'],password,db_conn['dbname'],str(id_from),str(id_to),city
            ],
        stdout=subprocess.PIPE
    )
    output = ps.communicate()[0]
    print output

    #except:
        #print "Could not plot graph: ",id_from,id_to,city
    return
def wkbpts_to_wkbline(wkb_pts,wkt=False):
    """
    :param wkt_pts: pts wkb {list}
    :return:
    """
    ls_pts = []
    for wkb in wkb_pts:
        pt = loads_wkb(wkb,hex = True)
        ls_pts.append(pt)
    line = LineString(ls_pts)
    if not wkt:
        return line.wkb_hex
    else:
        return line.wkt