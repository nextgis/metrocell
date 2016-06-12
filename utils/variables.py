
# base DIRs

# localhost
#METROCELL_BASE = '/Users/sasfeat/Documents/metrocell'

# nextgis
METROCELL_BASE = '/home/alexander_semenov/metrocell'
# PostgreSQL database parameters
TABLES ={'averaged_cell_meta':'averaged_cell_meta',
         'device_info':'device_info',
         'georeferenced':"georeferenced",
         'graph':'graph',
         'stations':'stations',
         'input_data':'input_data',
         'interchanges':'interchanges',
         'lines':'lines',
         'meta_cell':'meta_cell',
         'parsed_cell':'parsed_cell',
         'parsed_sensor':'parsed_sensor',
         'grab_errors':'grab_errors',
         'processing_status':'processing_status',
         'subway_info':'subway_info',
         'averaged_geo':'averaged_geo',
         'segment_quality':'segment_quality',
         'subway_cell_quality':'subway_cell_quality',
         'subway_cell_grabbing_quality':'subway_cell_grabbing_quality',
         'deriviative_types':'deriviative_types'

              }
# the only one server could contain 'main' : True
DB_CONN = {'localhost':{
              'host':'localhost',
              'postgres_port':5432,
              'user':'postgres',
              'password':None,
              'dbname':'metrocell',
              'tables':TABLES,
              #'raw_logs_path':'../data/raw',
              'main':False
           },
           'nextgis':{
               'host':'192.168.250.1',
               'postgres_port':5432,
               'user':'alexander_semenov',
               'password':'SWVP9oMSgZUv',
               'dbname':'metrocell',
               'tables':TABLES,
               #'raw_logs_path':'../data/raw',
               'main':True
           }
}

#available cities

CITIES = ['spb','msk']


# path to raw zip files. Note that subdirectories should be named the same as a available cities!
# Walker will search at the subdir depending on the input city. e.g.  '/metrocell/raw/spb'
INBOX = METROCELL_BASE + '/data/raw'
TIDY_FLD = METROCELL_BASE + '/data/tidy'
TEMP_FLD = METROCELL_BASE + '/data/temp'

LOGSPATH = METROCELL_BASE + "/data/jls/"

sensorMarksName = "sensor_time_marks.csv"
cellMarksName = "cell_time_marks.csv"
externalMarksName = "external_time_marks.csv"
sensorLogName = "sensor_time_log.csv"
cellLogName = "cell_time_log.csv"
externalLogName = "external_time_log.csv"
device_info = "device_info.txt"

available_devices = ['cell','sensor']
moveTypes = ['move','stop','inter']


excluded_meta_cols = ['DateTime']


# averaged alg parameters

r_plot_pars = {
    'scripts_fld':METROCELL_BASE + '/code/utils/stats',
    'out_fld': METROCELL_BASE + '/data/plots'
}
averaged_cell_pars = {'nNeighbors':50,  # the number of neighbours (for kNeighbours method of averaging)
                      'testsize' : 0.4, # percent of testing data for kNeighbours classifieer
                      'minData':15,     # the minimum number of rows for collected LACCID as input parameter (for kNeighbours method of averaging)
                      'minTime':30,      # the minimum passed time needed to move from one station to the another.
                                        # This value is set to except blunders effect appeared at the data collection.
                                        # By default it is 30 seconds.
                      'lcDelta':15,
                      'netOperators':
                         {'25099':'Beeline',
                          '25001':'MTS',
                          '25002':'MegaFon'
                          },
                     'gens' : ['4G','3G','2G'],


                      'predictionMethod' : 'byCorr',
                      'reducingType' : 'localMaxima',


                      #Parameters to check if data corresponds to correlation computation algorithm
                      'frameLength' : 5,    # minimum length of signal
                      'maxLag' : 7.5,

                      'max_shift_delta':0.2,

                      'interp_step_meters':20,
                      'median_window':7,

                      'minNoise': 0.3,

                      'default_quality':0,

                      'constantstd':4,
                      'poly1':0.1,
                      'poly2':0.1,
                      'posit':0.00001
                      }

SQLITEDBPATH = METROCELL_BASE + '/data/metrocell.db'
OUTCSVPATH = METROCELL_BASE + '/data/mathpositioning.csv'
DERVSBOUNDS = \
    {
        'l':0.1,
        'r':-0.1
    }

