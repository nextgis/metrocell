import sqlite3

class Dbase():
    def __init__(self,path,key = 'new'):
        self.connection = self.create_log_points(path,key)
    def create_log_points(self,path,key):
        # create sqlite database
        connection = sqlite3.connect(path+'\\'+ 'log_points.sqlite')
        cursor = connection.cursor()
        if key =='new':
            cursor.execute('''CREATE TABLE log_points (
                       race_id INTEGER,
                       x FLOAT,
                       power INTEGER,
                       ration FLOAT,
                       mcc VARCHAR(254),
                       y FLOAT,
                       psc VARCHAR(254),
                       active VARCHAR(254),
                       mnc VARCHAR(254),
                       cid INTEGER,
                       lac INTEGER,
                       seg_begin INTEGER,
                       seg_end INTEGER)''')
        connection.commit()
        return connection
