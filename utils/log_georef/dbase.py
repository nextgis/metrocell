import sqlite3

class Dbase():
    def __init__(self,path):
        self.connection = self.create_log_points(path)
    def create_log_points(self,path):
        # create sqlite database
        connection = sqlite3.connect(path+'\\'+ 'log_points.sqlite')
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE log_points (
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