# -*- coding: utf-8 -*-
# required libs
import sqlite3
import numpy as np
import pandas as pd

from argparse import ArgumentParser

# dev libs
import variables,utilities

def main():
    parser = ArgumentParser('Subway lines positioning prototype')
    parser.add_argument('-c','--city', choices=variables.CITIES, help = 'Current city')
    parser.add_argument('-t','--timeslice',type = int,help = 'Time which is needed to grab one slice of data(msc)')
    args = parser.parse_args()
    global CITY
    CITY = args.city
    global TIMESLICE
    TIMESLICE = args.timeslice
    segment_position = {
        'id':False, #перегон
        'part':False,   # часть перегона(начало|середина|конец)
        'point':False}  # точка на перегоне
    global ERRORS
    ERRORS = {'segmentend':"Нет данных по перегону в городе с id = %s, собранных пользователем в некоторый заезд!"%CITY,
              'noactive':"Нет активных вышек!"
              }
    # параметры для наборов данных:
    # коэффициенты кривой
    # характер кривой в начале, середине и конце(возрастает, убывает или примерно 0)
    # значения производной в начале, середине и конце [-1;1]
    samples = \
        {
            1:{
                'pars':
                    {
                        'coeffs':None,
                        'dertrends':None,
                        'dervalues':None
                    },
                'rawdata':pd.DataFrame(),
                'predicteddata':pd.DataFrame()
        },
            2:{
                'pars':
                    {
                        'coeffs':None,
                        'dertrends':None,
                        'dervalues':None
                    },
                'rawdata':pd.DataFrame(),
                'predicteddata':pd.DataFrame()
            },
            3:{
                'pars':
                    {
                        'coeffs':None,
                        'dertrends':None,
                        'dervalues':None
                    },
                'rawdata':pd.DataFrame(),
                'predicteddata':pd.DataFrame()
            }
        }

    startpoint,segment_fr = rawdata_generator()
    samples[1]['rawdata'] = rawslice_generator(startpoint,segment_fr)

    if not samples[1]['rawdata'].empty:
        print samples[1]['rawdata']
        samples[1]['predicteddata'] = possible_location(samples[1]['rawdata'])
        #sample1['pars'] = compute_signal_parameters(rawslice)
        if not samples[1]['predicteddata'].empty:
            # если в таблице только один id перегона, то мы его определили
            if len(list(set(samples[1]['predicteddata']['segment_id'])))==1:
                segment_position['segment'] = True

            # пока не будет получена окончательная координата объекта,
            # сбор данных не прекращать
            while (segment_position['id'] == False)&\
                  (segment_position['part'] == False)&\
                  (segment_position['point'] == False):

                startpoint = samples[1]['rawdata'].iloc[-1]
                samples[2]['rawdata'] = rawslice_generator(startpoint,segment_fr)
                if not samples[2]['rawdata'].empty:
                    print samples[2]['rawdata']
                    samples[2]['predicteddata'] = possible_location(samples[2]['rawdata'])
                    if not samples[2]['predicteddata'].empty:
                        samples[3]['rawdata'] = pd.concat([samples[1]['rawdata'],samples[2]['rawdata']])

                    else:
                        print ERRORS['segmentend']
        else:
            print ERRORS['segmentend']
    else:
        print ERRORS['segmentend']

def rawdata_generator(segment = None):
    """
    Достает сырые данные с перегона из БД и точку, с которой началось позиционирование
    :param segment:
    :return:
    """
    # забираем параметры для соединения с БД
    db_conn_pars = variables.DB_CONN[[hostname for hostname in variables.DB_CONN.keys() if variables.DB_CONN[hostname]['main']][0]]
    # используем dev функцию для записи таблицы в pandas.DataFrame
    rawdata_table = utilities.get_pd_df_from_sql(db_conn=db_conn_pars,tab_name=variables.TABLES['parsed_cell'],city = CITY)
    # сортируем
    rawdata_table = rawdata_table.sort_values(by = ['city','User','TimeStamp'])
    # генерируем случайную точку - как будто это наше текущее положение
    startpoint = rawdata_table.sample(1)
    # записываем все необходимые данные это точки - где и кто
    city = startpoint.iloc[0]['city']
    user = startpoint.iloc[0]['User']
    id_from = startpoint.iloc[0]['id_from']
    id_to = startpoint.iloc[0]['id_to']
    race_id = startpoint.iloc[0]['race_id']
    # в соотвествтии с параметрами полученными на предыдущем шаге, достаем данные по сегменту, на котором мы находимся
    segment_fr = rawdata_table[(rawdata_table['city'] == city)&
                  (rawdata_table['User'] == user)&
                  (rawdata_table['id_from'] == id_from)&
                  (rawdata_table['id_to'] == id_to)&
                  (rawdata_table['move_type'] == 'move')&
                  (rawdata_table['race_id'] == race_id)
    ]

    return startpoint,segment_fr

def rawslice_generator(startpoint,segment_fr):
    """
    Извлекает кусок данных размера TIMESLICE, начиная с временной отсечки в startpoint
    :param startpoint: исходная точка pd.DataFrame
    :param segment_fr: сегмент данных(перегон) pd.DataFrame
    :return:
    """
    rawslice = pd.DataFrame()
    # забираем начальную временную отсечку
    startstamp = startpoint.iloc[0]['TimeStamp']
    # вычисляем конечную временную отсечку
    finishstamp =  startstamp + TIMESLICE
    # забираем начальный индекс в таблице
    start_ix = startpoint.index[0]
    # отсекаем лишнее от перегона(нужно только то, что после startstamp)
    segment_fr = segment_fr[segment_fr.index>=start_ix]
    finish_ix = None
    # извлекаем кусок данных
    for i,row in segment_fr.iterrows():
        if row['TimeStamp']>finishstamp:
            finish_ix = i
            break
    if finish_ix:
        rawslice = segment_fr.ix[start_ix:finish_ix]
    return rawslice
def getunique_LACCID(rawslice):
    """
    Возвращает все возможные уникальные сочетания LAC и CID из rawslice
    """
    LACCIDS = []
    laccids = list(set(rawslice['LAC'] + ':' + rawslice['CID']))
    for lc in laccids:
        LAC,CID = lc.split(':')
        if (LAC!='-1') and (CID!='-1'):
            LACCIDS.append((LAC,CID))
    return LACCIDS
def simpleselectbylaccids(laccids):
    basesql = "SELECT * FROM fingerprint WHERE city = '%s' "%CITY
    sql_lc = ""
    for LAC,CID in laccids:
        sql_lc += """ ("LAC" = %s AND "CID" = %s) OR"""%(LAC,CID)
    if sql_lc:
        sql = basesql + 'AND' + sql_lc[:-3]
        return sql
    else:
        return None
def possible_location(rawslice):
    """
    Делает выборку возможного местоположения на основании данных в rawslice
    :param rawslice: первые TIMESLICE секунд данных
    :return: xy
    """
    res1 = pd.DataFrame()
    # забираем все пойманные laccidы за первые TIMESLICE секунд
    laccids = getunique_LACCID(rawslice)
    # формируем запрос
    sql1 = simpleselectbylaccids(laccids)
    # если вышки были пойманы
    if sql1:
        # выполняем запрос и возвращаем pandas.DataFrame
        sqlitedbpath = variables.SQLITEDBPATH
        conn = sqlite3.connect(sqlitedbpath)
        res1 = pd.read_sql(sql1,conn,index_col='id')
    else:
        print ERRORS['noactive']
    return res1
def datacompare(s1,s2,s3):
    """
    Сравнивает данные из трех наборов и проверяет :

    :param s1: набор данных за первые n секунд
    :param s2: набор данных за вторые n секунд
    :param s3: набор данных за 2n секунд
    :return:
    """

if __name__ == '__main__':
    main()