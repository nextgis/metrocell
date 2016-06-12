# -*- coding: utf-8 -*-
# required libs
import sqlite3,psycopg2
import numpy as np
import pandas as pd
import time
from argparse import ArgumentParser

# dev libs
import variables,utilities


"""
Алгоритм позиционирования в линейной сети метрополитена по станциям сотовой сети.
Он-лайн фаза позиционирования -- второй блок.
Использует базу данных, в которой хранятся шаблоны, описывающие каждый участок метрополитена по
идентфикаторам базовых и соседних станций, а также изменение их характеров сигнала.
Скрипт является прототипом он-лайн режима позиционирования:
    - случайным образом генерирует точку на некотором перегоне
    - последовательно схватывает из него участок данных в t секунд и отправляет его в логическую часть позиционирования
    - логика работы заключается в простой выборке всех возможных id станций,
    сравнении характеров сигналов и выявлении характерных точек на перегоне(потеря сигнала, смены базовой станции или появление новой соседней)
"""


def main():
    parser = ArgumentParser('Subway lines positioning prototype')
    parser.add_argument('-c','--city', choices=variables.CITIES, help = 'Город, в котором находится пользователь')
    parser.add_argument('-t','--timeslice',type = int,help = 'Врем, которое необходимо для сбора одного сегмента данных (ед.изм.: мсек)')
    args = parser.parse_args()
    global CITY
    CITY = args.city
    global TIMESLICE
    TIMESLICE = args.timeslice
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
                'pars':{},
                    #{
                    #    'coeffs':None,
                    #    'dertrend':None,
                    #    'dervalue':None
                    #},
                'rawdata':pd.DataFrame(),
                'predicteddata':pd.DataFrame(),
                'predictedsegments':pd.DataFrame()
        },
            2:{
                'pars':
                    {
                        #'coeffs':None,
                        #'dertrend':None,
                        #'dervalue':None
                    },
                'rawdata':pd.DataFrame(),
                'predicteddata':pd.DataFrame(),
                'predictedsegments':pd.DataFrame()
            },
            3:{
                'pars':
                    {
                        #'coeffs':None,
                        #'dertrend':None,
                        #'dervalue':None
                    },
                'rawdata':pd.DataFrame(),
                'predicteddata':pd.DataFrame(),
                'predictedsegments':pd.DataFrame()
            }
        }


    #testsegment_id = None
    db_conn = variables.DB_CONN[[hostname for hostname in variables.DB_CONN.keys() if variables.DB_CONN[hostname]['main']][0]]
    testsegments = segment_cellnum_more_two_generator(db_conn=db_conn)
    subway_info = utilities.get_pd_df_from_sql(db_conn,variables.TABLES['subway_info'],'id')
    lines_info = utilities.get_pd_df_from_sql(db_conn,variables.TABLES['lines'],'ogc_fid')
    startratio = 0
    numiterations = 50
    global DELTARATIO
    DELTARATIO = 0.05
    #user = 'natalie'
    user = None
    outpath = variables.OUTCSVPATH

    check_frame = pd.DataFrame()

    for i in range(0,numiterations):
        last_slice_point = None
        segment_position = {
                'id':False,     #перегон
                'part':False,   # часть перегона(начало|середина|конец)
                'point':False   # точка на перегоне
            }
        testsegment_id = testsegments.sample(1)['segment_id'].iloc[0]
        segment_id, point = None, None
        #check = pd.DataFrame({'point':[None],'point_dist':[None],'segment_id':[None],'time':[None]})
        check = pd.DataFrame({'segment_id':[None],'time':[None],'dist_error':[None]})
        usermoves = True
        start_ratio,segment_fr,true_segment = rawdata_generator(segment_id=testsegment_id,ratio=startratio,user = user)
        #segment_fr = segment_fr.sort_values(by = ['TimeStamp'])
        segment_fr = segment_fr.sort_values(by = ['ratio'])
        segment_fr = segment_fr.set_index([range(0,len(segment_fr))])
        start_ix = segment_fr[(segment_fr['ratio'] == start_ratio)].index[0]
        # последняя точка на перегоне
        last_segment_point = segment_fr.last_valid_index()
        # пока не будет получена окончательная координата объекта,
        # сбор данных не прекращать
        iter = 0
        while(segment_position['part'] == False)and(usermoves == True):
            iter+=1
            samples[1]['rawdata'] = rawslice_generator(start_ix,segment_fr)
            # забираем слайс данных в TIMESTAMP секунд


            if samples[1]['rawdata'].empty:
                usermoves = False
            else:
            #if not samples[1]['rawdata'].empty:
                # проверяем, забрали ли мы чего-нибудь
                # генерируем все возможные участки-местоположения из n1
                #samples[1]['predicteddata'] = possible_location(samples[1]['rawdata'])
                # если возможно местоположение хотя бы на одном перегоне - идем дальше
                #if not samples[1]['predicteddata'].empty:
                    # генерируем новый сегмент данных, начиная с последней точки предыдущего сегмента
                    #start_ix = samples[1]['rawdata'].last_valid_index()
                    #samples[2]['rawdata'] = rawslice_generator(start_ix,segment_fr)
                    # если сегмент был сгенерирован(может быть None в случае если данные закончились)
                    #if not samples[2]['rawdata'].empty:
                        #samples[2]['predicteddata'] = possible_location(samples[2]['rawdata'])
                        #if not samples[2]['predicteddata'].empty:
                            #samples[3]['rawdata'] = pd.concat([samples[1]['rawdata'],samples[2]['rawdata']])
                            #samples[3]['predicteddata'] = possible_location(samples[3]['rawdata'])
                            # вычисляем параметры кривых n1, n2 и 2n и
                            # находим области пересечения(независимо в пределах одного перегона)
                            #print 'sample n1:\n'
                            last_slice_point = samples[1]['rawdata']['ratio'].iloc[-1]
                            samples[1]['pars'] = approximate(samples[1]['rawdata'])

                            #print 'sample n2:\n'
                            #samples[2]['pars'] = approximate(samples[2]['rawdata'])
                            #samples[2]['predictedsegments'] = possible_location_on_segment(samples[2]['predicteddata'],samples[2]['pars'])
                            #print 'sample 2n:\n'
                            #samples[3]['pars'] = approximate(samples[3]['rawdata'])
                            #samples[3]['predictedsegments'] = possible_location_on_segment(samples[3]['predicteddata'],samples[3]['pars'])
                            # если аппроксимация удалась(в первом приближении если МНК сработал в приницпе)
                            #if (samples[1]['pars']!={})&(samples[2]['pars']!={})&(samples[3]['pars']!={}):
                            if (samples[1]['pars']!={}):

                                samples[1]['predictedsegments'] = possible_location_on_segment(samples[1]['predicteddata'],samples[1]['pars'])

                                # сравниваем три набора сырых данных между собой
                                #segment_id = check_part_prediction(samples)
                                segment_ids = samples[1]['predictedsegments']
                                if len(segment_ids) == 1:
                                #if segment_id:
                                    delta_ratio = np.abs(samples[1]['predictedsegments']['ratio_from'].iloc[0] - samples[1]['predictedsegments']['ratio_to'].iloc[0])
                                    segment_id = list(segment_ids['segment_id'])[0]
                                    print segment_id , true_segment
                                    segment_position['part'] = True

                                    # если хотя бы в одном наборе сегмент был определен
                                    # проверяем, есть ли среди наборов точки потери/появления сигнала
                                    # todo: или смены БС
                                    #point = datacompare(samples[1]['pars'],samples[2]['pars'],samples[3]['pars'],segment_id)
                                    #if point:
                                        #print segment_id, point
                                     #   segment_position['point'] = True
                                else:
                                    start_ix = samples[1]['rawdata'].last_valid_index()
                                    lastslice = samples[1]['rawdata']
                        #else:
                          #  samples[1]['rawdata'] = samples[2]['rawdata']
                    #else:
                     #   start_ix = samples[1]['rawdata'].last_valid_index()
                            else:
                                start_ix = samples[1]['rawdata'].last_valid_index()

        if last_slice_point:
            try:
                mean_segment_time = subway_info[(subway_info['segment_id'] == true_segment)&(subway_info['city'] == CITY)]['time_median'].iloc[0]
            except:
                continue
            segment_length = lines_info[(lines_info['code'] == true_segment)&(lines_info['city'] == CITY)]['line_length'].iloc[0]
            #delta_ratio = np.abs((last_slice_point - startratio))

            check['time'] = np.abs((last_slice_point - startratio))*mean_segment_time
            check['dist_error'] = delta_ratio/2*segment_length
            print true_segment,segment_id,testsegment_id
            if segment_id == true_segment:
                check['segment_id'] = 1
            #if point!=None:
                #check['point'] = 1
                #check['point_dist'] = np.abs(point - startratio)
        else:
            pass
        print '\n' , check

        check_frame = pd.concat([check_frame,check])

    print check_frame
    check_frame = check_frame.set_index([range(0,len(check_frame))])
    check_frame.to_csv(outpath)

def rawdata_generator(segment_id = None,ratio = None,user = None):
    """
    Достает сырые данные с перегона из БД и точку, с которой началось позиционирование
    :param segment:
    :return:
    """
    # забираем Sпараметры для соединения с БД
    db_conn_pars = variables.DB_CONN[[hostname for hostname in variables.DB_CONN.keys() if variables.DB_CONN[hostname]['main']][0]]
    # используем dev функцию для записи таблицы в pandas.DataFrame
    #rawdata_table = utilities.get_pd_df_from_sql(db_conn=db_conn_pars,tab_name=variables.TABLES['parsed_cell'],city = CITY)
    smoothed_table = get_pd_df_from_sql(db_conn=db_conn_pars,city = CITY,NetworkGen = '2G',MNC = 2)
    # сортируем
    #rawdata_table_proc = rawdata_table.sort_values(by = ['city','User','TimeStamp'])
    #rawdata_table_proc = rawdata_table_proc[rawdata_table_proc['move_type'] == 'move']
    rawdata_table_proc = smoothed_table.sort_values(by = ['city'])
    # генерируем случайную точку - как будто это наше текущее положение
    if segment_id:
        id_from,id_to = segment_id.split('-')
        #rawdata_table_proc = rawdata_table_proc[(rawdata_table_proc['id_from'] == id_from)&(rawdata_table_proc['id_to'] == id_to)]
        rawdata_table_proc = rawdata_table_proc[rawdata_table_proc['segment_id'] == segment_id]
        if user:
            #rawdata_table_proc = rawdata_table_proc[rawdata_table_proc['User']==user]
            if ratio:
                rawdata_table_proc = rawdata_table_proc[(rawdata_table_proc['ratio']>=ratio)][0:1]

    startpoint = rawdata_table_proc.sample(1)

    #true_segment = startpoint['id_from'].iloc[0] + '-' + startpoint['id_to'].iloc[0]
    true_segment = startpoint['segment_id'].iloc[0]
    #startpoint_ix = startpoint.first_valid_index()
    start_ratio = startpoint['ratio'].iloc[0]
    startpoint = startpoint.iloc[0]
    # в соотвествтии с параметрами полученными на предыдущем шаге, достаем данные по сегменту, на котором мы находимся
    segment_fr = smoothed_table[(smoothed_table['city'] == startpoint['city'])&
                   (smoothed_table['segment_id'] == startpoint['segment_id'])
                 # (rawdata_table['User'] == startpoint['User'])&
                  #(rawdata_table['id_from'] == startpoint['id_from'])&
                  #(rawdata_table['id_to'] == startpoint['id_to'])&

                  #(rawdata_table['move_type'] == 'move')&
                  #(rawdata_table['race_id'] == startpoint['race_id']
        # )
    ]
    if segment_fr.empty:
        print 'empty frame'

      #  pass
    return start_ratio,segment_fr,true_segment

def rawslice_generator(startpoint_ix,segment_fr):
    """
    Извлекает кусок данных размера TIMESLICE, начиная с временной отсечки в startpoint
    :param startpoint_ix: индекс исходной точки {int}
    :param segment_fr: сегмент данных(перегон) {pd.DataFrame}
    :return:
    """
    rawslice = pd.DataFrame()
    try:
        startpoint = segment_fr.ix[startpoint_ix]
    except:
        pass

    # забираем начальную временную отсечку

    #startstamp = startpoint['TimeStamp']
    startstamp = startpoint['ratio']
    # вычисляем конечную временную отсечку
    finishstamp =  startstamp + DELTARATIO
    # отсекаем лишнее от перегона(нужно только то, что после startstamp)
    segment_fr = segment_fr[segment_fr.index>=startpoint_ix]
    finish_ix = None
    # извлекаем кусок данных
    # lastindex = segment_fr.last_valid_index()
    for i,row in segment_fr.iterrows():
        if (row['ratio']>finishstamp):
            finish_ix = i
            break

    if finish_ix!=None:
        rawslice = segment_fr.ix[startpoint_ix:finish_ix]
    if rawslice.empty:
        pass
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
        conn.close()
    else:
        pass
        #print ERRORS['noactive']
    return res1
def approximate(rawdata,rank = 1):
    """
    Аппроксимирует участок сырых данных размером t сек, полученный он-лайн
    :param rawdata: сырые данные, полученные он-лайн
    :param rank: степень полинома, под который будут полгоняться сырые данные
    :return: {'LAC1-CID1':dertype_1,'LAC2-CID2':dertype_2 ... ,'LACn-CIDn':dertype_n}
    """
    #ideal_rank = None


    lc_grouped = rawdata.groupby(['LAC','CID'])
    approxed = {}
    for (LAC,CID),lc_gr in lc_grouped:
        #t = np.array(lc_gr['TimeStamp'])
        t = np.array(lc_gr['ratio'])
        y = np.array(lc_gr['Power'])
        coef = np.polyfit(t,y,rank,full = True)
        control = variables.averaged_cell_pars['posit']
        #print LAC,CID,round(coef[0][0],6)
        if abs(coef[0][0])>control:
            try:
                quality = np.sqrt(coef[1][0]/(len(t)-1))
            except:
                pass

            model = np.poly1d(coef[0])
            ti = range(0,TIMESLICE/1000+1)
            predicted = model(ti)
            appr_fr = pd.DataFrame({'TimeStamp':ti,'Power':predicted})
            diffs = appr_fr.diff(1,0)
            dervs = (diffs['Power']/diffs['TimeStamp']).apply(np.arctan)
            derv = np.nanmean(dervs)
            if derv>=0:
                trend = 0
            else:
                trend = 1


            #plot(ti,predicted,'.')
            #predicted = [a[0] for a in predicted]
            #smoothed = {'Power': predicted,'TimeStamp': ti}
            approxed.update({(LAC,CID):trend})
            #return predicted,quality
        #if abs(coef[0][0])<control:
         #   return approxed.update({laccid:None})

    return approxed
def datacompare(s1,s2,s3,seg_id):
    """
    Сравнивает данные из трех наборов и проверяет,
    совпадает ли характер кривой во всех трех наборах данных

    :param s1: набор данных за первые n секунд
    :param s2: набор данных за вторые n секунд
    :param s3: набор данных за 2n секунд
    :return:
    """
    skeys = {}
    skeys[1] = s1.keys()
    skeys[2] = s2.keys()
    if skeys[1]!=skeys[2]:
        for (LAC,CID),dertype in s3.iteritems():
            for i in [1,2]:
                if (LAC,CID) not in skeys[i]:
                    keyS = i
                    #keyDertype = dertype
            break
        if keyS == 1:
            keyratio = 'ratio_from'
            dertype = 0
        if keyS == 2:
            keyratio = 'ratio_to'
            dertype = 1
        sqlitedbpath = variables.SQLITEDBPATH
        conn = sqlite3.connect(sqlitedbpath)
        sql = """ SELECT * FROM deriviative_types WHERE "LAC" = '%(LAC)s' AND "CID" = '%(CID)s' AND segment_id = '%(segment_id)s' AND city = '%(city)s' AND dertype = '%(keyDertype)s' """\
              %{"LAC":LAC,"CID":CID,"segment_id":seg_id,'city':CITY,'keyDertype':dertype}
        conn.rollback()
        conn.close()
        res = pd.read_sql(sql,conn,index_col='id')
        return list(set(res[keyratio]))[0]

    else:
        return None
def possible_location_on_segment(predicted_data,deriviatives):
    """
    Производит учет соседних станций и определяет местоположение на сегменте
    :param predicted_data: все возможные местоположения {pd.DataFrame}
    :param deriviatives: {'LAC1-CID1':dertype_1,'LAC2-CID2':dertype_2 ... ,'LACn-CIDn':dertype_n}
    :return:
    """
    raw_laccids = []
    predicted_seg_parts = pd.DataFrame()
    predicted_seg_parts2 = pd.DataFrame()
    # выполняем запрос и возвращаем pandas.DataFrame
    sqlitedbpath = variables.SQLITEDBPATH
    conn = sqlite3.connect(sqlitedbpath)
    cur = conn.cursor()
    for s in deriviatives.keys():
        laccid = s[0] + '-' + s[1]
        raw_laccids.append(laccid)
    for (LAC,CID),dertype in deriviatives.iteritems():
        sql = """ SELECT * FROM deriviative_types WHERE "LAC" = '%(LAC)s' AND "CID" = '%(CID)s' AND dertype = %(dertype)i AND city = '%(city)s'"""\
              %{"LAC":LAC,"CID":CID,"dertype":dertype,'city':CITY}
        #conn.rollback()
        res = pd.read_sql_query(sql,conn,index_col='id')
        # собираем всё в одну таблицу
        predicted_seg_parts = pd.concat([predicted_seg_parts,res])
    conn.close()
    if not predicted_seg_parts.empty:
        predicted_seg_parts['laccid'] = predicted_seg_parts['LAC'] + '-' + predicted_seg_parts['CID']
        pred_laccids = predicted_seg_parts.groupby(['segment_id'])['laccid'].apply(np.unique)
        # ищем пересечения областей из выборок между БС и соседними
        # работаем внутри сегмента и проверяем, есть ли в нем удовлетворяющие условия
 #       lacs = list(predicted_seg_parts.groupby(['segment_id'])['LAC'].apply(np.unique))
  #      cids = list(predicted_seg_parts.groupby(['segment_id'])['CID'].apply(np.unique))

        filtered_segments = []
        for segment_id, laccids in pred_laccids.iteritems():
            if set(raw_laccids).issubset(set(laccids)):
                filtered_segments.append(segment_id)

        predicted_seg_parts = predicted_seg_parts[predicted_seg_parts['segment_id'].isin(filtered_segments)]
        predicted_groupes = predicted_seg_parts.groupby(['segment_id'])

        for seg_id, pr_gr in predicted_groupes:
            interval = np.array([-1,2])
            for i,row in pr_gr.iterrows():
                interval = np.clip(interval,row['ratio_from'],row['ratio_to'])
            # если есть, то добавляем полученный промежуток в общую табличку
            if interval[1]-interval[0]>0:
                pred_seg_part = pd.DataFrame({'segment_id':[seg_id],'ratio_from':[interval[0]],'ratio_to':[interval[1]]})
                predicted_seg_parts2 = pd.concat([predicted_seg_parts2,pred_seg_part])
    return predicted_seg_parts2
    #print predicted_seg_parts2
def check_part_prediction(samples):
    seg_id = None
    #for i in [1,2,3]:
    try:
        segments = list(set(samples[1]['predictedsegments']['segment_id']))
    except:
        pass
    if len(segments) == 1:
        #if seg_id:
        #    if not seg_id == segments[0]:
        #        break
        #if not seg_id:
        #    seg_id = segments[0]
        return segments[0]
    else:
        return None
def get_pd_df_from_sql(db_conn,city,NetworkGen,MNC):
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)
    data = {'city':city,'NetworkGen':NetworkGen,'MNC':MNC}
    sql = """SELECT * FROM averaged_cell_meta WHERE "MNC" = %(MNC)s AND city = '%(city)s' AND "NetworkGen" = '%(NetworkGen)s'""" % data
    fr = pd.read_sql_query(sql,conn,'id')
    return fr
def segment_cellnum_more_two_generator(db_conn):

    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)
    data = {'city':CITY}

    sql = """SELECT * FROM subway_cell_quality_2G_megafon WHERE "MNC" = '2' AND city = '%(city)s' AND cell_num >= 2""" % data
    fr = pd.read_sql_query(sql,conn,'id')
    return fr
if __name__ == '__main__':
    main()