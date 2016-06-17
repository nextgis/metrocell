# -*- coding: utf-8 -*-
# required libs
import sqlite3,psycopg2
import numpy as np
import pandas as pd
import itertools
import time
import sys
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
    parser.add_argument('-c','--city',type = str,required = True, choices=variables.CITIES, help = 'Город, в котором находится пользователь')
    parser.add_argument('-t','--timeslice',type = int,required = True,help = 'Время, которое необходимо для сбора одного сегмента данных (ед.изм.: мсек)')
    parser.add_argument('-d','--onlinedatapath',type=str,help='Модель данных, поступающих в систему в режиме онлайн(путь до файла)')
    parser.add_argument('-s','--startstamp',type=int,required = True,help='Момент запуска приложения')
    parser.add_argument('-f','--finishstamp',type=int,required = True,help='Момент прибытия в станцию назначения')
    args = parser.parse_args()
    localize(args.startstamp, args.finishstamp,args.city,args.timeslice,args.onlinedatapath)
    # параметры для наборов данных:
    # коэффициенты кривой
    # характер кривой в начале, середине и конце(возрастает, убывает или примерно 0)
    # значения производной в начале, середине и конце [-1;1]
def localize(startstamp,finishstamp,city='spb',timeslice=10000,
             onlinedatapath = None,user = None,race_id = None):

    global CITY
    CITY = city
    global TIMESLICE
    TIMESLICE = timeslice
    #global ERRORS
    #ERRORS = {'segmentend':"Нет данных по перегону в городе с id = %s, собранных пользователем в некоторый заезд!"%CITY,
    #          'noactive':"Нет активных вышек!"
    #          }

    db_conn = variables.DB_CONN[[hostname for hostname in variables.DB_CONN.keys() if variables.DB_CONN[hostname]['main']][0]]
    lines_info = utilities.get_pd_df_from_sql(db_conn,variables.TABLES['lines'],'ogc_fid')
    check_frame = pd.DataFrame()

    last_slice_point = None
    segment_position = {
            'id':False,     #перегон
            'part':False,   # часть перегона(начало|середина|конец)
            'point':False   # точка на перегоне
        }
    #testsegment_id = testsegments.sample(1)['segment_id'].iloc[0]
    segment_id, point = None, None
    usermoves = True
    #if segment_data!=None:
    #start_ratio,segment_fr,true_segment = get_rawdata_by_race(db_conn,CITY,segment_data['race_id'],segment_data['user'])
    if onlinedatapath!=None or (user!=None and race_id!=None):
        segment_fr = get_online_data_since_timestamp(startstamp,finishstamp,onlinedatapath,user,race_id)


    #check = pd.DataFrame({'b':[None],'time':[None],'dist_error':[None],'segment':[true_segment]})
    #else:
     #   start_ratio,segment_fr,true_segment = rawdata_generator(segment_id=testsegment_id,ratio=startratio,user = user)
    #segment_fr = segment_fr.sort_values(by = ['TimeStamp'])
    segment_fr = segment_fr.sort_values(by = ['TimeStamp'])
    segment_fr = segment_fr.set_index([range(0,len(segment_fr))])
    start_ix = segment_fr.index[0]
    #start_ix = segment_fr[(segment_fr['ratio'] == start_ratio)].index[0]
    # последняя точка на перегоне
    # пока не будет получена окончательная координата объекта,
    # сбор данных не прекращать
    iter = 0
    while(segment_position['part'] == False)and(usermoves == True):
        iter+=1
        rawdata = rawslice_generator(start_ix,segment_fr,timeslice)
        # забираем слайс данных в TIMESTAMP секунд
        if rawdata.empty:
            usermoves = False
        else:
            last_slice_point = rawdata['TimeStamp'].iloc[-1]
            rawpars = approximate(rawdata)
            if (rawpars!={}):
                predictedsegments = possible_location_on_segment(rawdata,rawpars)


                if len(predictedsegments) == 1:
                    delta_ratio = np.abs(predictedsegments['ratio_from'].iloc[0] - predictedsegments['ratio_to'].iloc[0])
                    segment_id = list(predictedsegments['segment_id'])[0]
                    # print segment_id , true_segment
                    segment_position['part'] = True

                else:
                    print iter,predictedsegments
                    start_ix = rawdata.last_valid_index()
                    lastslice = rawdata

            else:
                start_ix = rawdata.last_valid_index()
    if last_slice_point and segment_id:

        segment_length = lines_info[(lines_info['code'] == segment_id)&(lines_info['city'] == CITY)]['line_length'].iloc[0]

        time = (last_slice_point - startstamp)/1000
        dist_error = delta_ratio/2*segment_length
        print 'Вы на прегоне: ',segment_id,'\nВремя на позиционирование: ',time, 'сек\nРадиус ошибки в метрах: ',dist_error,'м'
        return {'segment_id':[segment_id],'time':[time],'dist_error':[dist_error]}
        #check['dist_error'] = delta_ratio/2*segment_length
        #print true_segment,segment_id,testsegment_id
        #if segment_id == true_segment:
         #   check['segment_id'] = 1
        #if point!=None:
            #check['point'] = 1
            #check['point_dist'] = np.abs(point - startratio)
    else:
        return {'segment_id':[None],'time':[None],'dist_error':[None]}
    #print '\n' , check
    #check_frame = pd.concat([check_frame,check])
    #print check_frame
    #check_frame = check_frame.set_index([range(0,len(check_frame))])
    #check_frame.to_csv(outpath)

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
    if segment_id!=None:
        id_from,id_to = segment_id.split('-')
        #rawdata_table_proc = rawdata_table_proc[(rawdata_table_proc['id_from'] == id_from)&(rawdata_table_proc['id_to'] == id_to)]
        rawdata_table_proc = rawdata_table_proc[rawdata_table_proc['segment_id'] == segment_id]
        if ratio!=None:
            rawdata_table_proc = rawdata_table_proc[(rawdata_table_proc['ratio']>=ratio)][0:1]

        #if user:
            #rawdata_table_proc = rawdata_table_proc[rawdata_table_proc['User']==user]

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

def rawslice_generator(startpoint_ix,segment_fr,timeslice):
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

    startstamp = startpoint['TimeStamp']
    #startstamp = startpoint['ratio']
    # вычисляем конечную временную отсечку
    finishstamp = startstamp + timeslice
    #finishstamp =  startstamp + DELTARATIO

    # отсекаем лишнее от перегона(нужно только то, что после startstamp)
    segment_fr = segment_fr[segment_fr.index>=startpoint_ix]
    finish_ix = None
    # извлекаем кусок данных
    # lastindex = segment_fr.last_valid_index()
    for i,row in segment_fr.iterrows():
        #if (row['ratio']>finishstamp):
        if (row['TimeStamp']>finishstamp):
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
        t = np.array(lc_gr['TimeStamp'])
        #t = np.array(lc_gr['ratio'])
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
def check_interesection(fr):
    """
    Проверяет пересекаются, ли сигналы от каких либо двух станций. Обычно, такая картина наблюдается в момент
    смены базовой станции в середине перегона
    :param fr: собранный участок данных {pd.DataFrame}
    :return:
    """
    intersected_pt = None
    lc_from_ix,lc_to_ix = None,None
    fr['laccid'] = fr['LAC'].astype(str) + '-' + fr['CID'].astype(str)
    #intersected_pts = pd.DataFrame(columns = ['from','to','ratio'])
    intersected_pts = {}
    alllaccids = list(set(list(fr['laccid'])))
    first_delta_power = None
    #fr_grouped = fr.groupby(['ratio'])
    fr_grouped = fr.groupby(['TimeStamp'])
    # перебираем все возможные сочетания вышек
    i=0
    for laccids_set in itertools.combinations(alllaccids,2):

        # определяем, пересекается ли пара
        for stamp,fr_gr in fr_grouped:
            # определяем разницу в силе сигнала между станицями
            lc_from = fr_gr[fr_gr['laccid'] == laccids_set[0]]
            lc_to = fr_gr[fr_gr['laccid'] == laccids_set[1]]
            try:
                delta_power = lc_to['Power'].iloc[0] - lc_from['Power'].iloc[0]
                if np.isnan(delta_power):
                    break
            except:
                # lc_to или lc_from пустые
                break
            # если вышка, сила сигнала от которой выше еще не определена
            if not lc_from_ix:
                if delta_power>0:
                    lc_from_ix = laccids_set[1]
                    lc_to_ix = laccids_set[0]
                else:
                    lc_from_ix = laccids_set[0]
                    lc_to_ix = laccids_set[1]
            # если это первая в итерации разница, запоминаем ее знак
            if first_delta_power == None:
                first_delta_power = delta_power
                first_dp_mark = str(first_delta_power)[0]
                pass

            else:
                # если знак изменился, значит смена произошла
                if ((str(delta_power)[0] != '-')&(first_dp_mark=='-'))|\
                        ((str(delta_power)[0] == '-')&(first_dp_mark!='-')):
                    #intersected_pt = pd.DataFrame({'from':[lc_from_ix],'to':[lc_to_ix],'ratio':[ratio]})
                    #intersected_pts = intersected_pts.append(intersected_pt,ignore_index = True)
                    #intersected_pt = {'from':lc_from_ix,'to':lc_to_ix,'ratio':ratio}
                    i+=1
                    intersected_pt = {'from':lc_from_ix,'to':lc_to_ix,'stamp':stamp}
                    intersected_pts[i] = intersected_pt
                    lc_from_ix = None
                    lc_to_ix = None
                    break


    #return intersected_pts
    return intersected_pts
def find_intersection_in_db(predicted_seg_parts2,intersected_pt):

    fpt_intersected_pts = pd.DataFrame()
    first_delta_power = None

    lc_from_ix,lc_to_ix = None,None
    control_position = None
    sqlitedbpath = variables.SQLITEDBPATH
    conn = sqlite3.connect(sqlitedbpath)
    #row = predicted_seg_parts2.iloc[0]
    for i,row in predicted_seg_parts2.iterrows():
        #min_delta_power = 10000
        seg_id = row['segment_id']
        sql = """ SELECT * FROM fingerprint WHERE segment_id = '%(segment_id)s' AND city = '%(city)s' """\
              %{'segment_id':seg_id,'city':CITY}
        fingerprint = pd.read_sql_query(sql,conn,index_col='id')
        fingerprint = fingerprint.sort_values(by = ['ratio'])
        fingerprint = fingerprint[(fingerprint['ratio']>row['ratio_from'])&(fingerprint['ratio']<row['ratio_to'])]
        fingerprint['laccid'] = fingerprint['LAC'] + '-' + fingerprint['CID']
        ratio_grouped = fingerprint.groupby(['ratio'])
        for ratio_ix , ratio_gr in ratio_grouped:
            lc_from = ratio_gr[ratio_gr['laccid'] == intersected_pt['from']]
            lc_to = ratio_gr[ratio_gr['laccid'] == intersected_pt['to']]
            if (len(lc_to)==1)and(len(lc_from)==1):
                delta_power = lc_to['Power'].iloc[0] - lc_from['Power'].iloc[0]
                if not lc_from_ix:
                    if delta_power>0:
                        lc_from_ix = intersected_pt['to']
                        lc_to_ix = intersected_pt['from']
                    else:
                        lc_from_ix = intersected_pt['from']
                        lc_to_ix = intersected_pt['to']
                # если это первая в итерации разница, запоминаем ее знак
                if first_delta_power == None:
                    first_delta_power = delta_power
                    first_dp_mark = str(first_delta_power)[0]
                    pass

                else:
                    # если знак изменился, значит смена произошла
                    if ((str(delta_power)[0] != '-')&(first_dp_mark=='-'))|\
                            ((str(delta_power)[0] == '-')&(first_dp_mark!='-')):
                        #intersected_pt = pd.DataFrame({'from':[lc_from_ix],'to':[lc_to_ix],'ratio':[ratio]})
                        #intersected_pts = intersected_pts.append(intersected_pt,ignore_index = True)
                        #intersected_pt = {'from':lc_from_ix,'to':lc_to_ix,'ratio':ratio_ix}
                        fpt_intersected_pt = pd.DataFrame({'ratio_from':[ratio_ix],'ratio_to':[ratio_ix],'segment_id':[seg_id]})
                        fpt_intersected_pts = fpt_intersected_pts.append(fpt_intersected_pt,ignore_index = True)

                        lc_from_ix = None
                        lc_to_ix = None
                        break
                    #
                    #break
        #control_positions.update({seg_id:min_delta.keys()[0]})


        #control_position = {seg_id:min_delta_res}
    conn.close()
    return fpt_intersected_pts
def possible_location_on_segment(predicted_data,deriviatives):
    """
    Производит учет соседних станций и определяет местоположение на сегменте
    :param predicted_data: все возможные местоположения {pd.DataFrame}
    :param deriviatives: {'LAC1-CID1':dertype_1,'LAC2-CID2':dertype_2 ... ,'LACn-CIDn':dertype_n}
    :return:
    """
    # точка пересечения двух сигналов
    intersected_pts = check_interesection(predicted_data)
    control_positions = pd.DataFrame()

    raw_laccids = []
    predicted_seg_parts = pd.DataFrame()
    predicted_seg_parts2 = pd.DataFrame()
    # выполняем запрос и возвращаем pandas.DataFrame
    sqlitedbpath = variables.SQLITEDBPATH
    conn = sqlite3.connect(sqlitedbpath)
    #cur = conn.cursor()
    # достаем все laccid из он-лайн данных
    for s in deriviatives.keys():
        laccid =str(s[0]) + '-' + str(s[1])
        raw_laccids.append(laccid)
    # достаем из базы все участки где наблюдается та же картина отдельно по всем LAC CID
    for (LAC,CID),dertype in deriviatives.iteritems():
        sql = """ SELECT * FROM deriviative_types WHERE "LAC" = '%(LAC)s' AND "CID" = '%(CID)s' AND dertype = %(dertype)i AND city = '%(city)s'"""\
              %{"LAC":LAC,"CID":CID,"dertype":dertype,'city':CITY}
        #conn.rollback()
        res = pd.read_sql_query(sql,conn,index_col='id')
        # собираем всё в одну таблицу
        predicted_seg_parts = pd.concat([predicted_seg_parts,res])

    # если такие перегоны есть в принципе
    if not predicted_seg_parts.empty:
        predicted_seg_parts['laccid'] = predicted_seg_parts['LAC'] + '-' + predicted_seg_parts['CID']
        pred_laccids = predicted_seg_parts.groupby(['segment_id'])['laccid'].apply(np.unique)
        # ищем пересечения областей из выборок между БС и соседними
        # работаем внутри пергеона и проверяем, есть ли в нем удовлетворяющие условия
        min_percentage = 100
        filtered_segments = []

        for segment_id, laccids in pred_laccids.iteritems():
            num_lc=0
            for lc in laccids:
                if lc in raw_laccids:
                    num_lc+=1
            if num_lc!=0:
                cur_percentage = abs(len(set(raw_laccids))-num_lc)

                if cur_percentage<min_percentage:
                    min_percentage = cur_percentage
                    filtered_segments = [segment_id]
                if cur_percentage==min_percentage:
                    filtered_segments.append(segment_id)
                #if set(raw_laccids).issubset(set(laccids)):
                 #   filtered_segments.append(segment_id)
        filtered_segments = list(set(filtered_segments))
        predicted_seg_parts = predicted_seg_parts[predicted_seg_parts['segment_id'].isin(filtered_segments)]
        predicted_groupes = predicted_seg_parts.groupby(['segment_id'])
        # проверяем пересекаются ли сигналы от выбранных laccid
        # если да - то это те сегменты которые нужны
        # нет - фильтруем их
        for seg_id, pr_gr in predicted_groupes:
            interval = np.array([-1,2])
            for i,row in pr_gr.iterrows():
                interval = np.clip(interval,row['ratio_from'],row['ratio_to'])
            # если есть, то добавляем полученный промежуток в общую табличку
            if interval[1]-interval[0]>0:
                pred_seg_part = pd.DataFrame({'segment_id':[seg_id],'ratio_from':[interval[0]],'ratio_to':[interval[1]]})
                predicted_seg_parts2 = pd.concat([predicted_seg_parts2,pred_seg_part])

        if intersected_pts!={}:
            for pt in intersected_pts:
                control_position = find_intersection_in_db(predicted_seg_parts2,intersected_pts[pt])
                if not control_position.empty:
                    #predicted_seg_parts2 = predicted_seg_parts2[predicted_seg_parts2['segment_id'] == min(control_positions, key=control_positions.get)]
                    control_positions = control_positions.append(control_position)
        if len(control_positions)==1:
            predicted_seg_parts2 = control_positions

        #for i,row in predicted_seg_parts2.iterrows():
        #    sql = """ SELECT * FROM deriviative_types WHERE segment_id = '%(segment_id)s' AND city = '%(city)s'"""\
        #      %{"segment_id":row['segment_id'],'city':CITY}
        #    #conn.rollback()
        #    res = pd.read_sql_query(sql,conn,index_col='id')
        #    res['laccid'] = res['LAC'] + '-' + res['CID']
        #    res = res[~res['laccid'].isin(raw_laccids)]
        #    res_grouped = res.groupby(['segment_id'])
        #    for seg_id,res_gr in res_grouped.iterrows():
        #
    conn.close()
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
def get_rawdata_by_race(db_conn,city,race_id,user):
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)
    conn.rollback()
    data = {'user':user,'city':city,'race_id':race_id}
    sql = """ SELECT * FROM parsed_cell WHERE
                                "User" = '%(user)s' AND
                                "race_id" = '%(race_id)s' AND
                                "city" = '%(city)s' AND
                                "move_type" = 'move'
                                """%(data)
    fr = pd.read_sql_query(sql,conn,'id')
    fr['segment_id'] = fr['id_from'] + '-' +fr['id_to']
    segments = set(fr['segment_id'])
    if len(segments)==1:
        true_segment = list(segments)[0]
        start_ratio = fr['ratio'].iloc[0]
        return start_ratio,fr,true_segment
    else:
        raise Exception

def segment_cellnum_more_two_generator(db_conn):
    segmentslen = {}
    connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
    conn = psycopg2.connect(connString)
    data = {'city':CITY}

    #sql = """SELECT * FROM subway_cell_quality_2G_megafon WHERE "MNC" = '2' AND city = '%(city)s' AND cell_num >= 2""" % data
    #fr = pd.read_sql_query(sql,conn,'id')

    sql = """SELECT * FROM subway_cell_quality_2G_megafon WHERE "MNC" = '2' AND city = '%(city)s' """%data
    fr = pd.read_sql_query(sql,conn,'id')
    segmentslen['all'] = len(set(list(fr['segment_id'])))
    fr = fr[fr['cell_num']>=2]
    important_segments = list(set(list(fr['segment_id'])))
    segmentslen['important'] = len(important_segments)
    print 'Percentage of important segments', 1.0*segmentslen['important']/segmentslen['all']*100

    return important_segments
def get_online_data_since_timestamp(startstamp,finishstamp,onlinedatapath=None,user=None,race_id=None):

    if onlinedatapath:
        fr = pd.read_table(onlinedatapath,sep = ';')
        fr = fr[(fr['TimeStamp']>startstamp)&(fr['TimeStamp']<finishstamp)]
    elif user and race_id:
        db_conn = variables.DB_CONN[[hostname for hostname in variables.DB_CONN.keys() if variables.DB_CONN[hostname]['main']][0]]
        connString = "host = %s user = %s password = %s dbname = %s" % (db_conn['host'],db_conn['user'],db_conn['password'],db_conn['dbname'])
        conn = psycopg2.connect(connString)
        sql = """ SELECT * FROM parsed_cell WHERE "User" = '%(user)s' AND "race_id" = '%(race_id)s' """\
              %{"user":user,"race_id":race_id}
        fr = pd.read_sql_query(sql,conn,'id')
        fr = fr[(fr['TimeStamp']>startstamp)&(fr['TimeStamp']<finishstamp)]
    else:
        raise Exception("Could not extract rawdata!")
    return fr
if __name__ == '__main__':
    main()