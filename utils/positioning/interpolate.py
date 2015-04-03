# -*- coding: utf-8 -*-

'''
Вспомогательный файл,
производит интерполяцию логов сигналов сотовой сети на равные интервалы
(1 секунда) и запись их в единую БД формата sqlite.

Вызов:
    python interpolate.py out_data/
где out_data -- каталог, содержащий логи, обработанные инструментом log_georef
'''


import sys
import os
import glob

from sqlalchemy import create_engine

import pandas as pd
import numpy as np
from scipy import interpolate

def get_local_file_list(path):
    '''Считывает список файлов (логов) из локального каталога
    '''
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(cur_dir, path)
    pattern = path + '/*.csv'

    return glob.glob(pattern)

def prepare_frame(url, max_lag=16, obs_id=None):
    '''Вспомогательная функция:
    считывает данные в виде датафрейма, чистит их от мусора,
    преобразовывает типы.
    Затем создает новый датафрейм с равноотстоящими отсчетами (интерполируюя данные)
    и записывает несколько новых колонок с лагами от 1 до max_lag
    '''
    frame = pd.io.parsers.read_csv(url)

    if ('Power' not  in frame.columns) and ('RSSI' in frame.columns):
        frame['Power'] = frame['RSSI']
    if ('Power' in frame.columns) and ('RSSI' not in frame.columns):
        frame['RSSI'] = frame['Power']

    frame.Power[pd.isnull(frame['Power'])] = frame.RSSI[pd.isnull(frame['Power'])].copy()
    frame.Power[pd.isnull(frame['Power'])] = 0

    # Удалим колонки, которые не нужны
    cols = [
        'User', 'Name', 'NetworkGen', 'NetworkType',
        'RSSI'
    ]
    frame.drop(cols, axis=1, inplace=True)
    try:
        frame = frame[frame['Active'] == '1']
    except:
        pass

    m0 = min(frame.TimeStamp)
    m1 = max(frame.TimeStamp)
    tnew = np.arange(m0, m1, 1000)

    f = interpolate.interp1d(frame.TimeStamp, frame.Power)
    fx = interpolate.interp1d(frame.TimeStamp, frame.x)
    fy = interpolate.interp1d(frame.TimeStamp, frame.y)
    fmnc = interpolate.interp1d(frame.TimeStamp, frame.MNC, kind='nearest')
    fmcc = interpolate.interp1d(frame.TimeStamp, frame.MCC, kind='nearest')
    flac = interpolate.interp1d(frame.TimeStamp, frame.LAC, kind='nearest')
    fcid = interpolate.interp1d(frame.TimeStamp, frame.CID, kind='nearest')


    frame = pd.DataFrame(
               {'Time': tnew, 'Power': f(tnew),
                'x': fx(tnew), 'y': fy(tnew),
                'MCC': [int(mcc) for mcc in fmcc(tnew)],
                'MNC': [int(mnc) for mnc in fmnc(tnew)],
                'LAC': [int(lac) for lac in flac(tnew)],
                'CID': [int(cid) for cid in fcid(tnew)]
                })

    #~ for i in range(1, max_lag+1):
        #~ name = 'L' + str(i).zfill(2)
        #~ frame[name] = frame.Power.shift(-i)

    # frame = frame.dropna()

    url = os.path.basename(url)
    # url: 109-108-2014120109.csv
    frame['SEG'] = url[:7]
    frame['Date'] = url[8:-4]
    if obs_id:
        frame['obs_id'] = obs_id

    return frame


if __name__ == "__main__":
    dirname = sys.argv[1]

    #~ import ipdb
    #~ ipdb.set_trace()

    database = pd.DataFrame()
    file_list = get_local_file_list(dirname)
    obs_id = 1
    for url in file_list:
        frame = prepare_frame(url, obs_id=obs_id)
        database = pd.concat([database, frame])
        obs_id += 1
    # database.to_csv(os.path.basename('database.csv'), index=False)

    engine = create_engine('sqlite:///obcervations.db')

    database.to_sql('observ', engine, flavor='sqlite', index=False)






