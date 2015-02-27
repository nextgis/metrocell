#!/bin/env python
# encoding: utf-8

import glob
import os

from collections import namedtuple, Counter

import urllib
import json
import geojson
import csv

import pandas as pd


# Параметры для получения данных из репозитория

BASE_URL = 'https://api.github.com/repos'
OWNER = 'nextgis'
REPO = 'metrocell'
PATH = 'data/proc/msk/cell'
GEOJSON_URL = 'https://raw.githubusercontent.com/nextgis/metrocell/master/segments/raw/msk/metro_lines.geojson'


class InvalidFileNameError(ValueError):
    """Название файла не соответствует предполагаемому шаблону.
    """
    pass


def get_geojson(url=GEOJSON_URL):
    """Возвращает geojson, представляющий линии метро и
    прочитанный из репозиторя github
    """
    
    f = urllib.urlopen(url)
    data = geojson.loads(f.read())
    
    return data
    

def get_file_list(base_url, owner, repo, path):
    """Возвращает список файлов, хранящихся в репозитории github в 
    указанном каталоге
    """
    url = '/'.join(['https://api.github.com/repos', owner, repo, 'contents', path])
    
    f = urllib.urlopen(url)
    flist = json.loads(f.read())
    
    # Отрежем лишнюю информацию:
    flist = [{
                'name': f['name'],
                'path': f['path'],
                'download_url': f['download_url']
            }
            for f in flist
    ]
    
    return flist
    
def get_local_file_list(path='../data/proc/msk/cell/'):
    '''Считывает список файлов (логов) из локального каталога
    '''
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(cur_dir, path)
    pattern = path + '/*.csv'
    
    return glob.glob(pattern)
    
    
    

    
def _is_name_valid(filename):
    """Простая проверка на то, что filename
    является подходящим именем файла для логов
    пример "хорошего" filename: 
        011-012-2014121214.csv
        011-012-2014121214-stop.csv
    """
    if filename[-4:] != '.csv':
        return False
    
    if not (filename[:3]+filename[4:7]).isdigit():
        return False
    
    if filename[3] != '-':
        return False
    
    return True
    

def get_operator(mnc):
    station_owner = {99: 'beeline', 2: 'megafon', 1: 'mts'}

    operator = 'unknown'
    try:
        mnc = int(mnc)
    except Error:
        return operator
    try:
        operator = station_owner[mnc]
    except KeyError:
        pass
    return operator

def get_begin_id(filename):
    """По имени файла возвращает id начала сегмента пути
    пример filename: 011-012-2014121214-stop.csv
    """
    if not _is_name_valid(filename):
        raise InvalidFileNameError
    
    return filename[:3]


def get_end_id(filename):
    """По имени файла возвращает id начала сегмента пути
    пример filename: 011-012-2014121214-stop.csv
    """
    if not _is_name_valid(filename):
        raise InvalidFileNameError
    
    return filename[4:7]


def path_is_stop(filename):
    """Возвращает истину, если название файла соотвествует
    сегменту остановки
    """
    if not _is_name_valid(filename):
        raise InvalidFileNameError
        
    return filename[-8: -4] == 'stop'
    

def describe_file(url_dict):
    """Возвращает список словарей, состоящий из всех уникальных (т.е. без дублирования)
    записей, встреченных в файле.
    На входе -- словарь, возвращаемый функцией get_file_list
    На выходе -- dataframe с колонками ['Begin', 'End', 'Stop','User', 'NetworkGen', 'MNC', 'MCC']
    """
    if type(url_dict) is dict:
        url = url_dict['download_url']
        filename = url_dict['name']
    else:
        url = url_dict
        filename = os.path.basename(url)
        
    begin, end = get_begin_id(filename), get_end_id(filename)
    stop = path_is_stop(filename)
    
    subset = ['User', 'NetworkGen', 'MNC', 'MCC']
    frame = pd.io.parsers.read_csv(url)
    frame = frame.drop_duplicates(subset = subset)

    for col in frame.columns:
        if col not in subset:
            frame.drop(col, axis=1, inplace=True)

    frame['Begin'] = begin
    frame['End'] = end
    frame['Stop'] = stop
    
    return frame


def get_stat(description_list, print_report=False):
    """Получить статистику поездок по списку словарей, в котором
    хранится описание файлов на гитхабе (возвращается функцией get_file_list)

    print_report выводить ли информацию о процессе сбора данных
    """
    data = pd.DataFrame()
    size = len(description_list)
    i = 0
    for url_desctiption in description_list:
        frame = describe_file(url_desctiption)
        data = pd.concat([data, frame])
        i += 1

        if print_report:
            print 'Count: %s / %s' %(i, size)

    return data


def join_json_stat(json_data, stat):
    """Расширяет geojson, добавляя к свойствам features
    статистические данные в формате get_stat.

    Вся "политика" производится тут:
      имя пользователя удаляется,
      замеры на станции игнорируются,
      mcc удаляется
    """

    features = json_data['features']
    for feat in features:
        props = feat['properties']
        begin_station, end_station = props['CODE'].split('-')
        
        description = []
        filtered = stat[(stat['Stop'] == False) & 
                        (stat['Begin'] == begin_station) &
                        (stat['End'] == end_station)]
        for mnc in filtered.MNC.unique():
            for ng in filtered.NetworkGen.unique():
                flt = filtered[(filtered['MNC'] == mnc) &
                               (filtered['NetworkGen'] == ng)]
                count = len(flt)
                if count > 0:
                    description.append({'operator': get_operator(mnc), 
                                        'net': ng, 'count': count})
        
        props['TRAVELS'] = description

    return json_data


def save_json(data, filename):
    with open(filename, 'w') as outfile:
        json.dump(data, outfile)


def save_stat(stat, filename):
    """Сохранить собранную статистику в csv файл
    """
    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['Begin', 'End', 'Stop','User', 'NetworkGen', 'MNC', 'MCC', 'COUNT'])
        for key, count in stat.iteritems():
            begin, end, stop, user, NetworkGen, MNC, MCC = key
            writer.writerow([begin, end, stop, user, NetworkGen, MNC, MCC, count])


if __name__ == "__main__":
    
    filename = 'segments.json'
    
    files_description = get_local_file_list()
    #~ files_description = get_file_list(BASE_URL, OWNER, REPO, PATH)
    c = get_stat(files_description, print_report=True)
    json_data = get_geojson()
    json_data = join_json_stat(json_data, c)
    save_json(json_data, filename)
    
