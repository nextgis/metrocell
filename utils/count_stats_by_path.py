#!/bin/env python
# encoding: utf-8

from collections import namedtuple, Counter

import urllib
import json

import csv

import pandas as pd


# Параметры для получения данных из репозитория
BASE_URL = 'https://api.github.com/repos'
OWNER = 'nextgis'
REPO = 'metrocell'
PATH = 'data/proc/msk/cell'


class InvalidFileNameError(ValueError):
    """Название файла не соответствует предполагаемому шаблону.
    """
    pass


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
    На выходе -- список из namedtuple('DataValues', ['Begin', 'End', 'Stop','User', 'NetworkGen', 'MNC', 'MCC'])
    """
    url = url_dict['download_url']
    filename = url_dict['name']
    begin, end = get_begin_id(filename), get_end_id(filename)
    stop = path_is_stop(filename)
    
    
    subset = ['User', 'NetworkGen', 'MNC', 'MCC']
    Record = namedtuple('DataValues', ['Begin', 'End', 'Stop'] + subset)
    
    frame = pd.io.parsers.read_csv(url)
    frame = frame.drop_duplicates(subset = subset)
    
    result = []
    for _, row in frame.iterrows():
        data = Record(begin, end, stop, row['User'], row['NetworkGen'], row['MNC'], row['MCC'])
        result.append(data)
    
    return result
    
def get_stat(description_list):
    """Получить статистику поездок по списку словарей, в котором
    хранится описание файлов на гитхабе (возвращается функцией get_file_list)
    """
    data = []
    for url_desctiption in description_list:
        stat = describe_file(url_desctiption)
        data += stat
        
    return Counter(data)


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
    
    filename = 'stat.csv'
    
    files_description = get_file_list(BASE_URL, OWNER, REPO, PATH)
    c  = get_stat(files_description)
    save_stat(c, filename)
    
