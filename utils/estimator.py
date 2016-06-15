# -*- coding: utf-8 -*-
from argparse import ArgumentParser
import pandas as pd
import positioning
import variables
import sys
parser = ArgumentParser(description = "Оценщик алгоритма позиционирования")
parser.add_argument('-t','--testsamples',required=True,help = "Исходная таблица с данными сэмплов(путь)")
args = parser.parse_args()

estimated_positions = pd.DataFrame()
outpath = variables.OUTCSVPATH
testsamples = pd.read_csv(args.testsamples,sep=',')

for i,row in testsamples.iterrows():

    #sys.stdout.write(str(i) + '/' + str(segmentslength) + '\n')
    #sys.stdout.flush()
    estimated_position = positioning.localize(row['timestamp_start'],
                         row['timestamp_finish'],
                         user = row['user'],
                         race_id = row['race_id']
                         )

    estimated_position['init_segment_id'] = [row['segment_id']]
    estimated_position['user'] = [row['user']]
    if estimated_position['segment_id'][0] == row['segment_id']:
        estimated_position['segment'] = [1]
    else:
        estimated_position['segment'] = [0]
    estimated_positions = estimated_positions.append(pd.DataFrame.from_dict(estimated_position),ignore_index = True)
    print row['timestamp_start'],row['timestamp_finish']
print estimated_positions

estimated_positions.to_csv(outpath)