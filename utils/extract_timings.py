# -*- encoding: utf-8 -*-
# prepare timings
# run from util folder
# Data will be put in the same folder with script
# example: python extract_timings.py city
# 

import os
import csv
import datetime
import glob
import sys

#ugly, I know)

city = sys.argv[1]

cell_data_loc = '/media/sim/Windows7_OS/work/metrocell/data/proc/' + city + '/cell'
graph_loc = '/home/sim/work/metro4all/repo/data/' + city + '/graph.csv'

#resulting csv with timings on all edges
f_time_fields = ('ID_FROM','ID_TO','TIME')

f_time_edge = open('graph_time_edge_' + city + '.csv','wb')
f_time_edge.write(','.join(f_time_fields))
f_time_edge.write('\n')
graph_data_time_edge_csv = csv.DictWriter(f_time_edge, fieldnames=f_time_fields)

#resulting csv with timings on stops
f_time_stop = open('graph_time_stop_' + city + '.csv','wb')
f_time_stop.write(','.join(f_time_fields))
f_time_stop.write('\n')
graph_data_time_stop_csv = csv.DictWriter(f_time_stop, fieldnames=f_time_fields)

#resulting csv with timings on edges only from the current graph (twice less edges)
f_time_fields = ('ID_FROM','ID_TO','NAME_FROM','NAME_TO','COST') #,'TIME_DIF')

f_time_edge_graph = open('graph_time_edge_graph_' + city + '.csv','wb')
f_time_edge_graph.write(';'.join(f_time_fields))
f_time_edge_graph.write('\n')
graph_data_time_edge_graph_csv = csv.DictWriter(f_time_edge_graph, fieldnames=f_time_fields, delimiter=";")


graph_data = []

graph_csv = open(graph_loc, 'rb')
graph_reader = csv.DictReader(graph_csv,delimiter=';')

for row in graph_reader:
    graph_data.append(row)

for graph_entry in graph_data:

    os.chdir(cell_data_loc)
    time_diff_secs_edge_pair = []
    
    for id_from,id_to in ((graph_entry['id_from'],graph_entry['id_to']),(graph_entry['id_to'],graph_entry['id_from'])):
        seg_id = id_from.zfill(3) + '-' + id_to.zfill(3)
        seg_files = glob.glob(seg_id + '*.*')

        time_diff_secs_edge = []
        time_diff_secs_stop = []

        for seg_csv_name in seg_files:
            seg_data = []
            if 'stop' not in seg_csv_name:
                seg_csv = open(seg_csv_name, 'rb')
                seg_reader = csv.DictReader(seg_csv,delimiter=',')

                for row in seg_reader:
                    seg_data.append(row)

                print(seg_id + ' tunnel')
                start_time = datetime.datetime.fromtimestamp(float(seg_data[1]['TimeStamp'])/1000)
                end_time = datetime.datetime.fromtimestamp(float(seg_data[-1]['TimeStamp'])/1000)
                time_diff_sec = (end_time - start_time).total_seconds()
                time_diff_secs_edge.append(time_diff_sec)
            else:
                seg_csv = open(seg_csv_name, 'rb')
                seg_reader = csv.DictReader(seg_csv,delimiter=',')

                for row in seg_reader:
                    seg_data.append(row)

                print(seg_id + ' stop')
                start_time = datetime.datetime.fromtimestamp(float(seg_data[1]['TimeStamp'])/1000)
                end_time = datetime.datetime.fromtimestamp(float(seg_data[-1]['TimeStamp'])/1000)
                time_diff_sec = (end_time - start_time).total_seconds()
                time_diff_secs_stop.append(time_diff_sec)            

        #write every edge
        if len(time_diff_secs_edge) != 0:
            time_diff_mean = reduce(lambda x, y: x + y, time_diff_secs_edge) / len(time_diff_secs_edge)
            graph_data_time_edge_csv.writerow(dict(ID_FROM=id_from,
                                              ID_TO=id_to,
                                              TIME=round(time_diff_mean,3)))
            time_diff_secs_edge_pair.append(time_diff_mean)

        if len(time_diff_secs_stop) != 0:
            time_diff_mean = reduce(lambda x, y: x + y, time_diff_secs_stop) / len(time_diff_secs_stop)
            graph_data_time_stop_csv.writerow(dict(ID_FROM=id_from,
                                              ID_TO=id_to,
                                              TIME=round(time_diff_mean,3)))

    if len(time_diff_secs_edge_pair) == 1:
        time_diff_mean = time_diff_secs_edge_pair[0]
        graph_data_time_edge_graph_csv.writerow(dict(ID_FROM=graph_entry['id_from'],
                                              ID_TO=graph_entry['id_to'],
                                              NAME_FROM=graph_entry['name_from'],
                                              NAME_TO=graph_entry['name_to'],
                                              COST=int(round(time_diff_mean,0))))
    elif len(time_diff_secs_edge_pair) == 2:
        time_diff_mean = (time_diff_secs_edge_pair[0] + time_diff_secs_edge_pair[1])/2
        time_diff = abs(time_diff_secs_edge_pair[0] - time_diff_secs_edge_pair[1])
        graph_data_time_edge_graph_csv.writerow(dict(ID_FROM=graph_entry['id_from'],
                                              ID_TO=graph_entry['id_to'],
                                              NAME_FROM=graph_entry['name_from'],
                                              NAME_TO=graph_entry['name_to'],
                                              COST=int(round(time_diff_mean,0)))) #TIME_DIF=round(time_diff,3)))
    else:
        graph_data_time_edge_graph_csv.writerow(dict(ID_FROM=graph_entry['id_from'],
                                              ID_TO=graph_entry['id_to'],
                                              NAME_FROM=graph_entry['name_from'],
                                              NAME_TO=graph_entry['name_to'],
                                              COST=graph_entry['cost'])) #TIME_DIF=round(time_diff,3)))

f_time_edge.close()
f_time_stop.close()
f_time_edge_graph.close()