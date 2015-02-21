import os
import csv
import datetime
import glob

#ugly, I know)
cell_data_loc = '/media/sim/Windows7_OS/work/metrocell/data/proc/msk/cell'
graph_loc = '/home/sim/work/metro4all/repo/data/msk/graph.csv'

#resulting csv with timings on edges
f_time_edge = open('graph_time_edge.csv','wb')
f_time_fields = ('ID_FROM','ID_TO','TIME')
f_time_edge.write(','.join(f_time_edge_fields))
f_time_edge.write('\n')
graph_data_time_edge_csv = csv.DictWriter(f_time_edge, fieldnames=f_time_fields)

#resulting csv with timings on stops
f_time_stop = open('graph_time_stop.csv','wb')
f_time_stop.write(','.join(f_time_stop_fields))
f_time_stop.write('\n')
graph_data_time_stop_csv = csv.DictWriter(f_time_stop, fieldnames=f_time_fields)

graph_data_time_edge = []
graph_data_time_stop = []
graph_data = []

graph_fieldnames=['id_from','id_to','name_from','name_to','cost']
graph_csv = open(graph_loc, 'rb')
graph_reader = csv.DictReader(graph_csv,delimiter=';')

for row in graph_reader:
    graph_data.append(row)

for graph_entry in graph_data:
    seg_id = graph_entry['id_from'].zfill(3) + '-' + graph_entry['id_to'].zfill(3)

    os.chdir(cell_data_loc)
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

            start_time = datetime.datetime.fromtimestamp(float(seg_data[1]['TimeStamp'])/1000)
            end_time = datetime.datetime.fromtimestamp(float(seg_data[-1]['TimeStamp'])/1000)
            time_diff_sec = (end_time - start_time).total_seconds()
            time_diff_secs_edge.append(time_diff_sec)
        else:
            seg_csv = open(seg_csv_name, 'rb')
            seg_reader = csv.DictReader(seg_csv,delimiter=',')

            for row in seg_reader:
                seg_data.append(row)

            start_time = datetime.datetime.fromtimestamp(float(seg_data[1]['TimeStamp'])/1000)
            end_time = datetime.datetime.fromtimestamp(float(seg_data[-1]['TimeStamp'])/1000)
            time_diff_sec = (end_time - start_time).total_seconds()
            time_diff_secs_stop.append(time_diff_sec)            

    if len(time_diff_secs_edge) != 0:
        time_diff_mean = reduce(lambda x, y: x + y, time_diff_secs_edge) / len(time_diff_secs_edge)
        graph_data_time_edge_csv.writerow(dict(ID_FROM=graph_entry['id_from'],
                                          ID_TO=graph_entry['id_to'],
                                          TIME=round(time_diff_mean,3)))

    if len(time_diff_secs_stop) != 0:
        time_diff_mean = reduce(lambda x, y: x + y, time_diff_secs_stop) / len(time_diff_secs_stop)
        graph_data_time_stop_csv.writerow(dict(ID_FROM=graph_entry['id_from'],
                                          ID_TO=graph_entry['id_to'],
                                          TIME=round(time_diff_mean,3)))

f_time_edge.close()
f_time_stop.close()