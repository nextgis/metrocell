# coding=utf-8
import copy
from datetime import datetime

__author__ = 'yellow'


class SimpleTimeStrategy():
    """
    Class for georeferencing log entries to 'real' metro lines
    with simple linear time strategy
    """

    @staticmethod
    def georeferencing(line_id, line_log_entries, point_interpolator):
        """
        Georeferencing log rows !for one segment! on any layer using interpolator
        :param line_id: id of segment
        :param line_log_entries: log rows for segment
        :param point_interpolator: interpolator for any layer
        :return:
        """
        # get start time, finish time and total time for segment
        start_time_ts = float(line_log_entries[0]['TimeStamp'])/1e3
        start_time = datetime.fromtimestamp(start_time_ts)

        finish_time_ts = float(line_log_entries[-1]['TimeStamp'])/1e3
        finish_time = datetime.fromtimestamp(finish_time_ts)

        segment_time_period = finish_time - start_time


        new_entries = []
        # interpolate log entries
        for log_entry in line_log_entries:
            log_entry_ts = float(log_entry['TimeStamp'])/1e3
            log_entry_dt = datetime.fromtimestamp(log_entry_ts)

            start_time_delta = log_entry_dt - start_time

            time_ratio = start_time_delta.total_seconds()/segment_time_period.total_seconds()

            log_entry_point = point_interpolator.interpolate_by_ratio(line_id, time_ratio)

            new_entry = copy.copy(log_entry)
            new_entry['x'] = log_entry_point.x
            new_entry['y'] = log_entry_point.y

            new_entries.append(new_entry)

        return new_entries