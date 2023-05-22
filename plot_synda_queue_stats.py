#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plotting code copied from @durack1's code at https://github.com/durack1/ESGFReports
"""

import os
import json
import datetime
import argparse
import numpy
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

timeNow = datetime.datetime.now()
timeFormat = timeNow.strftime('%y%m%d')

def human_read_to_bytes(size):
    size_name = ("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    size = size.split()                
    num, unit = float(size[0]), size[1] 
    if unit in ["Byte", "Bytes"]:
        unit = "B"
    idx = size_name.index(unit)  
    factor = 1000 ** idx               
    return int(num * factor)

def gen_plot(data_file, status, start_date, end_date, ymin=None, ymax=None, output_dir=None):

    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    with open(data_file, 'r') as f:
        synda_queue_data = json.load(f)

    date_format = '%Y-%m-%dT%H:%M:%SZ'
    datetimes = []
    data_footprint = []
    for timestamp, statuses in synda_queue_data.items():
        if status in statuses:
            dt = datetime.datetime.strptime(timestamp, date_format)
            df = human_read_to_bytes(statuses[status]['size'])
            datetimes.append(dt)
            data_footprint.append(df)
    
    filename = "{}_{}-{}".format(status, start_str, end_str)

    filename = '_'.join([timeFormat, filename])  # Append date prefix
    filename = os.path.join(output_dir, filename)

    # plot data
    plot_filename = filename+".png"
    print("Saving plot to {}".format(plot_filename))

    fig, ax = plt.subplots(figsize=(10, 5))

    # convert footprint to terabytes
    data_footprint = [df/(10**12) for df in data_footprint]

    @ticker.FuncFormatter
    def major_formatter(x, pos):
        return "%.2f TB" % x

    ax.plot(datetimes, data_footprint)

    ylim_min = ymin if ymin else numpy.min(data_footprint)
    ylim_max = ymax if ymax else numpy.max(data_footprint)
    ax.set(xlim=(start_date, end_date), ylim=(ylim_min, ylim_max))

    title = "'{}' data on Synda".format(status)

    ax.set(xlabel='date', ylabel='data footprint', title=title)
    ax.yaxis.set_major_formatter(major_formatter)
    ax.grid()

    fig.savefig(plot_filename)


def gen_stacked_area_chart(data_file, start_date, end_date, ymin=None, ymax=None, output_dir=None):

    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    with open(data_file, 'r') as f:
        synda_queue_data = json.load(f)

    status_list = ['published', 'done', 'retracted', 'error', 'waiting', 'running', 'miscellaneous']

    date_format = '%Y-%m-%dT%H:%M:%SZ'
    datetimes = []
    data_footprint = { s:[] for s in status_list }
    for timestamp, statuses in synda_queue_data.items():
        dt = datetime.datetime.strptime(timestamp, date_format)
        datetimes.append(dt)
        for status in status_list:
            if status in statuses:
                df = human_read_to_bytes(statuses[status]['size'])
                df /= (10**12)
                data_footprint[status].append(df)
            else:
                data_footprint[status].append(0)

    filename = "synda_data_footprint_{}-{}".format(start_str, end_str)

    filename = '_'.join([timeFormat, filename])  # Append date prefix
    filename = os.path.join(output_dir, filename)

    # plot data
    plot_filename = filename+".png"
    print("Saving plot to {}".format(plot_filename))

    fig, ax = plt.subplots(figsize=(10, 5))

    @ticker.FuncFormatter
    def major_formatter(x, pos):
        return "%.2f TB" % x

    labels = []
    data_footprint_list = []
    for k, df in data_footprint.items():
        labels.append(k)
        data_footprint_list.append(df)

    ax.stackplot(datetimes, data_footprint_list, labels=labels)
    ax.legend(loc='upper left')

    ylim_min = ymin if ymin else numpy.min(data_footprint_list)
    ylim_max = ymax if ymax else numpy.sum(data_footprint_list, axis=0).max()
    ax.set(xlim=(start_date, end_date), ylim=(ylim_min, ylim_max))

    title = "Dataset file statuses on Synda".format(status)

    ax.set(xlabel='date', ylabel='data footprint', title=title)
    ax.yaxis.set_major_formatter(major_formatter)
    ax.grid()

    fig.savefig(plot_filename)

def main():

    parser = argparse.ArgumentParser(
        description="Gather data footprint per day from ESGF")
    parser.add_argument("--file", "-f", dest="data_file", type=str,
                        default=None, help="JSON file containing file counts and storage sizes along time for each status")
    parser.add_argument("--stacked_chart", "-sc", action="store_true",
                        help="Create a stacked area chart of all statuses in the JSON file (default is false)" )
    parser.add_argument("--status", "-s", dest="status", type=str,
                        default="published", help="Synda status to plot (default is 'published')")
    parser.add_argument("--start_date", "-sd", dest="start_date", type=str,
                        default=None, help="Start date in YYYY-MM-DD format (default is None)")
    parser.add_argument("--end_date", "-ed", dest="end_date", type=str,
                        default=None, help="End date in YYYY-MM-DD format (default is None)")
    parser.add_argument("--output", "-o", dest="output", type=str,
                        default=os.path.curdir, help="Output directory (default is current directory)")
    parser.add_argument("--ymax", dest="ymax", type=int, default=None,
                        help="Maximum of y-axis for data footprint plot (default is None)")
    parser.add_argument("--ymin", dest="ymin", type=int, default=None,
                        help="Minimum of y-axis for data footprint plot (default is None)")
    args = parser.parse_args()

    if args.data_file is None:
        print("You must input a JSON file produced by process_report_log.py")
        return
    elif not os.path.isfile(args.data_file):
        print("{} is not a file".format(args.data_file))
        return

    if args.start_date is None:
        print("You must enter a start date.")
        return
    else:
        try:
            start_date = datetime.datetime.strptime(
                args.start_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError(
                "Incorrect start date format, should be YYYY-MM-DD")
            return

    if args.end_date is None:
        print("You must enter an end date.")
        return
    else:
        try:
            end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Incorrect end date format, should be YYYY-MM-DD")
            return

    if not os.path.isdir(args.output):
        print("{} is not a directory. Exiting.".format(args.output))
        return

    if args.stacked_chart:
        gen_stacked_area_chart(args.data_file, start_date, end_date, args.ymin, args.ymax, args.output)
    else:
        gen_plot(args.data_file, args.status, start_date, end_date, args.ymin, args.ymax, args.output)


if __name__ == '__main__':
    main()
