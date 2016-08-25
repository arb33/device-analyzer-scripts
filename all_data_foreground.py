#!/usr/bin/env python
#
# Copyright 2016 Kelly Widdicks, Alastair R. Beresford
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import gzip
import sys
import os
import csv
import io
import glob
import numpy as np
from collections import namedtuple
import dateutil.parser
from datetime import datetime, timedelta

global foreground_use
global data_rx
global data_tx

fields_da = ('Entry','Num','Date','EntryType','Value')
DARecord = namedtuple('DARecord', fields_da)
def read_file(path):
    try:
        with io.BufferedReader(gzip.open(path, 'rU')) as data:
            for line in data:
                #Repack variable number of items per line into five expected items
                #(Problem is internal DA format uses ';' to separate csv items as well
                # as to separate app names inside the 'Value' field.)
                e = line.split(';')
                value = reduce(lambda x, y: x + ',' + y, e[4:])
                repacked = e[0:4] + [value]
                yield apply(DARecord._make, (repacked,))
    except:
        print('Failed to read file: ' + path)

fields_filename = ('i', 'FileName', 'Start', 'End', 'Days', 'PropData', 'InUK', 'OutUK', 'PropUK')
FileNameRecord = namedtuple('FileNameRecord', fields_filename)
def read_file_names(path):
    with open(path, 'rU') as data:
        csv.field_size_limit(sys.maxsize)
        reader = csv.reader(data, delimiter=' ')
        for row in map(FileNameRecord._make, reader):
            yield row

fields_lancs = ('Entry','Num','Date','EntryType','Value')
DARecordLancs = namedtuple('DARecordLancs', fields_lancs)
def read_file_lancs(path):
    try:
        with open(path, 'rU') as data:
            csv.field_size_limit(sys.maxsize)
            reader = csv.reader(data, delimiter=';')
            for row in map(DARecord._make, reader):
                yield row
    except:
        print('Failed to read file: ' + path)

FileNameRecordLancs = namedtuple('FileNameRecordLancs', ('FileName'))
def read_file_names_lancs(path):
    with open(path, 'rU') as data:
        csv.field_size_limit(sys.maxsize)
        reader = csv.reader(data, delimiter='\n')
        for row in map(FileNameRecordLancs._make, reader):
            yield row

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        print('Output path exists')

def parse_file(file, lancs):
    global foreground_use
    global data_rx
    global data_tx

    logs_to_parse = ['app', 'screen', 'hf', 'net']

    current_hour = None
    current_day = None
    no_of_days = 0

    app_foreground_use = np.zeros(24)
    screen_on = False
    screen_unlocked = False
    last_importance_app_pid = None

    last_app_data = {}
    all_data_rx = [[] for hour in range(0,24)]
    all_data_tx = [[] for hour in range(0,24)]

    for row in (read_file_lancs(file) if lancs else read_file(file)):
        row_entry_type = row.EntryType
        entry_val = row_entry_type.split('|')
        row_date = row.Date
        date_time = row_date.rsplit('T')
        row_value = row.Value

        if entry_val[0] not in logs_to_parse or row_date == '(invalid date)':
            continue

        if current_day != date_time[0]:
            current_day = date_time[0]
            no_of_days+=1

        current_hour = int(date_time[1].split(':')[0])

        # An app is in the foreground so log its process id
        if 'importance' in entry_val and row_value == 'foreground':
            last_importance_app_pid = entry_val[1]
        # Get the name of the app currently in the foreground
        elif 'app' in entry_val and 'name' in entry_val and last_importance_app_pid != None:
            # The app pids for the app importance and app name logs don't match, so ignore it
            if entry_val[1] == last_importance_app_pid:
                # The user is using the device
                if screen_on and screen_unlocked:
                    # Increment the number of times it was in the foreground
                    app_foreground_use[current_hour]+=1
            last_importance_app_pid = None
        # Screen locked/unlocked
        elif row_entry_type.startswith('hf|locked'):
            if row_value == 'true':
                screen_unlocked = False
            else:
                screen_unlocked = True
        # Screen on/off
        elif row_entry_type.startswith('screen|power'):
            if 'off' in row_value:
                screen_on = False
            else:
                screen_on = True
        # App data
        if row_entry_type.startswith('net|app'):
            app_id = entry_val[2]
            if app_id not in last_app_data:
                last_app_data[app_id] = [None, None]
            if entry_val[3] == 'rx_bytes':
                app_last_rx = last_app_data[app_id][0]
                if app_last_rx == None:
                    pass
                elif int(row_value) > app_last_rx:
                    all_data_rx[current_hour].append(int(row_value) - app_last_rx)
                elif int(row_value) < app_last_rx:
                    all_data_rx[current_hour].append(int(row_value))
                last_app_data[app_id][0] = int(row_value)
            elif entry_val[3] == 'tx_bytes':
                app_last_tx = last_app_data[app_id][1]
                if app_last_tx == None:
                    pass
                elif int(row_value) > app_last_tx:
                    all_data_tx[current_hour].append(int(row_value) - app_last_tx)
                elif int(row_value) < app_last_tx:
                    all_data_tx[current_hour].append(int(row_value))
                last_app_data[app_id][1] = int(row_value)


    if no_of_days != 0:

        mean_app_foreground_use = [(ihour/no_of_days) for ihour in app_foreground_use]
        if not all(i == 0 for i in mean_app_foreground_use):
            [foreground_use[i].append(mean_app_foreground_use[i]) for i in range(0,24)]

        mean_rx = [(sum(ihour)/no_of_days) for ihour in all_data_rx]
        mean_tx = [(sum(ihour)/no_of_days) for ihour in all_data_tx]
        if not all(i == 0 for i in mean_rx):
            [data_rx[i].append(mean_rx[i]) for i in range(0,24)]
        if not all(i == 0 for i in mean_tx):
            [data_tx[i].append(mean_tx[i]) for i in range(0,24)]

def calculate_print_summaries():
    global foreground_use
    global data_rx
    global data_tx

    foreground_total = [0 if not hour else sum(hour) for hour in foreground_use]
    data_rx_total = [0 if not hour else sum(hour) for hour in data_rx]
    data_tx_total = [0 if not hour else sum(hour) for hour in data_tx]

    foreground_mean = [0 if not hour else np.mean(hour) for hour in foreground_use]
    data_rx_mean = [0 if not hour else np.mean(hour) for hour in data_rx]
    data_tx_mean = [0 if not hour else np.mean(hour) for hour in data_tx]

    foreground_no_of_devices = [0 if not hour else len(hour) for hour in foreground_use]
    data_rx_no_of_devices = [0 if not hour else len(hour) for hour in data_rx]
    data_tx_no_of_devices = [0 if not hour else len(hour) for hour in data_tx]

    foreground_min = [0 if not hour else np.min(hour) for hour in foreground_use]
    data_rx_min = [0 if not hour else np.min(hour) for hour in data_rx]
    data_tx_min = [0 if not hour else np.min(hour) for hour in data_tx]

    foreground_max = [0 if not hour else np.max(hour) for hour in foreground_use]
    data_rx_max = [0 if not hour else np.max(hour) for hour in data_rx]
    data_tx_max = [0 if not hour else np.max(hour) for hour in data_tx]

    foreground_med = [0 if not hour else np.median(hour) for hour in foreground_use]
    data_rx_med = [0 if not hour else np.median(hour) for hour in data_rx]
    data_tx_med = [0 if not hour else np.median(hour) for hour in data_tx]

    with open('total_out/all_totals_hourly.csv', 'a') as f:
        f.write('{0};{1}\n'.format('foreground total', foreground_total))
        f.write('{0};{1}\n'.format('data rx total', data_rx_total))
        f.write('{0};{1}\n'.format('data tx total', data_tx_total))
    with open('total_out/all_means_hourly.csv', 'a') as f:
        f.write('{0};{1}\n'.format('foreground mean', foreground_mean))
        f.write('{0};{1}\n'.format('data rx mean', data_rx_mean))
        f.write('{0};{1}\n'.format('data tx mean', data_tx_mean))
    with open('total_out/all_deviceNo_hourly.csv', 'a') as f:
        f.write('{0};{1}\n'.format('foreground no of devices', foreground_no_of_devices))
        f.write('{0};{1}\n'.format('data rx no of devices', data_rx_no_of_devices))
        f.write('{0};{1}\n'.format('data tx no of devices', data_tx_no_of_devices))
    with open('total_out/all_mins_hourly.csv', 'a') as f:
        f.write('{0};{1}\n'.format('foreground min', foreground_min))
        f.write('{0};{1}\n'.format('data rx min', data_rx_min))
        f.write('{0};{1}\n'.format('data tx min', data_tx_min))
    with open('total_out/all_maxs_hourly.csv', 'a') as f:
        f.write('{0};{1}\n'.format('foreground max', foreground_max))
        f.write('{0};{1}\n'.format('data rx max', data_rx_max))
        f.write('{0};{1}\n'.format('data tx max', data_tx_max))
    with open('total_out/all_meds_hourly.csv', 'a') as f:
        f.write('{0};{1}\n'.format('foreground med', foreground_med))
        f.write('{0};{1}\n'.format('data rx med', data_rx_med))
        f.write('{0};{1}\n'.format('data tx med', data_tx_med))
    with open('total_out/all_hourly.csv', 'a') as f:
        f.write('{0};{1};{2};{3};{4};{5};{6}\n'.format('foreground use', foreground_total, foreground_mean, foreground_no_of_devices, foreground_min, foreground_max, foreground_med))
        f.write('{0};{1};{2};{3};{4};{5};{6}\n'.format('data rx', data_rx_total, data_rx_mean, data_rx_no_of_devices, data_rx_min, data_rx_max, data_rx_med))
        f.write('{0};{1};{2};{3};{4};{5};{6}\n'.format('data tx', data_tx_total, data_tx_mean, data_tx_no_of_devices, data_tx_min, data_tx_max, data_tx_med))


if __name__ == '__main__':
    global foreground_use
    global data_rx
    global data_tx

    pathOfIdsFile = sys.argv[1]
    pathOfFiles = sys.argv[2]
    lancs = bool(len(sys.argv) > 3)

    startTime = datetime.now()

    foreground_use = [[] for x in range(0,24)]
    data_rx = [[] for x in range(0,24)]
    data_tx = [[] for x in range(0,24)]

    # Make sure 'out/' folder exists and reset/create output files
    make_sure_path_exists('total_out/')
    output_files = ['all_totals_hourly.csv', 'all_means_hourly.csv', 'all_deviceNo_hourly.csv', 'all_mins_hourly.csv', 'all_maxs_hourly.csv', 'all_meds_hourly.csv', 'all_hourly.csv']
    for of_name in output_files:
        with open('total_out/' + of_name, 'w') as f:
            f.write('')

    for file in (read_file_names_lancs(pathOfIdsFile) if lancs else read_file_names(pathOfIdsFile)):
        fname = file.FileName
        fullfpath = pathOfFiles + file.FileName + '.csv'
        if not lancs:
            fullfpath = fullfpath + '.gz'
        print("Parsing file: " + fname)
        parse_file(fullfpath, lancs)

    calculate_print_summaries()

    # **** For checking timings *****
    endFilesTime = datetime.now()
    print("All files summarised in {0}".format(str((endFilesTime - startTime))))
