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

global apps_practices

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
    except Exception as ex:
        print(ex)
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
    except Exception as ex:
        print(ex)
        print('Failed to read file: ' + path)

FileNameRecordLancs = namedtuple('FileNameRecordLancs', ('FileName'))
def read_file_names_lancs(path):
    with open(path, 'rU') as data:
        csv.field_size_limit(sys.maxsize)
        reader = csv.reader(data, delimiter='\n')
        for row in map(FileNameRecordLancs._make, reader):
            yield row

AppRecord = namedtuple('AppRecord', ('FullName', 'Name', 'Practice'))
def read_app_mapping(path):
    with open(path, 'rU') as data:
        csv.field_size_limit(sys.maxsize)
        reader = csv.reader(data, delimiter=';')
        for row in map(AppRecord._make, reader):
            yield row

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        print('Output path exists')

def get_t_gap(first, second):
    return (dateutil.parser.parse(second) - dateutil.parser.parse(first)).total_seconds()

def parse_file(file, lancs):
    global apps_practices
    global sms_sent_hourly
    global sms_sent_total_hourly
    global sms_received_hourly
    global sms_received_total_hourly
    global mean_phone_call_durations_hourly
    global mean_no_of_phone_calls_hourly

    logs_to_parse = ['net','app']

    current_hour = None
    current_day = None
    no_of_days = 0

    ids_names = {}
    current_app_name_id_mapping = {}
    app_data = {}

    current_hour = None

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

        # APP DATA
        if row_entry_type.startswith('net|app'):
            app_id = entry_val[2]
            app_name = None
            for key, val in current_app_name_id_mapping.items():
                if val == app_id:
                    app_name = key
            if app_name == None:
                continue

            if entry_val[3] == 'rx_bytes':
                app_last_rx = app_data[app_name][0]
                if app_last_rx == None:
                    pass
                elif int(row_value) > app_last_rx:
                    app_data[app_name][1][current_hour].append(int(row_value) - app_last_rx)
                elif int(row_value) < app_last_rx:
                    app_data[app_name][1][current_hour].append(int(row_value))
                app_data[app_name][0] = int(row_value)
            elif entry_val[3] == 'tx_bytes':
                app_last_tx = app_data[app_name][2]
                if app_last_tx == None:
                    pass
                elif int(row_value) > app_last_tx:
                    app_data[app_name][3][current_hour].append(int(row_value) - app_last_tx)
                elif int(row_value) < app_last_tx:
                    app_data[app_name][3][current_hour].append(int(row_value))
                app_data[app_name][2] = int(row_value)
        # APP NAMES
        elif row_entry_type.startswith('app|installed'):
            for app_entry in row_value.split(','):
                temp_name =  app_entry.split('@')[0]
                if temp_name in apps_practices:
                    app_info = app_entry.split('@')[1].split(':')
                    temp_app_id = app_info[len(app_info) - 2]
                    if temp_name not in current_app_name_id_mapping:
                        app_data[temp_name] = [None, [[] for x in range(0,24)], None, [[] for x in range(0,24)]]

                    # Remove old mapping if it exists
                    if temp_app_id not in ids_names:
                        ids_names[temp_app_id] = temp_name
                    elif ids_names[temp_app_id] != temp_name:
                        for key, val in current_app_name_id_mapping.items():
                            if val == temp_app_id and key != temp_name:
                                current_app_name_id_mapping[key] = ''
                        ids_names[temp_app_id] = temp_name

                    current_app_name_id_mapping[temp_name] = temp_app_id


    if no_of_days != 0:
        # Append this device's hourly app data to overall data
        for app, data in app_data.items():
            # Calculate hourly means for the device app
            mean_rx = [(sum(ihour)/no_of_days) for ihour in data[1]]
            mean_tx = [(sum(ihour)/no_of_days) for ihour in data[3]]
            if not all(i == 0 for i in mean_rx):
                [apps_practices[app][2][i].append(mean_rx[i]) for i in range(0,24)]
            if not all(i == 0 for i in mean_tx):
                [apps_practices[app][3][i].append(mean_tx[i]) for i in range(0,24)]

def calculate_print_app_practice_summaries():
    global apps_practices

    practices = {}

    # APP SUMMARY
    for app, data in apps_practices.items():
        # Initialise the practice in the dictionary if it doesn't already exist
        app_name = data[0]
        practice_name = data[1]
        if practice_name not in practices:
            practices[data[1]]  = [[] for i in range(0,24)], [[] for i in range(0,24)]

        # Calculate the overall hourly totals, means, no of devices, mins, maxs and medians for the apps across devices for rx
        mean_rx = [0 if not hour else np.mean(hour) for hour in data[2]]

        # Calculate the overall hourly totals, means, no of devices, mins, maxs and medians for the apps across devices for tx
        mean_tx = [0 if not hour else np.mean(hour) for hour in data[3]]

        # Append app hourly means to the overall practices means
        for i in range(0,24):
            practices[practice_name][0][i].append(mean_rx[i])
            practices[practice_name][1][i].append(mean_tx[i])

    # PRACTICE SUMMARY
    for practice, data in practices.items():
        # Calculate the overall hourly total of means for the apps across devices for rx and tx
        total_rx = [0 if not hour else sum(hour) for hour in data[0]]
        total_tx = [0 if not hour else sum(hour) for hour in data[1]]
        no_of_apps = len(data[0][0])

        # Write practice summaries to files
        with open('out/practice_hourly_summaries.csv', 'a') as f:
            f.write('{0};{1};{2}\n{3};{4};{5}\n'.format(practice, no_of_apps, 'rx_bytes;{0}'.format(total_rx), practice, no_of_apps, 'tx_bytes;{0}'.format(total_tx)))


if __name__ == '__main__':
    global apps_practices

    pathOfIdsFile = sys.argv[1]
    pathOfFiles = sys.argv[2]
    pathOfAppMappingFile = sys.argv[3]
    lancs = bool(len(sys.argv) > 4)

    startTime = datetime.now()

    apps_practices = {}
    for app in read_app_mapping(pathOfAppMappingFile):
        apps_practices[app.FullName] = (app.Name, app.Practice, [[] for x in range(0,24)], [[] for x in range(0,24)])

    # Make sure 'out/' folder exists and reset/create output files
    make_sure_path_exists('out/')
    output_files = ['practice_hourly_summaries.csv']
    for of_name in output_files:
        with open('out/' + of_name, 'w') as f:
            f.write('')

    for file in (read_file_names_lancs(pathOfIdsFile) if lancs else read_file_names(pathOfIdsFile)):
        fname = file.FileName
        fullfpath = pathOfFiles + file.FileName + '.csv'
        if not lancs:
            fullfpath = fullfpath + '.gz'
        print("Parsing file: " + fname)
        parse_file(fullfpath, lancs)

    calculate_print_app_practice_summaries()

    # **** For checking timings *****
    endFilesTime = datetime.now()
    print("All files summarised in {0}".format(str((endFilesTime - startTime))))
