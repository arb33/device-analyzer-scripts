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
from collections import namedtuple, OrderedDict
import dateutil.parser
from datetime import datetime, timedelta

global apps_rx
global apps_tx
global foreground_use
global app_practice_mapping
global p_practice_demand_contribution
global p_practice_use_contribution
global all_use_contribution
global all_demand_contribution
global contribution

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

def get_practice_name(app):
    global app_practice_mapping

    if app not in app_practice_mapping:
        return None
    else:
        return app_practice_mapping[app]

def parse_file(file, lancs, fname):
    global apps_rx
    global apps_tx
    global foreground_use
    global p_practice_demand_contribution
    global p_practice_use_contribution
    global all_use_contribution
    global all_demand_contribution
    global contribution

    logs_to_parse = ['app', 'screen', 'hf', 'net']

    current_hour = None
    current_day = None
    no_of_days = 0

    app_foreground_use = {}

    screen_on = False
    screen_unlocked = False
    last_importance_app_pid = None

    last_app_data = {}
    all_data_rx = [[] for hour in range(0,24)]
    all_data_tx = [[] for hour in range(0,24)]

    ids_names = {}
    current_app_name_id_mapping = {}
    app_data = {}

    for row in (read_file_lancs(file) if lancs else read_file(file)):
        row_entry_type = row.EntryType
        entry_val = row_entry_type.split('|')
        row_date = row.Date
        date_time = row_date.rsplit('T')
        row_value = row.Value.strip()

        if entry_val[0] not in logs_to_parse or row_date == '(invalid date)':
            continue

        if current_day != date_time[0]:
            current_day = date_time[0]
            no_of_days+=1

        current_hour = int(date_time[1].split(':')[0])

        # An app is in the foreground so log its process id
        if 'importance' in entry_val and 'foreground' in row_value:
            last_importance_app_pid = entry_val[1]
        # Get the name of the app currently in the foreground
        elif 'app' in entry_val and 'name' in entry_val and last_importance_app_pid != None:

            #row_value contains "<app name>:<play store group>" so retrieve just app name:
            app_name = row_value.split(":")[0]

            # The app pids for the app importance and app name logs don't match, so ignore it
            if entry_val[1] == last_importance_app_pid:
                # The user is using the device
                if screen_on and screen_unlocked:
                    # Increment the number of times it was in the foreground
                    if app_name not in app_foreground_use:
                        app_foreground_use[app_name] = [0 for x in range(0,24)]
                    app_foreground_use[app_name][current_hour]+=1

            last_importance_app_pid = None
        # Screen locked/unlocked
        elif row_entry_type.startswith('hf|locked'):
            if 'true' in row_value:
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
        elif row_entry_type.startswith('net|app'):
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
        # App installed logs
        elif row_entry_type.startswith('app|installed'):
            for app_entry in row_value.split(','):
                installed_details = app_entry.split('@')
                if len(installed_details) > 1:
                    temp_name = installed_details[0]
                    app_info = installed_details[1].split(':')
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
        for app, data in app_foreground_use.items():
            # Calculate hourly means for the device app foregound instances
            mean_app_foreground_use = [(no_of_foreground_instances/no_of_days) for no_of_foreground_instances in data]
            if not all(i == 0 for i in mean_app_foreground_use):
                if app not in foreground_use:
                    foreground_use[app] = [[] for x in range(0,24)]
                [foreground_use[app][i].append(mean_app_foreground_use[i]) for i in range(0,24)]
                # Add user to practice use contribution
                practice = get_practice_name(app)
                if practice != None:
                    p_practice_use_contribution[practice].add(fname)
                all_use_contribution.add(fname)
                contribution.add(fname)

        for app, data in app_data.items():
            mean_rx = [(sum(ihour)/no_of_days) for ihour in data[1]]
            mean_tx = [(sum(ihour)/no_of_days) for ihour in data[3]]
            if not all(i == 0 for i in mean_rx):
                if app not in apps_rx:
                    apps_rx[app] = [[] for i in range(0,24)]
                [apps_rx[app][i].append(mean_rx[i]) for i in range(0,24)]
            if not all(i == 0 for i in mean_tx):
                if app not in apps_tx:
                    apps_tx[app] = [[] for i in range(0,24)]
                [apps_tx[app][i].append(mean_tx[i]) for i in range(0,24)]
            # Add user to practice demand contribution
            if not all(i == 0 for i in mean_rx + mean_tx):
                practice = get_practice_name(app)
                if practice != None:
                    p_practice_demand_contribution[practice].add(fname)
                all_demand_contribution.add(fname)
                contribution.add(fname)

def calculate_print_summaries():
    global apps_rx
    global apps_tx
    global foreground_use
    global p_practice_demand_contribution
    global p_practice_use_contribution
    global all_use_contribution
    global all_demand_contribution
    global contribution

    with open('overall_summary/contribution.csv', 'w') as f:
        f.write('use,{0}\n'.format(len(all_use_contribution)))
        f.write('demand,{0}\n'.format(len(all_demand_contribution)))
        f.write('total no of devices,{0}\n'.format(len(contribution)))

    with open('overall_summary/practice_demand_contribution.csv', 'w') as f:
        f.write('Category, no of devices\n')

    with open('overall_summary/practice_use_contribution.csv', 'w') as f:
        f.write('Category, no of devices\n')

    for practice, devices in p_practice_demand_contribution.items():
        with open('overall_summary/practice_demand_contribution.csv', 'a') as f:
            f.write('"{0}",{1}\n'.format(practice, len(devices)))

    for practice, devices in p_practice_use_contribution.items():
        with open('overall_summary/practice_use_contribution.csv', 'a') as f:
            f.write('"{0}",{1}\n'.format(practice, len(devices)))

    foreground_total = [[] for i in range(0,24)]
    practices_foreground = OrderedDict()

    for app, data in foreground_use.items():
        sum_use = [0 if not hour else sum(hour) for hour in data]
        [foreground_total[i].append(sum_use[i]) for i in range(0,24)]
        practice = get_practice_name(app)
        if practice != None:
            if practice not in practices_foreground:
                practices_foreground[practice] = [[] for i in range(0,24)]
            [practices_foreground[practice][i].append(sum_use[i]) for i in range(0,24)]

    data_rx_total = [[] for i in range(0,24)]
    data_tx_total = [[] for i in range(0,24)]
    data_all_total = [[] for i in range(0,24)]
    practices_rx = OrderedDict()
    practices_tx = OrderedDict()
    practices_data = OrderedDict()

    for app, data in apps_rx.items():
        sum_rx = [0 if not hour else sum(hour) for hour in data]
        [data_rx_total[i].append(sum_rx[i]) for i in range(0,24)]
        [data_all_total[i].append(sum_rx[i]) for i in range(0,24)]
        practice = get_practice_name(app)
        if practice != None:
            if practice not in practices_rx:
                practices_rx[practice] = [[] for i in range(0,24)]
            [practices_rx[practice][i].append(sum_rx[i]) for i in range(0,24)]
            if practice not in practices_data:
                practices_data[practice] = [[] for i in range(0,24)]
            [practices_data[practice][i].append(sum_rx[i]) for i in range(0,24)]

    for app, data in apps_tx.items():
        sum_tx = [0 if not hour else sum(hour) for hour in data]
        [data_tx_total[i].append(sum_tx[i]) for i in range(0,24)]
        [data_all_total[i].append(sum_tx[i]) for i in range(0,24)]
        practice = get_practice_name(app)
        if practice != None:
            if practice not in practices_tx:
                practices_tx[practice] = [[] for i in range(0,24)]
            [practices_tx[practice][i].append(sum_tx[i]) for i in range(0,24)]
            if practice not in practices_data:
                practices_data[practice] = [[] for i in range(0,24)]
            [practices_data[practice][i].append(sum_tx[i]) for i in range(0,24)]

    overall_use_from_categories = 0
    overall_demand_from_categories = 0
    overall_use = 0
    overall_demand = 0

    with open('overall_summary/all_totals.csv', 'a') as f:
        foreground_all = [0 if not hour else sum(hour) for hour in foreground_total]
        overall_use = overall_use + sum(foreground_all)

        rx_all = [0 if not hour else sum(hour) for hour in data_rx_total]
        tx_all = [0 if not hour else sum(hour) for hour in data_tx_total]
        data_all = [0 if not hour else sum(hour) for hour in data_all_total]
        overall_demand = overall_demand + sum(data_all)

        f.write('{0};{1}\n'.format('foreground use', foreground_all))
        f.write('{0};{1}\n'.format('data rx', rx_all))
        f.write('{0};{1}\n'.format('data tx', tx_all))
        f.write('{0};{1}\n'.format('data all', data_all))

    for practice, data in practices_foreground.items():
        total_foreground_use = [0 if not hour else sum(hour) for hour in data]
        overall_use_from_categories = overall_use_from_categories + sum(total_foreground_use)
        with open('overall_summary/all_practice_use.csv', 'a') as f:
            f.write('"{0}"'.format(practice))
            for i in range(0,24):
                f.write(',{0}'.format(total_foreground_use[i]))
            f.write('\n')

    for practice, data in practices_rx.items():
        total_rx = [0 if not hour else sum(hour) for hour in data]
        # Write practice summaries to files
        with open('overall_summary/all_practice_rx.csv', 'a') as f:
            f.write('"{0}"'.format(practice))
            for i in range(0,24):
                f.write(',{0}'.format(total_rx[i]))
            f.write('\n')

    for practice, data in practices_tx.items():
        total_tx = [0 if not hour else sum(hour) for hour in data]
        # Write practice summaries to files
        with open('overall_summary/all_practice_tx.csv', 'a') as f:
            f.write('"{0}"'.format(practice))
            for i in range(0,24):
                f.write(',{0}'.format(total_tx[i]))
            f.write('\n')

    for practice, data in practices_data.items():
        total_data = [0 if not hour else sum(hour) for hour in data]
        overall_demand_from_categories = overall_demand_from_categories + sum(total_data)
        # Write practice summaries to files
        with open('overall_summary/all_practice_data.csv', 'a') as f:
            f.write('"{0}"'.format(practice))
            for i in range(0,24):
                f.write(',{0}'.format(total_data[i]))
            f.write('\n')

    # OVERALL SUMMARY FOR USE
    with open('overall_summary/daily_practice_use.csv', 'w') as f:
        f.write('{0},{1}\n\n'.format('Overall use', overall_use))
        percentage_for_all_categories = (overall_use_from_categories/overall_use) * 100
        f.write('{0},{1},{2}\n\n'.format('Overall use from categories', overall_use_from_categories, percentage_for_all_categories))
        f.write('Category, use (instances), percentage of overall use (%)\n')
    for practice, data in practices_foreground.items():
        category_use = sum([0 if not hour else sum(hour) for hour in data])
        category_percentage = (category_use/overall_use) * 100
        with open('overall_summary/daily_practice_use.csv', 'a') as f:
            f.write('"{0}",{1},{2}\n'.format(practice, category_use, category_percentage))

    # OVERALL SUMMARY FOR DEMAND
    with open('overall_summary/daily_practice_data.csv', 'w') as f:
        f.write('{0},{1}\n\n'.format('Overall demand', overall_demand))
        percentage_for_all_categories = (overall_demand_from_categories/overall_demand) * 100
        f.write('{0},{1},{2}\n\n'.format('Overall demand from categories', overall_demand_from_categories, percentage_for_all_categories))
        f.write('Category, demand (bytes), percentage of overall demand (%)\n')
    for practice, data in practices_data.items():
        category_demand = sum([0 if not hour else sum(hour) for hour in data])
        category_percentage = (category_demand/overall_demand) * 100
        with open('overall_summary/daily_practice_data.csv', 'a') as f:
            f.write('"{0}",{1},{2}\n'.format(practice, category_demand, category_percentage))


if __name__ == '__main__':
    global apps_rx
    global apps_tx
    global foreground_use
    global app_practice_mapping
    global p_practice_demand_contribution
    global p_practice_use_contribution
    global all_use_contribution
    global all_demand_contribution
    global contribution

    pathOfIdsFile = sys.argv[1]
    pathOfFiles = sys.argv[2]
    pathOfAppPracticeMapping = sys.argv[3]
    lancs = bool(len(sys.argv) > 4)

    startTime = datetime.now()

    foreground_use = [[] for x in range(0,24)]
    data_rx = [[] for x in range(0,24)]
    data_tx = [[] for x in range(0,24)]

    apps_rx = {}
    apps_tx = {}
    foreground_use = {}

    all_use_contribution = set()
    all_demand_contribution = set()
    contribution = set()

    app_practice_mapping = {}
    p_practice_demand_contribution = {}
    p_practice_use_contribution = {}
    for app in read_app_mapping(pathOfAppPracticeMapping):
        app_practice_mapping[app.FullName] = app.Practice
        if app.Practice not in p_practice_demand_contribution:
            p_practice_demand_contribution[app.Practice] = set()
        if app.Practice not in p_practice_use_contribution:
            p_practice_use_contribution[app.Practice] = set()

    # Make sure 'out/' folder exists and reset/create output files
    make_sure_path_exists('overall_summary/')

    with open('overall_summary/all_practice_use.csv', 'w') as f:
        f.write('hour')
        for i in range(0,24):
            f.write(',{0}'.format(i))
        f.write('\n')

    with open('overall_summary/all_practice_rx.csv', 'w') as f:
        f.write('hour')
        for i in range(0,24):
            f.write(',{0}'.format(i))
        f.write('\n')

    with open('overall_summary/all_practice_tx.csv', 'w') as f:
        f.write('hour')
        for i in range(0,24):
            f.write(',{0}'.format(i))
        f.write('\n')

    with open('overall_summary/all_practice_data.csv', 'w') as f:
        f.write('hour')
        for i in range(0,24):
            f.write(',{0}'.format(i))
        f.write('\n')

    for file in (read_file_names_lancs(pathOfIdsFile) if lancs else read_file_names(pathOfIdsFile)):
        fname = file.FileName
        fullfpath = pathOfFiles + file.FileName + '.csv'
        if not lancs:
            fullfpath = fullfpath + '.gz'
        print("Parsing file: " + fname)
        parse_file(fullfpath, lancs, fname)

    calculate_print_summaries()

    # **** For checking timings *****
    endFilesTime = datetime.now()
    print("All files summarised in {0}".format(str((endFilesTime - startTime))))
