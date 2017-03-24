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
import subprocess
from collections import namedtuple, OrderedDict
import dateutil.parser
from datetime import datetime, timedelta
from functools import reduce

global app_practice_mapping

global all_demand_rx_contribution
global all_demand_tx_contribution
global all_demand_contribution

global data_rx_total
global data_tx_total

global overall_weekday_rx
global overall_weekday_tx
global overall_weekday
global overall_weekend_rx
global overall_weekend_tx
global overall_weekend

fields_da = ('Entry','Num','Date','EntryType','Value')
DARecord = namedtuple('DARecord', fields_da)
def read_file(path):
    try:
        with io.TextIOWrapper(io.BufferedReader(gzip.open(path))) as data:
            for line in data:
                #Repack variable number of items per line into five expected items
                #(Problem is internal DA format uses ';' to separate csv items as well
                # as to separate app names inside the 'Value' field.)
                e = line.split(';')
                value = reduce(lambda x, y: x + ',' + y, e[4:])
                repacked = e[0:4] + [value]
                yield DARecord._make(repacked)
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

def get_t_gap(first, second):
    return (dateutil.parser.parse(second) - dateutil.parser.parse(first)).total_seconds()

def search_dates(file_path, lancs):
    start_date = None
    end_date = None
    last_date_time_in_log = None

    for row in (read_file_lancs(file_path) if lancs else read_file(file_path)):
        date_time = row.Date.rsplit('T')
        if '(invalid date)' in row.Date:
            continue

        if start_date == None:
            current_hour = int(date_time[1].split(':')[0])
            if current_hour <= 4:
                start_date = date_time[0]
            else:
                start_date = ((datetime.strptime(date_time[0], '%Y-%m-%d')) + timedelta(days=1)).strftime('%Y-%m-%d')

        last_date_time_in_log = date_time

    last_hour_in_log = int(last_date_time_in_log[1].split(':')[0])
    if last_hour_in_log >= 4:
        end_date = last_date_time_in_log[0]
    else:
        end_date = ((datetime.strptime(last_date_time_in_log[0], '%Y-%m-%d')) - timedelta(days=1)).strftime('%Y-%m-%d')

    return start_date+'T04:00:00', end_date+'T04:00:00'

def get_start_end_dates(file_path, lancs):
    start = None
    end = None
    if lancs:
        start = str(subprocess.check_output(['head', '-1', file_path])).split(';')[2][:-9]
        end = str(subprocess.check_output(['tail', '-1', file_path])).split(';')[2][:-9]
        if '(invalid date)' in start or '(invalid date)' in end:
            start, end = search_dates()
    else:
        start, end = search_dates(file_path, lancs)

    start_date_time = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S')
    end_date_time = datetime.strptime(end, '%Y-%m-%dT%H:%M:%S')

    start_date_to_return = None
    end_date_to_return = None
    # If before 4am, then the date is fine - else add a day
    if start_date_time.time().hour <= 4:
        start_date_to_return = start_date_time.strftime('%Y-%m-%d')
    else:
        start_date_to_return = (start_date_time + timedelta(days=1)).strftime('%Y-%m-%d')
    # If after or equal to 4am, then the date is fine - else remove a day
    if end_date_time.time().hour >= 4:
        end_date_to_return = end_date_time.strftime('%Y-%m-%d')
    else:
        end_date_to_return = (end_date_time - timedelta(days=1)).strftime('%Y-%m-%d')

    return start_date_to_return+'T04:00:00', end_date_to_return+'T04:00:00'

def parse_file(file_path, lancs, fname):
    global all_demand_rx_contribution
    global all_demand_tx_contribution
    global all_demand_contribution
    global data_rx_total
    global data_tx_total
    global overall_weekday_rx
    global overall_weekday_tx
    global overall_weekday
    global overall_weekend_rx
    global overall_weekend_tx
    global overall_weekend

    start_date, end_date = get_start_end_dates(file_path, lancs)

    logs_to_parse = ['app', 'screen', 'hf', 'net']

    current_hour = None
    current_day = None
    current_weekday = None
    no_of_days = 0

    app_foreground_use = {}

    screen_on = False
    screen_unlocked = False
    last_importance_app_pid = None

    last_app_data = {}
    all_data_rx = [[] for hour in range(0,24)]
    all_data_tx = [[] for hour in range(0,24)]

    no_of_days_week = [0 for day in range(0,7)]

    ids_names = {}
    current_app_name_id_mapping = {}
    app_data = {}

    for row in (read_file_lancs(file_path) if lancs else read_file(file_path)):
        row_entry_type = row.EntryType
        entry_val = row_entry_type.split('|')
        row_date = row.Date
        date_time = row_date.rsplit('T')
        row_value = row.Value.strip()

        if entry_val[0] not in logs_to_parse or row_date == '(invalid date)':
            continue

        if row_date[:-9] < start_date or row_date[:-9] > end_date:
            continue

        if current_day != date_time[0]:
            current_day = date_time[0]
            current_weekday = datetime.strptime(date_time[0], '%Y-%m-%d').weekday()
            no_of_days_week[current_weekday]+=1
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
                        app_foreground_use[app_name] = [[0 for x in range(0,24)] for y in range(0,7)]
                    app_foreground_use[app_name][current_weekday][current_hour]+=1

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
                    app_data[app_name][1][current_weekday][current_hour].append(int(row_value) - app_last_rx)
                elif int(row_value) < app_last_rx:
                    app_data[app_name][1][current_weekday][current_hour].append(int(row_value))
                app_data[app_name][0] = int(row_value)
            elif entry_val[3] == 'tx_bytes':
                app_last_tx = app_data[app_name][2]
                if app_last_tx == None:
                    pass
                elif int(row_value) > app_last_tx:
                    app_data[app_name][3][current_weekday][current_hour].append(int(row_value) - app_last_tx)
                elif int(row_value) < app_last_tx:
                    app_data[app_name][3][current_weekday][current_hour].append(int(row_value))
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
                        app_data[temp_name] = [None, [[[] for x in range(0,24)] for y in range(0,7)], None, [[[] for x in range(0,24)] for y in range(0,7)]]

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
        for app, data in app_data.items():

            mean_rx = [[] for i in range(0,7)]
            mean_tx = [[] for i in range(0,7)]

            weekday_total_rx = [0 for i in range(0,24)]
            weekday_total_tx = [0 for i in range(0,24)]
            weekday_total = [0 for i in range(0,24)]

            weekend_total_rx = [0 for i in range(0,24)]
            weekend_total_tx = [0 for i in range(0,24)]
            weekend_total = [0 for i in range(0,24)]

            for index, no_of_days_of_day in enumerate(no_of_days_week):
                mean_rx[index] = [(sum(ihour)/no_of_days_of_day) for ihour in data[1][index]]
                mean_tx[index] = [(sum(ihour)/no_of_days_of_day) for ihour in data[3][index]]

                day_total_rx = [sum(ihour) for ihour in data[1][index]]
                day_total_tx = [sum(ihour) for ihour in data[3][index]]
                if index < 5:
                    for hour in range(0,24):
                        weekday_total_rx[hour] = weekday_total_rx[hour] + day_total_rx[hour]
                        weekday_total_tx[hour] = weekday_total_tx[hour] + day_total_tx[hour]
                        weekday_total[hour] = weekday_total[hour] + day_total_rx[hour] + day_total_tx[hour]
                else:
                    for hour in range(0,24):
                        weekend_total_rx[hour] = weekend_total_rx[hour] + day_total_rx[hour]
                        weekend_total_tx[hour] = weekend_total_tx[hour] + day_total_tx[hour]
                        weekend_total[hour] = weekend_total[hour] + day_total_rx[hour] + day_total_tx[hour]

            no_of_weekday_days = sum(no_of_days_week[:5])
            weekday_mean_rx = [ihour/no_of_weekday_days for ihour in weekday_total_rx]
            weekday_mean_tx = [ihour/no_of_weekday_days for ihour in weekday_total_tx]
            weekday_mean = [ihour/no_of_weekday_days for ihour in weekday_total]

            no_of_weekend_days = sum(no_of_days_week[5:7])
            weekend_mean_rx = [ihour/no_of_weekend_days for ihour in weekend_total_rx]
            weekend_mean_tx =[ihour/no_of_weekend_days for ihour in weekend_total_tx]
            weekend_mean = [ihour/no_of_weekend_days for ihour in weekend_total]

            add_to_overall_total = False

            if not all(hour == 0 for day in mean_rx for hour in day):
                add_to_overall_total = True
                for day in range(0,7):
                    for hour in range(0,24):
                        data_rx_total[day][hour] = data_rx_total[day][hour] + mean_rx[day][hour]
                all_demand_rx_contribution.add(fname)
                all_demand_contribution.add(fname)
                # Weekday and weekend rx
                for i in range(0,24):
                    overall_weekday_rx[i] = overall_weekday_rx[i] + weekday_mean_rx[i]
                    overall_weekend_rx[i] = overall_weekend_rx[i] + weekend_mean_rx[i]

            if not all(hour == 0 for day in mean_tx for hour in day):
                add_to_overall_total = True
                for day in range(0,7):
                    for hour in range(0,24):
                        data_tx_total[day][hour] = data_tx_total[day][hour] + mean_tx[day][hour]
                all_demand_tx_contribution.add(fname)
                all_demand_contribution.add(fname)
                # Weekday and weekend tx
                for i in range(0,24):
                    overall_weekday_tx[i] = overall_weekday_tx[i] + weekday_mean_tx[i]
                    overall_weekend_tx[i] = overall_weekend_tx[i] + weekend_mean_tx[i]

            if add_to_overall_total is True:
                for i in range(0,24):
                    overall_weekday[i] = overall_weekday[i] + weekday_mean[i]
                    overall_weekend[i] = overall_weekend[i] + weekend_mean[i]

def calculate_print_summaries():
    global data_rx_total
    global data_tx_total
    global all_demand_rx_contribution
    global all_demand_tx_contribution
    global all_demand_contribution
    global overall_weekday_rx
    global overall_weekday_tx
    global overall_weekday
    global overall_weekend_rx
    global overall_weekend_tx
    global overall_weekend

    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    with open('day_totals_output/contribution.csv', 'w') as f:
        f.write('demand rx,{0}\n'.format(len(all_demand_rx_contribution)))
        f.write('demand tx,{0}\n'.format(len(all_demand_tx_contribution)))
        f.write('demand rx and tx,{0}\n'.format(len(all_demand_contribution)))

    data_total = [[0 for i in range(0,24)] for day in range(0,7)]
    for day in range(0,7):
        for hour in range(0,24):
            data_total[day][hour] = data_rx_total[day][hour] + data_tx_total[day][hour]

    with open('day_totals_output/days_of_week_demand_rx.csv', 'w') as f:
        for index, day in enumerate(days_of_week):
            f.write('{0};{1}\n'.format(day, data_rx_total[index]))

    with open('day_totals_output/days_of_week_demand_tx.csv', 'w') as f:
        for index, day in enumerate(days_of_week):
            f.write('{0};{1}\n'.format(day, data_tx_total[index]))

    with open('day_totals_output/days_of_week_demand_all.csv', 'w') as f:
        for index, day in enumerate(days_of_week):
            f.write('{0};{1}\n'.format(day, data_total[index]))

    with open('day_totals_output/weekday_weekend_demand.csv', 'w') as f:
        f.write('weekday rx;{0}\n'.format(overall_weekday_rx))
        f.write('weekday tx;{0}\n'.format(overall_weekday_tx))
        f.write('weekday;{0}\n'.format(overall_weekday))
        f.write('weekend rx;{0}\n'.format(overall_weekend_rx))
        f.write('weekend tx;{0}\n'.format(overall_weekend_tx))
        f.write('weekend;{0}\n'.format(overall_weekend))

if __name__ == '__main__':
    global app_practice_mapping

    global all_demand_rx_contribution
    global all_demand_tx_contribution
    global all_demand_contribution

    global data_rx_total
    global data_tx_total

    global overall_weekday_rx
    global overall_weekday_tx
    global overall_weekday
    global overall_weekend_rx
    global overall_weekend_tx
    global overall_weekend

    pathOfIdsFile = sys.argv[1]
    pathOfFiles = sys.argv[2]
    pathOfAppPracticeMapping = sys.argv[3]
    lancs = bool(len(sys.argv) > 4)

    startTime = datetime.now()

    data_rx_total = [[0 for i in range(0,24)] for day in range(0,7)]
    data_tx_total = [[0 for i in range(0,24)] for day in range(0,7)]

    overall_weekday_rx = [0 for i in range(0,24)]
    overall_weekday_tx = [0 for i in range(0,24)]
    overall_weekday = [0 for i in range(0,24)]
    overall_weekend_rx = [0 for i in range(0,24)]
    overall_weekend_tx = [0 for i in range(0,24)]
    overall_weekend = [0 for i in range(0,24)]

    all_demand_rx_contribution = set()
    all_demand_tx_contribution = set()
    all_demand_contribution = set()

    app_practice_mapping = {}
    for app in read_app_mapping(pathOfAppPracticeMapping):
        app_practice_mapping[app.FullName] = app.Practice

    # Make sure 'out/' folder exists and reset/create output files
    make_sure_path_exists('day_totals_output/')

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
