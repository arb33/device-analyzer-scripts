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

global apps                             #List of apps installed on 50 or more devices
global devices_apps_foreground_use      #Hourly mean no of foreground instances for apps across devices whilst the device is in use - 'in use' means screen on and unlocked
global devices_apps_foreground_other    #Hourly mean no of foreground instances for apps across devices other than when the device is in use
global devices_use_durations            #Hourly mean time device was on across devices
global devices_use_instances            #Hourly mean no of times device was on across devices

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

AppRecord = namedtuple('AppRecord', ('FullName'))
def read_app_mapping(path):
    with open(path, 'rU') as data:
        csv.field_size_limit(sys.maxsize)
        reader = csv.reader(data, delimiter=',')
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
    global apps
    global devices_apps_foreground_use
    global devices_apps_foreground_other
    global devices_use_durations
    global devices_use_instances

    logs_to_parse = ['app', 'screen', 'hf']

    current_hour = None
    current_day = None
    no_of_days = 0

    app_foreground_use = {}
    app_foreground_other = {}

    screen_on = False
    screen_unlocked = False

    screen_on_start_time = None
    screen_on_times = [[] for x in range(0,24)]

    last_importance_app_pid = None

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
            if row_value in apps and entry_val[1] == last_importance_app_pid:
                # The user is using the device
                if screen_on and screen_unlocked:
                    # Increment the number of times it was in the foreground
                    if row_value not in app_foreground_use:
                        app_foreground_use[row_value] = [0 for x in range(0,24)]
                    app_foreground_use[row_value][current_hour]+=1
                # Otherwise log the foreground instance in the app_foreground_other list
                else:
                    if row_value not in app_foreground_other:
                        app_foreground_other[row_value] = [0 for x in range(0,24)]
                    app_foreground_other[row_value][current_hour]+=1

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
                if screen_on_start_time != None:
                    screen_on_times[current_hour].append(get_t_gap(screen_on_start_time, row_date))
                    screen_on_start_time = None
            else:
                screen_on = True
                screen_on_start_time = row_date


    if no_of_days != 0:

        # Foreground apps whilst device was in use - i.e. screen on and unlocked
        for app, data in app_foreground_use.items():
            # Calculate hourly means for the device app foregound instances
            mean_app_foreground_use = [(no_of_foreground_instances/no_of_days) for no_of_foreground_instances in data]
            if not all(i == 0 for i in mean_app_foreground_use):
                if app not in devices_apps_foreground_use:
                    devices_apps_foreground_use[app] = [[] for x in range(0,24)]
                [devices_apps_foreground_use[app][i].append(mean_app_foreground_use[i]) for i in range(0,24)]

        # Foreground apps whilst device other than in use
        for app, data in app_foreground_other.items():
            # Calculate hourly means for the device app foregound instances
            mean_app_foreground_other = [(no_of_foreground_instances/no_of_days) for no_of_foreground_instances in data]
            if not all(i == 0 for i in mean_app_foreground_other):
                if app not in devices_apps_foreground_other:
                    devices_apps_foreground_other[app] = [[] for x in range(0,24)]
                [devices_apps_foreground_other[app][i].append(mean_app_foreground_other[i]) for i in range(0,24)]

        # Screen on/off times - sessions of use in seconds within an hour, averaged across the hour
        mean_hourly_device_use_durations = [(sum(hour_use_time)/no_of_days) for hour_use_time in screen_on_times]
        mean_hourly_device_use_instances = [(len(hour_use_time)/no_of_days) for hour_use_time in screen_on_times]
        if not all(i == 0 for i in mean_hourly_device_use_durations):
            [devices_use_durations[i].append(mean_hourly_device_use_durations[i]) for i in range(0,24)]
        if not all(i == 0 for i in mean_hourly_device_use_instances):
            [devices_use_instances[i].append(mean_hourly_device_use_instances[i]) for i in range(0,24)]

def calculate_print_app_foreground():
    global devices_apps_foreground_use
    global devices_apps_foreground_other

    # App foregound use summary
    for app, data in devices_apps_foreground_use.items():
        total_i = [0 if not hour else sum(hour) for hour in data]
        mean_i = [0 if not hour else np.mean(hour) for hour in data]
        no_of_devices = [0 if not hour else len(hour) for hour in data]
        min_i = [0 if not hour else np.min(hour) for hour in data]
        max_i = [0 if not hour else np.max(hour) for hour in data]
        med_i = [0 if not hour else np.median(hour) for hour in data]

        with open('use_out/app_foreground_use_hourly.csv', 'a') as f:
            f.write('{0};{1};{2};{3};{4};{5};{6}\n'.format(app, total_i, mean_i, no_of_devices, min_i, max_i, med_i))
        with open('use_out/app_use_totals_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, total_i))
        with open('use_out/app_use_means_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, mean_i))
        with open('use_out/app_use_deviceNo_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, no_of_devices))
        with open('use_out/app_use_mins_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, min_i))
        with open('use_out/app_use_maxs_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, max_i))
        with open('use_out/app_use_meds_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, med_i))

    # App foregound other summary
    for app, data in devices_apps_foreground_other.items():
        total_i = [0 if not hour else sum(hour) for hour in data]
        mean_i = [0 if not hour else np.mean(hour) for hour in data]
        no_of_devices = [0 if not hour else len(hour) for hour in data]
        min_i = [0 if not hour else np.min(hour) for hour in data]
        max_i = [0 if not hour else np.max(hour) for hour in data]
        med_i = [0 if not hour else np.median(hour) for hour in data]

        with open('use_out/app_foreground_other_hourly.csv', 'a') as f:
            f.write('{0};{1};{2};{3};{4};{5};{6}\n'.format(app, total_i, mean_i, no_of_devices, min_i, max_i, med_i))
        with open('use_out/app_other_totals_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, total_i))
        with open('use_out/app_other_means_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, mean_i))
        with open('use_out/app_other_deviceNo_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, no_of_devices))
        with open('use_out/app_other_mins_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, min_i))
        with open('use_out/app_other_maxs_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, max_i))
        with open('use_out/app_other_meds_hourly.csv', 'a') as f:
            f.write('{0};{1}\n'.format(app, med_i))

def calculate_print_device_use():
    global devices_use_durations
    global devices_use_instances

    # Device use summary
    # Calculate device use durations summary - hourly totals, means, no of devices, mins, maxs, medians across devices
    dur_total_device_use = [0 if not hour else sum(hour) for hour in devices_use_durations]
    dur_mean_device_use = [0 if not hour else np.mean(hour) for hour in devices_use_durations]
    dur_devices_device_use = [0 if not hour else len(hour) for hour in devices_use_durations]
    dur_min_device_use = [0 if not hour else np.min(hour) for hour in devices_use_durations]
    dur_max_device_use = [0 if not hour else np.max(hour) for hour in devices_use_durations]
    dur_med_device_use = [0 if not hour else np.median(hour) for hour in devices_use_durations]
    # Calculate number of device uses summary - hourly totals, means, no of devices, mins, maxs, medians across devices
    no_total_device_use = [0 if not hour else sum(hour) for hour in devices_use_instances]
    no_mean_device_use = [0 if not hour else np.mean(hour) for hour in devices_use_instances]
    no_devices_device_use = [0 if not hour else len(hour) for hour in devices_use_instances]
    no_min_device_use = [0 if not hour else np.min(hour) for hour in devices_use_instances]
    no_max_device_use = [0 if not hour else np.max(hour) for hour in devices_use_instances]
    no_med_device_use = [0 if not hour else np.median(hour) for hour in devices_use_instances]

    # Write device use summary to file
    with open('use_out/device_use_hourly.csv', 'a') as f:
        f.write('durations;\nduration totals;{0}\nmean durations;{1}\nno. devices;{2}\nmin duration;{3}\nmax duration;{4}\nmedian duration;{5}\n'.format(dur_total_device_use, dur_mean_device_use, dur_devices_device_use, dur_min_device_use, dur_max_device_use, dur_med_device_use))
        f.write('no. of device uses;\nno. of device uses totals;{0}\nmean no.;{1}\nno. devices;{2}\nmin no.;{3}\nmax no.;{4}\nmedian no.;{5}\n'.format(no_total_device_use, no_mean_device_use, no_devices_device_use, no_min_device_use, no_max_device_use, no_med_device_use))

if __name__ == '__main__':
    global apps
    global devices_apps_foreground_use
    global devices_apps_foreground_other
    global devices_use_durations
    global devices_use_instances

    pathOfIdsFile = sys.argv[1]
    pathOfFiles = sys.argv[2]
    pathOfAppMappingFile = sys.argv[3]
    lancs = bool(len(sys.argv) > 4)

    startTime = datetime.now()

    apps = {}
    devices_apps_foreground_use = {}
    devices_apps_foreground_other = {}
    for app in read_app_mapping(pathOfAppMappingFile):
        apps[app.FullName] = ''

    devices_use_durations = [[] for x in range(0,24)]
    devices_use_instances = [[] for x in range(0,24)]

    # Make sure 'out/' folder exists and reset/create output files
    make_sure_path_exists('use_out/')
    output_files = ['device_use_hourly.csv', 'app_foreground_use_hourly.csv', 'app_use_totals_hourly.csv', 'app_use_means_hourly.csv', 'app_use_deviceNo_hourly.csv', 'app_use_mins_hourly.csv', 'app_use_maxs_hourly.csv', 'app_use_meds_hourly.csv']
    output_files_other = ['app_foreground_other_hourly.csv', 'app_other_totals_hourly.csv', 'app_other_means_hourly.csv', 'app_other_deviceNo_hourly.csv', 'app_other_mins_hourly.csv', 'app_other_maxs_hourly.csv', 'app_other_meds_hourly.csv']
    for of_name in output_files + output_files_other:
        with open('use_out/' + of_name, 'w') as f:
            f.write('')

    for file in (read_file_names_lancs(pathOfIdsFile) if lancs else read_file_names(pathOfIdsFile)):
        fname = file.FileName
        fullfpath = pathOfFiles + file.FileName + '.csv'
        if not lancs:
            fullfpath = fullfpath + '.gz'
        print("Parsing file: " + fname)
        parse_file(fullfpath, lancs)

    calculate_print_app_foreground()
    calculate_print_device_use()

    # **** For checking timings *****
    endFilesTime = datetime.now()
    print("All files summarised in {0}".format(str((endFilesTime - startTime))))
