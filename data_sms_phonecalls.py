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
global sms_sent_hourly
global sms_sent_total_hourly
global sms_received_hourly
global sms_received_total_hourly
global mean_phone_call_durations_hourly
global mean_no_of_phone_calls_hourly

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
    global apps_practices
    global sms_sent_hourly
    global sms_sent_total_hourly
    global sms_received_hourly
    global sms_received_total_hourly
    global mean_phone_call_durations_hourly
    global mean_no_of_phone_calls_hourly

    last_s_sms = None
    last_r_sms = None
    sms_sent = [[] for x in range(0,24)]
    sms_received = [[] for x in range(0,24)]

    last_phone_state = None
    last_phone_datetime = None
    phone_calls = [[] for x in range(0,24)]

    logs_to_parse = ['net','app', 'sms', 'phone']

    current_hour = None
    current_day = None
    no_of_days = 0

    ids_names = {}
    current_app_name_id_mapping = {}
    app_data = {}
    # app_data['Other'] = [None, [[] for x in range(0,24)], None, [[] for x in range(0,24)]]

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
                # app_name = 'Other'

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
        # SMS
        elif row_entry_type.startswith('sms') and entry_val[1] == 'count':
            if entry_val[2] == 'inbox':
                if last_r_sms == None:
                    sms_received = [[] for x in range(0,24)]
                elif int(row_value) > last_r_sms:
                    sms_received[current_hour].append(int(int(row_value) - last_r_sms))
                last_r_sms = int(row_value)
            elif entry_val[2] == 'sent':
                if last_s_sms == None:
                    sms_sent = [[] for x in range(0,24)]
                elif int(row_value) > last_s_sms:
                    sms_sent[current_hour].append(int(int(row_value) - last_s_sms))
                last_s_sms = int(row_value)
        # PHONE CALLS
        elif row_entry_type.startswith('phone'):
            currentPhoneState = row_entry_type.split('|')[1]
            if last_phone_state == 'offhook' and (currentPhoneState == 'idle' or currentPhoneState == 'calling' or currentPhoneState == 'ringing'):
                phone_call_hour = int((last_phone_datetime.rsplit('T')[1]).split(':')[0])
                phone_calls[phone_call_hour].append(get_t_gap(last_phone_datetime,row_date))
            if currentPhoneState == 'offhook' or currentPhoneState == 'idle' or currentPhoneState == 'calling' or currentPhoneState == 'ringing':
                last_phone_state = currentPhoneState
                last_phone_datetime = row_date

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

        # Append this device's sms hourly averages to overall sms
        mean_sent = [(sum(ihour)/no_of_days) for ihour in sms_sent]
        mean_received = [(sum(ihour)/no_of_days) for ihour in sms_received]
        if not all(i == 0 for i in mean_sent):
            [sms_sent_hourly[i].append(mean_sent[i]) for i in range(0,24)]
        if not all(i == 0 for i in mean_received):
            [sms_received_hourly[i].append(mean_received[i]) for i in range(0,24)]

        # Append this device's hourly phone call average durations and average no. of phone calls to overall phone calls
        mean_phone_call_durations = [(sum(ihour)/no_of_days) for ihour in phone_calls]
        mean_no_phone_calls = [(len(ihour)/no_of_days) for ihour in phone_calls]
        if not all(i == 0 for i in mean_phone_call_durations):
            [mean_phone_call_durations_hourly[i].append(mean_phone_call_durations[i]) for i in range(0,24)]
        if not all(i == 0 for i in mean_no_phone_calls):
            [mean_no_of_phone_calls_hourly[i].append(mean_no_phone_calls[i]) for i in range(0,24)]

def calculate_print_app_data_summary():
    global apps_practices

    # APP SUMMARY
    for app, data in apps_practices.items():
        app_name = data[0]
        practice_name = data[1]

        # Calculate the overall hourly totals, means, no of devices, mins, maxs and medians for the apps across devices for rx
        total_rx = [0 if not hour else sum(hour) for hour in data[2]]
        mean_rx = [0 if not hour else np.mean(hour) for hour in data[2]]
        devices_rx = [0 if not hour else len(hour) for hour in data[2]]
        min_rx = [0 if not hour else np.min(hour) for hour in data[2]]
        max_rx = [0 if not hour else np.max(hour) for hour in data[2]]
        med_rx = [0 if not hour else np.median(hour) for hour in data[2]]

        # Calculate the overall hourly totals, means, no of devices, mins, maxs and medians for the apps across devices for tx
        total_tx = [0 if not hour else sum(hour) for hour in data[3]]
        mean_tx = [0 if not hour else np.mean(hour) for hour in data[3]]
        devices_tx = [0 if not hour else len(hour) for hour in data[3]]
        min_tx = [0 if not hour else np.min(hour) for hour in data[3]]
        max_tx = [0 if not hour else np.max(hour) for hour in data[3]]
        med_tx = [0 if not hour else np.median(hour) for hour in data[3]]

        # Write app summaries to files
        with open('out/app_hourly_summaries.csv', 'a') as f:
            f.write('{0};{1}\n{2};{3}\n'.format(app, 'rx_bytes;{0},{1},{2},{3},{4},{5}'.format(total_rx, mean_rx, devices_rx, min_rx, max_rx, med_rx), app, 'tx_bytes;{0},{1},{2},{3},{4},{5}'.format(total_tx, mean_tx, devices_tx, min_tx, max_tx, med_tx)))
        with open('out/app_hourly_totals.csv', 'a') as f:
            f.write('{0};{1}\n{2};{3}\n'.format(app, 'rx_bytes;{0}'.format(total_rx), app, 'tx_bytes;{0}'.format(total_tx)))
        with open('out/app_hourly_means.csv', 'a') as f:
            f.write('{0};{1}\n{2};{3}\n'.format(app, 'rx_bytes;{0}'.format(mean_rx), app, 'tx_bytes;{0}'.format(mean_tx)))
        with open('out/app_hourly_devicesNo.csv', 'a') as f:
            f.write('{0};{1}\n{2};{3}\n'.format(app, 'rx_bytes;{0}'.format(devices_rx), app, 'tx_bytes;{0}'.format(devices_tx)))
        with open('out/app_hourly_mins.csv', 'a') as f:
            f.write('{0};{1}\n{2};{3}\n'.format(app, 'rx_bytes;{0}'.format(min_rx), app, 'tx_bytes;{0}'.format(min_tx)))
        with open('out/app_hourly_maxs.csv', 'a') as f:
            f.write('{0};{1}\n{2};{3}\n'.format(app, 'rx_bytes;{0}'.format(max_rx), app, 'tx_bytes;{0}'.format(max_tx)))
        with open('out/app_hourly_meds.csv', 'a') as f:
            f.write('{0};{1}\n{2};{3}\n'.format(app, 'rx_bytes;{0}'.format(med_rx), app, 'tx_bytes;{0}'.format(med_tx)))

def calculate_print_sms_summaries():
    global sms_sent_hourly
    global sms_received_hourly

    # SMS SUMMARY
    # Calculate sent sms summary - hourly totals, means, no of devices, mins, maxs, medians across devices
    total_sms_sent = [0 if not hour else sum(hour) for hour in sms_sent_hourly]
    mean_sms_sent = [0 if not hour else np.mean(hour) for hour in sms_sent_hourly]
    devices_sent = [0 if not hour else len(hour) for hour in sms_sent_hourly]
    min_sent = [0 if not hour else np.min(hour) for hour in sms_sent_hourly]
    max_sent = [0 if not hour else np.max(hour) for hour in sms_sent_hourly]
    med_sent = [0 if not hour else np.median(hour) for hour in sms_sent_hourly]
    # Calculate received sms summary - hourly totals, means, no of devices, mins, maxs, medians across devices
    total_sms_received = [0 if not hour else sum(hour) for hour in sms_received_hourly]
    mean_sms_received = [0 if not hour else np.mean(hour) for hour in sms_received_hourly]
    devices_received = [0 if not hour else len(hour) for hour in sms_received_hourly]
    min_received = [0 if not hour else np.min(hour) for hour in sms_received_hourly]
    max_received = [0 if not hour else np.max(hour) for hour in sms_received_hourly]
    med_received = [0 if not hour else np.median(hour) for hour in sms_received_hourly]

    # Write SMS summary to file
    with open('out/sms_summary.csv', 'a') as f:
        f.write('sms_sent;\ntotal sent;{0}\nmean sent;{1}\nno. devices sent;{2}\nmin sent;{3}\nmax sent;{4}\nmedian sent;{5}\n'.format(total_sms_sent, mean_sms_sent, devices_sent, min_sent, max_sent, med_sent))
        f.write('sms_received;\ntotal received;{0}\nmean received;{1}\nno. devices received;{2}\nmin received;{3}\nmax received;{4}\nmedian received;{5}\n'.format(total_sms_received, mean_sms_received, devices_received, min_received, max_received, med_received))

def calculate_print_phone_call_summaries():
    global mean_phone_call_durations_hourly
    global mean_no_of_phone_calls_hourly

    # PHONE CALLS SUMMARY
    # Calculate phone call durations summary - hourly totals, means, no of devices, mins, maxs, medians across devices
    dur_total_phone_calls = [0 if not hour else sum(hour) for hour in mean_phone_call_durations_hourly]
    dur_mean_phone_calls = [0 if not hour else np.mean(hour) for hour in mean_phone_call_durations_hourly]
    dur_devices_phone_calls = [0 if not hour else len(hour) for hour in mean_phone_call_durations_hourly]
    dur_min_phone_calls = [0 if not hour else np.min(hour) for hour in mean_phone_call_durations_hourly]
    dur_max_phone_calls = [0 if not hour else np.max(hour) for hour in mean_phone_call_durations_hourly]
    dur_med_phone_calls = [0 if not hour else np.median(hour) for hour in mean_phone_call_durations_hourly]
    # Calculate number of phone calls summary - hourly totals, means, no of devices, mins, maxs, medians across devices
    no_total_phone_calls = [0 if not hour else sum(hour) for hour in mean_no_of_phone_calls_hourly]
    no_mean_phone_calls = [0 if not hour else np.mean(hour) for hour in mean_no_of_phone_calls_hourly]
    no_devices_phone_calls = [0 if not hour else len(hour) for hour in mean_no_of_phone_calls_hourly]
    no_min_phone_calls = [0 if not hour else np.min(hour) for hour in mean_no_of_phone_calls_hourly]
    no_max_phone_calls = [0 if not hour else np.max(hour) for hour in mean_no_of_phone_calls_hourly]
    no_med_phone_calls = [0 if not hour else np.median(hour) for hour in mean_no_of_phone_calls_hourly]

    # Write phone calls summary to file
    with open('out/phone_calls_summary.csv', 'a') as f:
        f.write('durations;\nduration totals;{0}\nmean durations;{1}\nno. devices;{2}\nmin duration;{3}\nmax duration;{4}\nmedian duration;{5}\n'.format(dur_total_phone_calls, dur_mean_phone_calls, dur_devices_phone_calls, dur_min_phone_calls, dur_max_phone_calls, dur_med_phone_calls))
        f.write('no. of calls;\nno. of calls totals;{0}\nmean no.;{1}\nno. devices;{2}\nmin no.;{3}\nmax no.;{4}\nmedian no.;{5}\n'.format(no_total_phone_calls, no_mean_phone_calls, no_devices_phone_calls, no_min_phone_calls, no_max_phone_calls, no_med_phone_calls))

if __name__ == '__main__':
    global apps_practices
    global sms_sent_hourly
    global sms_sent_total_hourly
    global sms_received_hourly
    global sms_received_total_hourly
    global mean_phone_call_durations_hourly
    global mean_no_of_phone_calls_hourly

    sms_sent_hourly = [[] for x in range(0,24)]
    sms_sent_total_hourly = [[] for x in range(0,24)]
    sms_received_hourly = [[] for x in range(0,24)]
    sms_received_total_hourly = [[] for x in range(0,24)]
    mean_phone_call_durations_hourly = [[] for x in range(0,24)]
    mean_no_of_phone_calls_hourly = [[] for x in range(0,24)]

    pathOfIdsFile = sys.argv[1]
    pathOfFiles = sys.argv[2]
    pathOfAppMappingFile = sys.argv[3]
    lancs = bool(len(sys.argv) > 4)

    startTime = datetime.now()

    apps_practices = {}
    for app in read_app_mapping(pathOfAppMappingFile):
        apps_practices[app.FullName] = ('', '', [[] for x in range(0,24)], [[] for x in range(0,24)])

    # Make sure 'out/' folder exists and reset/create output files
    make_sure_path_exists('out/')
    output_files = ['sms_summary.csv', 'phone_calls_summary.csv', 'app_hourly_summaries.csv', 'app_hourly_totals.csv', 'app_hourly_means.csv', 'app_hourly_devicesNo.csv', 'app_hourly_mins.csv', 'app_hourly_maxs.csv', 'app_hourly_meds.csv']
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

    calculate_print_app_data_summary()
    calculate_print_sms_summaries()
    calculate_print_phone_call_summaries()

    # **** For checking timings *****
    endFilesTime = datetime.now()
    print("All files summarised in {0}".format(str((endFilesTime - startTime))))
