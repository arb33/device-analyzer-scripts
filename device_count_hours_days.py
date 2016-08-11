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
from collections import namedtuple
import dateutil.parser
from datetime import datetime, timedelta
import numpy as np

global hdc_facebook_rx
global hdc_facebook_tx
global hlc_facebook_rx
global hlc_facebook_tx

global hdc_snapchat_rx
global hdc_snapchat_tx
global hlc_snapchat_rx
global hlc_snapchat_tx

global whdc_facebook_rx
global whdc_facebook_tx
global whlc_facebook_rx
global whlc_facebook_tx

global whdc_snapchat_rx
global whdc_snapchat_tx
global whlc_snapchat_rx
global whlc_snapchat_tx

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

def count_hourly_app_data_logs(file, lancs):
    global hdc_facebook_rx
    global hdc_facebook_tx
    global hlc_facebook_rx
    global hlc_facebook_tx

    global hdc_snapchat_rx
    global hdc_snapchat_tx
    global hlc_snapchat_rx
    global hlc_snapchat_tx

    global whdc_facebook_rx
    global whdc_facebook_tx
    global whlc_facebook_rx
    global whlc_facebook_tx

    global whdc_snapchat_rx
    global whdc_snapchat_tx
    global whlc_snapchat_rx
    global whlc_snapchat_tx

    hc_facebook_rx = np.zeros(24)
    hc_facebook_tx = np.zeros(24)

    hc_snapchat_rx = np.zeros(24)
    hc_snapchat_tx = np.zeros(24)

    whc_facebook_rx = np.zeros((7, 24))
    whc_facebook_tx = np.zeros((7, 24))

    whc_snapchat_rx = np.zeros((7, 24))
    whc_snapchat_tx = np.zeros((7, 24))

    logs_to_parse = ['net','app']
    apps_to_parse = ['com.facebook.katana', 'com.snapchat.android']
    current_app_name_id_mapping = {}
    app_data = {}

    for row in (read_file_lancs(file) if lancs else read_file(file)):
        row_entry_type = row.EntryType
        entry_val = row_entry_type.split('|')
        row_date = row.Date
        date_time = row_date.rsplit('T')
        row_value = row.Value

        if row_date == '(invalid date)' or entry_val[0] not in logs_to_parse:
            continue

        if row_entry_type.startswith('net|app'):
            app_id = entry_val[2]
            app_name = None
            for key, val in current_app_name_id_mapping.items():
                if val == app_id:
                    app_name = key
            if app_name == None:
                continue

            weekday = dateutil.parser.parse(row_date).weekday()
            hour = dateutil.parser.parse(row_date).hour
            if entry_val[3] == 'rx_bytes':
                last_rx = app_data[app_name][0]
                if last_rx != None and last_rx != row_value:
                    if 'facebook' in app_data[app_name][2]:
                        hc_facebook_rx[hour] = 1
                        hlc_facebook_rx[hour] += 1
                        whc_facebook_rx[weekday][hour] = 1
                        whlc_facebook_rx[weekday][hour] += 1
                    else:
                        hc_snapchat_rx[hour] = 1
                        hlc_snapchat_rx[hour] += 1
                        whc_snapchat_rx[weekday][hour] = 1
                        whlc_snapchat_rx[weekday][hour] += 1
                app_data[app_name][0] = row_value
            else:
                last_tx = app_data[app_name][1]
                if last_tx != None and last_tx != row_value:
                    if 'facebook' in app_data[app_name][2]:
                        hc_facebook_tx[hour] = 1
                        hlc_facebook_tx[hour] += 1
                        whc_facebook_tx[weekday][hour] = 1
                        whlc_facebook_tx[weekday][hour] += 1
                    else:
                        hc_snapchat_tx[hour] = 1
                        hlc_snapchat_tx[hour] += 1
                        whc_snapchat_tx[weekday][hour] = 1
                        whlc_snapchat_tx[weekday][hour] += 1
                app_data[app_name][1] = row_value
        elif row_entry_type.startswith('app|installed'):
            for app_entry in row.Value.split(','):
                temp_name =  app_entry.split('@')[0]
                if temp_name in apps_to_parse:
                    app_info = app_entry.split('@')[1].split(':')
                    temp_app_id = app_info[len(app_info) - 2]
                    if temp_name not in current_app_name_id_mapping:
                        app_data[temp_name] = [None, None, temp_name]
                    current_app_name_id_mapping[temp_name] = temp_app_id


    hdc_facebook_rx = [x + y for x, y in zip(hdc_facebook_rx, hc_facebook_rx)]
    hdc_facebook_tx = [x + y for x, y in zip(hdc_facebook_tx, hc_facebook_tx)]
    hdc_snapchat_rx = [x + y for x, y in zip(hdc_snapchat_rx, hc_snapchat_rx)]
    hdc_snapchat_tx = [x + y for x, y in zip(hdc_snapchat_tx, hc_snapchat_tx)]

    whdc_facebook_rx = [x + y for x, y in zip(whdc_facebook_rx, whc_facebook_rx)]
    whdc_facebook_tx = [x + y for x, y in zip(whdc_facebook_tx, whc_facebook_tx)]
    whdc_snapchat_rx = [x + y for x, y in zip(whdc_snapchat_rx, whc_snapchat_rx)]
    whdc_snapchat_tx = [x + y for x, y in zip(whdc_snapchat_tx, whc_snapchat_tx)]

if __name__ == '__main__':
    # Hourly device count for Facebook rx and tx logs
    hdc_facebook_rx = np.zeros(24)
    hdc_facebook_tx = np.zeros(24)
    # Hourly total logs count for Facebook rx and tx logs
    hlc_facebook_rx = np.zeros(24)
    hlc_facebook_tx = np.zeros(24)

    # Hourly device count for snapchat rx and tx logs
    hdc_snapchat_rx = np.zeros(24)
    hdc_snapchat_tx = np.zeros(24)
    # Hourly total logs count for snapchat rx and tx logs
    hlc_snapchat_rx = np.zeros(24)
    hlc_snapchat_tx = np.zeros(24)

    # Days hourly device count for Facebook rx and tx logs
    whdc_facebook_rx = np.zeros((7, 24))
    whdc_facebook_tx = np.zeros((7, 24))
    # Days hourly total logs count for Facebook rx and tx logs
    whlc_facebook_rx = np.zeros((7, 24))
    whlc_facebook_tx = np.zeros((7, 24))

    # Days hourly device count for snapchat rx and tx logs
    whdc_snapchat_rx = np.zeros((7, 24))
    whdc_snapchat_tx = np.zeros((7, 24))
    # Days hourly total logs count for snapchat rx and tx logs
    whlc_snapchat_rx = np.zeros((7, 24))
    whlc_snapchat_tx = np.zeros((7, 24))

    pathOfIdsFile = sys.argv[1]
    pathOfFiles = sys.argv[2]
    lancs = bool(len(sys.argv) > 3)

    startTime = datetime.now()

    for file in (read_file_names_lancs(pathOfIdsFile) if lancs else read_file_names(pathOfIdsFile)):
        fname = file.FileName
        fullfpath = pathOfFiles + file.FileName + '.csv'
        if not lancs:
            fullfpath = fullfpath + '.gz'
        print("Parsing file: " + fname)
        count_hourly_app_data_logs(fullfpath, lancs)

    with open('out_device_count_hours_days.csv', 'w') as f:
        hourly_fb_output = ('HOURLY DEVICE COUNT FOR FACEBOOK RX: \n{0}\n'.format(hdc_facebook_rx)
        + 'HOURLY DEVICE COUNT FOR FACEBOOK TX: \n{0}\n'.format(hdc_facebook_tx)
        + 'HOURLY TOTAL NO. LOGS FOR FACEBOOK RX: \n{0}\n'.format(hlc_facebook_rx)
        + 'HOURLY TOTAL NO. LOGS FOR FACEBOOK TX: \n{0}\n'.format(hlc_facebook_tx))

        f.write('FACEBOOK: \n{0}DAY HOURLY DEVICE COUNT FOR FACEBOOK RX (DAY: HOURLY DEVICES): \n'.format(hourly_fb_output))
        for x in range(0,7):
            f.write('{0}: {1}\n'.format(x, whdc_facebook_rx[x]))
        f.write('DAY HOURLY DEVICE COUNT FOR FACEBOOK TX (DAY: HOURLY DEVICES): \n')
        for x in range(0,7):
            f.write('{0}: {1}\n'.format(x, whdc_facebook_tx[x]))
        f.write('DAY HOURLY TOTAL NO. LOGS FOR FACEBOOK RX (DAY: NO. OF LOGS): \n')
        for x in range(0,7):
            f.write('{0}: {1}\n'.format(x, whlc_facebook_rx[x]))
        f.write('DAY HOURLY TOTAL NO. LOGS FOR FACEBOOK TX (DAY: NO. OF LOGS): \n')
        for x in range(0,7):
            f.write('{0}: {1}\n'.format(x, whlc_facebook_tx[x]))

        hourly_sc_output = ('HOURLY DEVICE COUNT FOR snapchat RX: \n{0}\n'.format(hdc_snapchat_rx)
        + 'HOURLY DEVICE COUNT FOR snapchat TX: \n{0}\n'.format(hdc_snapchat_tx)
        + 'HOURLY TOTAL NO. LOGS FOR snapchat RX: \n{0}\n'.format(hlc_snapchat_rx)
        + 'HOURLY TOTAL NO. LOGS FOR snapchat TX: \n{0}\n'.format(hlc_snapchat_tx))

        f.write('\nSNAPCHAT: \n{0}DAY HOURLY DEVICE COUNT FOR SNAPCHAT RX (DAY: HOURLY DEVICES): \n'.format(hourly_sc_output))
        for x in range(0,7):
            f.write('{0}: {1}\n'.format(x, whdc_snapchat_rx[x]))
        f.write('DAY HOURLY DEVICE COUNT FOR SNAPCHAT TX (DAY: HOURLY DEVICES): \n')
        for x in range(0,7):
            f.write('{0}: {1}\n'.format(x, whdc_snapchat_tx[x]))
        f.write('DAY HOURLY TOTAL NO. LOGS FOR SNAPCHAT RX (DAY: NO. OF LOGS): \n')
        for x in range(0,7):
            f.write('{0}: {1}\n'.format(x, whlc_snapchat_rx[x]))
        f.write('DAY HOURLY TOTAL NO. LOGS FOR SNAPCHAT TX (DAY: NO. OF LOGS): \n')
        for x in range(0,7):
            f.write('{0}: {1}\n'.format(x, whlc_snapchat_tx[x]))

    # **** For checking timings *****
    endFilesTime = datetime.now()
    print("All files summarised in {0}".format(str((endFilesTime - startTime))))
