all_data_foreground.py (**** No longer required ****)
Description:
Outputs totals, means, no. of devices, mins, maxs and medians of devices hourly averages for all app foreground instances and all data demand rx and tx.
Args:
1. Device ids csv file
2. Path of device files
Output files:
1. total_out/all_totals_hourly.csv
2. total_out/all_means_hourly.csv
3. total_out/all_deviceNo_hourly.csv
4. total_out/all_mins_hourly.csv
5. total_out/all_maxs_hourly.csv
6. total_out/all_meds_hourly.csv
7. total_out/all_hourly.csv


app_use_time.py
Description:
Outputs totals, means, no. of devices, mins, maxs and medians of devices hourly averages for foreground app instances whilst the screen is on and unlocked or other. It also outputs the same calculations for devices hourly averages of use durations and no. of uses, i.e. time screen was on. App use is also summarised into practice use for both foreground app instances whilst the screen is on and unlocked or other
Args:
1. Device ids csv file
2. Path of device files
3. Greater50InstallsApps.csv
Output files:
1. use_out/device_use_hourly.csv
2. use_out/app_foreground_use_hourly.csv
3. use_out/app_use_totals_hourly.csv
4. use_out/app_use_means_hourly.csv
5. use_out/app_use_deviceNo_hourly.csv
6. use_out/app_use_mins_hourly.csv
7. use_out/app_use_maxs_hourly.csv
8. use_out/app_use_meds_hourly.csv
9. use_out/app_foreground_other_hourly.csv
10. use_out/app_other_totals_hourly.csv
11. use_out/app_other_means_hourly.csv
12. use_out/app_other_deviceNo_hourly.csv
13. use_out/app_other_mins_hourly.csv
14. use_out/app_other_maxs_hourly.csv
15. use_out/app_other_meds_hourly.csv
16. use_out/practice_hourly_use_summaries_foreground.csv
17. use_out/practice_hourly_use_summaries_other.csv


data_sms_phonecalls.py
Description:
Outputs totals, means, no. of devices, mins, maxs and medians of devices hourly averages for data demand rx and tx, SMS sent and received, no. of phone calls and duration of phone calls.
Args:
1. Device ids csv file
2. Path of device files
3. app-greater50-installs-on-devices-at-least-14-days.csv
Output files:
1. out/sms_summary.csv
2. out/phone_calls_summary.csv
3. out/app_hourly_summaries.csv
4. out/app_hourly_totals.csv
5. out/app_hourly_means.csv
6. out/app_hourly_devicesNo.csv
7. out/app_hourly_mins.csv
8. out/app_hourly_maxs.csv
9. out/app_hourly_meds.csv


overall_summary.py
Description:
Helps give an idea of what percentage of the dataset we're representing overall and per category with the Cambridge app list (apps installed on 50 devices or more).
Outputs the number of devices that contribute to: overall data, overall demand of categories, overall use of categories, demand of each category and use of each category (contribution, practice_demand_contribution, practice_use_contribution).
Also outputs hourly data summed for devices and categories (all_practice_data, all_practice_rx, all_practice_tx, all_practice_use).
Also outputs daily data summed for devices and categories with percentages (daily_practice_data, daily_practice_use).
Args:
1. Device ids csv file
2. Path of device files
3. Greater50InstallsApps.csv
Output files:
1. overall_summary/all_practice_data.csv
2. overall_summary/all_practice_rx.csv
3. overall_summary/all_practice_tx.csv
4. overall_summary/all_practice_use.csv
5. overall_summary/all_totals.csv
6. overall_summary/contribution.csv
7. overall_summary/practice_demand_contribution.csv
8. overall_summary/practice_use_contribution.csv
9. overall_summary/daily_practice_data.csv
10. overall_summary/daily_practice_use.csv

parse_everything.py
Description:
Same as overall_summary.py but also outputs sms and phone calls like data_sms_phonecalls.py.
Args:
1. Device ids csv file
2. Path of device files
3. Greater50InstallsApps.csv
Output files:
1. everything/all_practice_data.csv
2. everything/all_practice_rx.csv
3. everything/all_practice_tx.csv
4. everything/all_practice_use.csv
5. everything/all_totals.csv
6. everything/contribution.csv
7. everything/practice_demand_contribution.csv
8. everything/practice_use_contribution.csv
9. everything/daily_practice_data.csv
10. everything/daily_practice_use.csv
11. everything/sms_summary.csv
12. everything/phone_calls_summary.csv

practice_data_demand.py
Description:
Outputs hourly totals of data demand per practice, i.e. totals of hourly app averages based on devices hourly app averages.
Args:
1. Device ids csv file
2. Path of device files
3. Greater50InstallsApps.csv
Output files:
1. out/practice_hourly_summaries.csv
