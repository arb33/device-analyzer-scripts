#!/usr/bin/env python
#
# Find all devices which have traces which last more than five weeks
# and have a majority of samples inside the UK and Ireland
#
# Definition of UK and Ireland is a location in lat/lon range 
#  * top-right (60.5,1.5)
#  * bot-left  (50  ,-11)
#
# Useful site to work these out: http://www.darrinward.com/lat-long/
#
# Input is the processed DA data in a CSV of this form:
#
# 2013-08-23T10:16:38.111-0600|46.112863|-47.162376
	
if __name__=='__main__':
	import sys
	from time import mktime, strptime
	from os import listdir
	from os.path import isfile, join
	from datetime import date
	
	if len(sys.argv) != 2:
		print 'Usage: ', sys.argv[0], '<directory of files>'
		print 'returns a list of files which match, one per line'
		sys.exit(1)

	uk_lon_max = 1.5
	uk_lon_min = -11
	uk_lat_max = 60.5
	uk_lat_min = 50

	filenames = [f for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f))]
	device_count = 0
	
	for filename in filenames:
	
		device_count += 1
	
		#Determine range of dates and counts of sightings in/out of the uk
		dates_seen = {}		
		inside_uk = 0
		outside_uk = 0
		
		fp = open(join(sys.argv[1], filename), 'rb')
		for line in fp.readlines():
			elements = line.split('|')
			if len(elements) != 3:
				continue
			timestamp, lat, lon = elements
			#Would like to write date.strptime(timestamp, "%Y-%m-%d") but old python!
			t = date.fromtimestamp(mktime(strptime(timestamp.split('T')[0], '%Y-%m-%d')))
			lat = float(lat)
			lon = float(lon)

			dates_seen[t] = None			
			
			if lon < uk_lon_max and lon > uk_lon_min and \
			   lat < uk_lat_max and lat > uk_lat_min:
				inside_uk += 1
			else:
				outside_uk += 1

		#calculate proportion of days we have data for
		dates_list = dates_seen.keys()
		dates_list.sort()
		days_seen = len(dates_list)
		if days_seen < 35:
			continue #Not enough data for this device
		date_start = dates_list[0]
		date_end = dates_list[-1]
		prop_days_seen =  float(days_seen - 1) / (date_end - date_start).days		
		if date_end < date(2014, 1, 1):
			continue #Too old
		total = float(inside_uk + outside_uk)
		prop_inside_uk = (inside_uk / total)
		if prop_inside_uk < 0.5:
			continue #Outside the UK too much
						
		print device_count, filename,
		print date_start.strftime('%Y-%m-%d'), date_end.strftime('%Y-%m-%d'), 
		print days_seen, prop_days_seen, inside_uk, outside_uk, prop_inside_uk