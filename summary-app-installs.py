#!/usr/bin/env python

def app_names(fp):
	"""
	Give a file pointer fp, extract app names from a DA analysed/apps-installed file
	Assumes file is formated as 
	"time | <count> | CSV of app names, each name colon separated with perms"
	"""
	app_dict = {}
	for line in fp.readlines():
		items = line.split("|")
		if len(items) < 3:
			print "Error, expecting line with at least three items", line[:20]+"..."
			continue
		apps = items[2]
		for app in apps.split(","):
			if len(app) > 2:
				#string is of form "app_name:perms_list:market" since version 1.1.6 of DA
				#we'll simply ignore data from older versions of DA.
				app_details =  app.split(":")
				if len(app_details) < 3:
					continue
					
				fullname = app_details[0]
				name = fullname.split("@")[0]
				perms = app_details[1]
				market = app_details[-1]
				#for the moment, the last app status wins; does it matter if this changes over time?
				app_dict[name] = (market, "android.permission.INTERNET" in perms)
	return app_dict.items()

if __name__ == '__main__':
	"""
	Given a directory, process all files in it.
	"""
	import sys
	from os import listdir
	from os.path import isfile, join
	
	if len(sys.argv) != 3:
		print "Usage:", sys.argv[0], " <dir of device app files> <threshold>"
		sys.exit(1)
		
	app_count = {}
	filenames = [f for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f))]
	for filename in filenames:
		fp = open(join(sys.argv[1], filename), "rb")
		for app, (market, has_internet) in app_names(fp):
			#This is horrible; rework!
			if app_count.has_key(app):
				total, counters = app_count[app]
				if counters.has_key(market):
					counters[market] += 1
				else:
					counters[market] = 1
				if counters.has_key("Internet_"+str(has_internet)):
					counters["Internet_"+str(has_internet)] += 1
				else:
					counters["Internet_"+str(has_internet)] = 1
				app_count[app] = (total + 1, counters)
			else:
				app_count[app] = (1, {market: 1, "Internet_"+str(has_internet): 1})
	
	app_list = []
	for app, (count, detail) in app_count.items():
		app_list.append((count, app, detail))
	app_list.sort()
	app_list.reverse()
	
	for count, app, detail in app_list:
		if count >= int(sys.argv[2]):
			print app + ", " + str(count) + ",", detail
