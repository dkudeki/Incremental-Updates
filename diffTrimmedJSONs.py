import os, sys, json, csv, datetime
from multiprocessing import Pool, Manager, Queue
from itertools import repeat
from datetime import timedelta

def findRecordFromHTID(changed_records,record_folder,file,q):
	with open(record_folder + '/' + file,'r') as readfile:
		record_segment = readfile.readlines()
		for record in record_segment:
			dict_record = json.loads(record)
			for field in dict_record['fields']:
				if '974' in field:
					for subfield in field['974']['subfields']:
						if 'u' in subfield:
							htid = subfield['u']
							if htid in changed_records:
								q.put((htid,dict_record))

def listener(q,output_file):
	with open(output_file,'a') as outfile:
		outfile.write('{\n')
		while 1:
			m = q.get()
#			print("Got Message")
			if m == 'kill':
				outfile.write('}')
				break

#			print(m[0])
#			print(m[1])
			outfile.write('"' + m[0] + '": ' + json.dumps(m[1]) + ',\n')
			outfile.flush()

def diffTrimmedJSONs(records_changed,old_records,new_records):
	start_time = datetime.datetime.now().time()

	changed_records = []
	with open(records_changed,'r') as readfile:
		record_id_reader = csv.reader(readfile)
		for row in record_id_reader:
			changed_records.append(row[0])
	
	print(changed_records)

	manager = Manager()
	q = manager.Queue()
	p = Pool(4)

	watcher = p.apply_async(listener, (q,'old_records_found.json'))

	for root, dirs, files in os.walk(old_records):
		results = p.starmap(findRecordFromHTID,zip(repeat(changed_records),repeat(old_records),[f for f in files if f[0] != '.'],repeat(q)))

	q.put('kill')

	end_time = datetime.datetime.now().time()
	print("Start time: " + str(start_time))
	print("End time: " + str(end_time))
	print("Run duration: " + str(datetime.datetime.combine(datetime.date.min,end_time)-datetime.datetime.combine(datetime.date.min,start_time)))
#	for root, dirs, files in os.walk(old_records):
#		for file in files:
#			if file[0] != '.':
#				with open(old_records + '/' + file,'r') as old_readfile:
#					old_record_segment = old_readfile.readlines()
#					for old_record in old_record_segment:
#						dict_record = json.loads(old_record)
#						for field in dict_record['fields']:
#							if '974' in field:
#								for subfield in field['974']['subfields']:
#									if 'u' in subfield:
#										old_records_found[subfield['u']] = dict_record

diffTrimmedJSONs(sys.argv[1],sys.argv[2],sys.argv[3])