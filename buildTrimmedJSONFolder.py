import os, sys, json, csv, datetime, shutil
from multiprocessing import Pool
from itertools import repeat
from datetime import timedelta

def saveSegment(record_folder,file,output_folder):
	print(file)
	with open(record_folder + '/' + file,'r') as readfile:
		record_segment = readfile.readlines()
		for record in record_segment:
			dict_record = json.loads(record)
			for field in dict_record['fields']:
				if '974' in field:
					for subfield in field['974']['subfields']:
						if 'u' in subfield:
							htid = subfield['u']
							with open(output_folder + '/' + htid + '.json','w') as outfile:
								json.dump(dict_record,outfile)

def buildTrimmedJSONFolder(old_records,output_folder):
	if os.path.exists(output_folder):
		shutil.rmtree(output_folder)

	os.mkdir(output_folder)

	start_time = datetime.datetime.now().time()

	p = Pool(4)

	for root, dirs, files in os.walk(old_records):
		p.starmap(saveSegment,zip(repeat(old_records),[f for f in files if f[0] != '.'],repeat(output_folder)))

	end_time = datetime.datetime.now().time()
	print("Start time: " + str(start_time))
	print("End time: " + str(end_time))
	print("Run duration: " + str(datetime.datetime.combine(datetime.date.min,end_time)-datetime.datetime.combine(datetime.date.min,start_time)))

buildTrimmedJSONFolder(sys.argv[1],sys.argv[2])