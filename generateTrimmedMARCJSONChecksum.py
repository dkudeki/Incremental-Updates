import os, sys, json, hashlib, datetime
from multiprocessing import Pool
from itertools import repeat
from datetime import timedelta

def generateMARCJSONChecksum(jsonl_file,parent_folder):
	checksums = {}

	print(jsonl_file)

	with open(parent_folder + '/' + jsonl_file,'r') as read_file:
		records = read_file.readlines()
		for record in records:
			m = hashlib.md5()
			dict_record = json.loads(record)
			for field in dict_record['fields']:
				if '974' in field:
					for subfield in field['974']['subfields']:
						if 'u' in subfield:
							htid = subfield['u']

			m.update(record.encode('utf-8'))
			checksums[htid] = m.hexdigest()

	return(checksums)

def processMARCJSON(jsonl_folder,core_count):
	start_time = datetime.datetime.now().time()

	p = Pool(core_count)
	for root, dirs, files in os.walk(jsonl_folder):
		print(files)
		results = p.starmap(generateMARCJSONChecksum,zip([f for f in files if f[0] != '.'],repeat(jsonl_folder)))

	output = {}
	for result in results:
		output = {**output, **result}

	with open(jsonl_folder + '_checksums.json','w') as output_file:
		json.dump(output,output_file)

	end_time = datetime.datetime.now().time()
	print("Start time: " + str(start_time))
	print("End time: " + str(end_time))
	print("Run duration: " + str(datetime.datetime.combine(datetime.date.min,end_time)-datetime.datetime.combine(datetime.date.min,start_time)))

if __name__ == "__main__":
	processMARCJSON(sys.argv[1],int(sys.argv[2]))