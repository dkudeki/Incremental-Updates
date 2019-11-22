import os, sys, csv, json
import multiprocessing as mp
#from itertools import repeat
#from functools import wraps

#def unpack(func):
#	@wraps(func)
#	def wrapper(arg_tuple):
#		return func(*arg_tuple)
#	return wrapper
#
#@unpack
def populateChangeLists(jsonl_file,parent_folder,changes,q):
	results = {}
	for group in changes:
		results[group] = []

	with open(parent_folder + '/' + jsonl_file,'r') as read_file:
		records = read_file.readlines()
		for record in records:
			dict_record = json.loads(record)
			for field in dict_record['fields']:
				if '974' in field:
					for subfield in field['974']['subfields']:
						if 'u' in subfield:
							if subfield['u'] in changes['added']:
								results['added'].append(record)
							elif subfield['u'] in changes['changed']:
								results['changed'].append(record)
							else:
								pass

#	print("populateChangeLists results:")
#	print(results)
	q.put(results)
	return results

def listener(q,outfile_names):
	print("listener check-in:")
	outfiles = {}
	for o in outfile_names:
		outfiles[o] = open(outfile_names[o],'w')
		print(o)
		print(outfile_names[o])

	while(1):
		results = q.get()
		if results == 'kill':
			break

#		print("listener results:")
#		print(results)

		for group in results:
			if group != 'removed' and results[group]:
				print(group)
				print(results[group])
				print(outfiles[group])
				for line in results[group]:
					print(line)
					outfiles[group].write(line)

	for file in outfiles:
		outfiles[file].close()

def processFiles(marcjson_folder,changes,outfiles,core_count):
	manager = mp.Manager()
	q = manager.Queue()
	p = mp.Pool(int(core_count))

	print("processFiles check-in:")
	for o in outfiles:
		print(o)
		print(outfiles[o])

	watcher = p.apply_async(listener, (q,outfiles))

	jobs = []
	for root, dirs, files in os.walk(marcjson_folder):
		for f in files:
			print(f)
			if f[0] != '.':
				job = p.apply_async(populateChangeLists,(f,marcjson_folder,changes,q))
				jobs.append(job)

	for job in jobs:
		job.get()

	q.put('kill')
	p.close()
	p.join()

def gatherChangedRecords(changes_folder,marcjson_folder,core_count):
	changes = {}
	for root, dirs, files in os.walk(changes_folder):
		for f in files:
			if f[-4:] == '.csv':
				label = f[f.find('_')+1:-4]
				changes[label] = []
				with open(root + '/' + f,'r') as infile:
					reader = csv.reader(infile)
					for row in reader:
						changes[label].append(row[0])

	outfiles = {}
	for group in changes:
		outfiles[group] = changes_folder + '/' + group + '.jsonl'
		print(group)
		print(outfiles[group])

	processFiles(marcjson_folder,changes,outfiles,core_count)

#	for g in outfiles:
#		print(g)
#		print(outfiles[g])
#		print("Closing ^^^^")
#		outfiles[g].close()

if __name__ == "__main__":
	gatherChangedRecords(sys.argv[1],sys.argv[2],sys.argv[3])