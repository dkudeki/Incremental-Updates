import os, sys, csv

def gatherChangedRecords(changes_folder,marcjson_folder):
	changes = {}
	for root, dirs, files in os.walk(changes_folder):
		for f in files:
			label = f[f.find('_')+1:-4]
			changes[label] = []
			with open(root + f,'r') as infile:
				reader = csv.reader(infile)
				for row in reader:
					changes[label].append(row[0])

	outfiles = {}
	for group in changes:
		outfiles[group] = open(changes_folder + '/' + group + '.jsonl','w')


	for g in outfiles:
		outfiles[g].close()

if __name__ == "__main__":
	gatherChangedRecords(sys.argv[1],sys.argv[2])