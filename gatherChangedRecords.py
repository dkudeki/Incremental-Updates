import os, sys, csv, json

def gatherChangedRecords(changes_folder,marcjson_folder):
	changes = {}
	for root, dirs, files in os.walk(changes_folder):
		for f in files:
			label = f.find('_')+1:-4
			changes[label] = []
			with open(root + f,'r') as infile:
				reader = csv.reader(infile)
				changes[label] = list(reader)

	with open('output.json','w') as outfile:
		json.dump(changes,outfile)


if __name__ == "__main__":
	gatherChangedRecords(sys.argv[1],sys.argv[2])