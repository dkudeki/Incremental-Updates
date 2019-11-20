import os, sys, json, csv

def compareChecksumLists(old_checksums,new_checksums):
	with open(old_checksums,'r') as old_file:
		old_dict = json.load(old_file)

	with open(new_checksums,'r') as new_file:
		new_dict = json.load(new_file)

	old_set = set(old_dict.keys())
	new_set = set(new_dict.keys())

	try:
		os.mkdir('metadata_changes')
	except:
		pass

	records_removed = old_set - new_set
	print("Number of records removed:\t" + str(len(records_removed)))

	with open('metadata_changes/records_removed.csv','w') as outfile:
		writer = csv.writer(outfile)
		for item in records_removed:
			writer.writerow([item])

	records_added = new_set - old_set
	print("Number of records added:\t" + str(len(records_added)))

	with open('metadata_changes/records_added.csv','w') as outfile:
		writer = csv.writer(outfile)
		for item in records_added:
			writer.writerow([item])

	records_in_common = old_set & new_set
	print("Number of records in common:\t" + str(len(records_in_common)))

	changes = []
	for htid in records_in_common:
		if old_dict[htid] != new_dict[htid]:
			changes.append(htid)

	print("Number of records changed:\t" + str(len(changes)))

	with open('metadata_changes/records_changed.csv','w') as outfile:
		writer = csv.writer(outfile)
		for item in changes:
			writer.writerow([item])

if __name__ == "__main__":
	compareChecksumLists(sys.argv[1],sys.argv[2])