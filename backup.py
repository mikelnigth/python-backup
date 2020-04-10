#
#	Author: Miquel Servera
#   Description:	The script must be executed with the function as paramether:
#						- full	Full			copy all the files
#						- inc	Incremental		copy new and modified files since last full backup
#						- diff	Differential	copy new and modified files since last backup
#   	            The script is configured with the file config.json
#	Python version: 3.7
#
#   ----- Libraries -----
#
import socket
import datetime
import time
import sys
import os
import os.path
import shutil
import json
import zipfile
import tarfile
#
#   ----- Useful Functions -----
def list_list(lista):
	for i in lista:
		print(i)
#
#   ----- Other Functions -----
def check_terminal():							# Check the paramthers introduced via terminal
	errors = 0
	if (len(sys.argv) != 2):
		print("[ERROR] Function not introduced")
		exit()
	function = sys.argv[1]
	if (function != "full") and (function != "inc") and (function != "diff"):
		print("[ERROR] Function not defined")
		errors = errors + 1
	if (errors != 0):
		exit()
	else:
		return function
#
def check_configuration():										# Check files necessaries in the script folder
	errors = 0
	# File Configuration
	config_file = os.path.dirname(os.path.abspath(__file__)) + "/config.json"
	if (os.path.exists(config_file) == False):
		print("[ERROR] Configuration file not found")
		exit()
	with open (config_file) as json_config:
		config = json.load(json_config)
		source_path = config['configuration'][0]['source']
		destination_path = config['configuration'][0]['destination']
		database_path = config['configuration'][0]['database']
		database_template_path = config['configuration'][0]['database_template']
		exceptions_list_path = config['configuration'][0]['exceptions']
		logs_path = config['configuration'][0]['logs']
	# Folder Source
	if (os.path.exists(source_path) == False):
		print("[ERROR] Source folder not exist")
		errors = errors + 1
	# Folder Destination
	if (os.path.exists(destination_path) == False):
		print("[ERROR] Destination folder not exist")
		errors = errors + 1
	# File Database
	if (os.path.exists(database_path) == False):
		print("[WARNING] Database file not exist, will be created a new one in full option")
	# Logs file
	if (os.path.exists(logs_path) == False):
		print("[WARNING] Logs file not exist, will be created a new one in inc or diff option")
	# File Database template
	if (os.path.exists(database_template_path) == False):
		print("[ERROR] Database template file not exist")
		errors = errors + 1
	# File Exceptions
	if (os.path.exists(exceptions_list_path) == False):
		print("[ERROR] Exceptions file not exist")
		errors = errors + 1
	if (errors != 0):
		exit()
	else:
		return source_path,destination_path,database_path,database_template_path,exceptions_list_path,logs_path
#
def check_item(element):
	n = 2
	if (os.path.exists(element) == True):
		while (os.path.exists(element + "_" + str(n)) == True):
			n = n + 1
		element = element + "_" + str(n)
	return element
#
def check_exceptions(item):
	coincidences = 0
	for i in list_exceptions:
		if (i in item) == True:
			coincidences = coincidences + 1
	if coincidences == 0:
		return False
	if coincidences != 0:
		return True
#
def load_database():
	with open (database_path) as database_input:
		database = json.load(database_input)
	return database
#
def load_exceptions():
	with open (exceptions_list_path) as exceptions_input:
		exceptions = json.load(exceptions_input)
		for i in exceptions['exceptions']:
			list_exceptions.append(i)
	return
#
def write_database(database):
	# Information
	database['information'][0]["Host"] = socket.gethostname()
	database['information'][0]["Files"] = len(database['files'])
	i = 0
	size = 0
	while i < len(database['files']):
		size = size + database['files'][i]["size"]
		i = i + 1
	database['information'][0]["Size"] = size
	# Bot info
	database['bot_info'][0]["general"][0]["backup"] = folder_destination.replace(os.path.dirname(os.path.abspath(folder_destination)) + "/",'')
	database['bot_info'][0]["general"][0]["initial_run_s "] = initial_time
	database['bot_info'][0]["general"][0]["finished_run_s"] = str(datetime.datetime.now().strftime("%Y-%m-%d   %H:%M"))
	database['bot_info'][0]["general"][0]["finished_run_i"] = int(datetime.datetime.now().strftime("%Y%m%d%H%M"))
	database['bot_info'][0][function][0]["backup"] = folder_destination.replace(os.path.dirname(os.path.abspath(folder_destination)) + "/",'')
	database['bot_info'][0][function][0]["initial_run_s "] = initial_time
	database['bot_info'][0][function][0]["finished_run_s"] = str(datetime.datetime.now().strftime("%Y-%m-%d   %H:%M"))
	database['bot_info'][0][function][0]["finished_run_i"] = int(datetime.datetime.now().strftime("%Y%m%d%H%M"))
	# write Database
	with open(database_path, 'w') as database_output:
		json.dump(database, database_output, indent=2)
	return
#
def write_logs(list_files_new,list_files_modified):
	logs = open(logs_path,'a+')
	logs.write("                              \n")
	logs.write("                              \n")
	logs.write("                              \n")
	logs.write("------------------------------\n")
	logs.write(str(datetime.datetime.now().strftime("%Y-%m-%d   %H:%M\n")))
	logs.write("------------------------------\n")
	logs.write("------- List new files -------\n")
	logs.write("Files: " + str(len(list_files_new)) + "\n")
	for i in list_files_new:
		logs.write(i + "\n")
	logs.write("------------------------------\n")
	logs.write("----- List modified files -----\n")
	logs.write("Files: " + str(len(list_files_modified)) + "\n")
	for i in list_files_modified:
		logs.write(i + "\n")
	return
#
def insert_database(database,backup_list):
	for i in backup_list:
		database['files'].append({
			"name" : i.replace(os.path.dirname(os.path.abspath(i)) + "/",''),
			"size" : os.stat(i).st_size,
			"backup" : folder_destination.replace(os.path.dirname(os.path.abspath(folder_destination)) + "/",''),
			"backup_s": str(datetime.datetime.now().strftime("%Y-%m-%d   %H:%M")),
			"backup_i" : int(datetime.datetime.now().strftime("%Y%m%d%H%M")),
			"path": i
		})
	return
#
def edite_database(database,i,index_file):
	database['files'][index_file]["size"] = os.stat(i).st_size
	database['files'][index_file]["backup"] = folder_destination.replace(os.path.dirname(os.path.abspath(folder_destination)) + "/",'')
	database['files'][index_file]["backup_s"] = str(datetime.datetime.now().strftime("%Y-%m-%d   %H:%M"))
	database['files'][index_file]["backup_i"] = int(datetime.datetime.now().strftime("%Y%m%d%H%M"))
	return
#
def do_backup(backup_list):
	with tarfile.open(str(folder_destination), "w") as tar:
		for i in backup_list:
			tar.add(i)
	return
#
#
#
def option_full():		# Backup all files
	# Backup database
	database_backup_path = database_path + "_backup_" + str(date)
	database_backup_path = check_item(database_backup_path)
	shutil.copy(database_path, database_backup_path)
	# Remove old database
	os.remove(database_path)
	# Copy new database from template
	shutil.copy(database_template_path, database_path)
	# Import database
	database = load_database()
	# Backup process
	do_backup(list_files_all)
	# Write database
	insert_database(database,list_files_all)
	write_database(database)
	return
#
def option_inc_diff():		# Backup new and modified files
	if (os.path.exists(database_path) == False):
		print("[ERROR] Database file not exist, mandatory first full backup")
		return
	# Import database
	database = load_database()
	# Definition of reference date
	# for function inc is the las FULL backup
	# for function diff is the las backup
	if (function == "inc"):
		reference_date = database['bot_info'][0]["full"][0]["finished_run_i"]
	if (function == "diff"):
		reference_date = database['bot_info'][0]["general"][0]["finished_run_i"]
	# List new files (not registered in the database)
	# and
	# List modified files (reference: date in the database)
	for index_all in list_files_all:
		i = 0
		is_new = True
		while i < len(database['files']):
			if (index_all == database['files'][i]['path']):
				# File is in the backup
				is_new = False
				if (int(time.strftime("%Y%m%d%H%M", time.localtime(os.path.getmtime(index_all)))) > reference_date):
					# File has been updated
					list_files_modified.append(index_all)
					edite_database(database,index_all,i)
			i = i + 1
		# File is not in the backup, then is new
		if (is_new == True):
			list_files_new.append(index_all)
	print("----- List new files -----")
	insert_database(database,list_files_new)
	list_list(list_files_new)
	print("----- List updated files -----")
	list_list(list_files_modified)
	# Merge list_files_new and list_files_modified to one
	backup_list = []
	for i in list_files_modified:
		backup_list.append(i)
	for i in list_files_new:
		backup_list.append(i)
	if len(backup_list) != 0:
		do_backup(backup_list)
		write_database(database)
		write_logs(list_files_new,list_files_modified)
	return
#
#   ----- Main Function -----
#
if __name__ == "__main__":
	function = check_terminal()
	source_path,destination_path,database_path,database_template_path,exceptions_list_path,logs_path = check_configuration()
	host = socket.gethostname()
	date = datetime.date.today()
	initial_time = str(datetime.datetime.now().strftime("%Y-%m-%d   %H:%M"))
	# Create destination folder
	folder_destination = check_item(destination_path + str(date) + "---" + str(function) + ".tar")
	# List  that will be used
	list_files_all = []			# Contain all the files (in path format) except the exceptions
	list_exceptions = []		# Contain all the exceptions loaded (in path format) 
	list_files_new = []			# Contain new files since gived date (in path format) 
	list_files_modified = []	# Contain edited files since gived date (in path format) 
	load_exceptions()
	for r, d, f in os.walk(source_path):
		for item in f:
			if (check_exceptions(os.path.join(r, item)) == False):
				list_files_all.append(os.path.join(r, item))
	if (function == "full"):
		option_full()
	if (function == "inc") or (function == "diff"):
		option_inc_diff()
	exit()




#	print(time.strftime("%Y%m%d%H%M", time.localtime(os.path.getmtime(i))))
#print(os.path.abspath(__file__))
#print(os.path.dirname(os.path.abspath(__file__)))