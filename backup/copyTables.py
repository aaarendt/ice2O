'''
	This script copies tables between databases using postgres command line. The excel table contains two columns: table name and a 0 or 1 indicating which table will be copied.
	Column with table names is named 'table', column with 0 or 1 is 'update'.
'''

#External Modules
import os
import pandas as pd
import sys

# internal modules
thisfilelocation=os.getcwd() #path for this file
utils_folder=os.path.join(os.path.dirname(thisfilelocation), "utils")
top_folder=os.path.join(os.path.dirname(thisfilelocation))
sys.path.append(utils_folder) #add a folder one level up to the system path; this is where modules are stored
sys.path.append(top_folder)
from utils import DbImport
from settings import *

# FROM #
user = AWS_localhost['user']
fromdb = AWS_localhost['dbname']
fromHost = AWS_localhost['host']
fromPort = AWS_localhost['port']

# TO #
todb = AWS_backup['dbname']
toHost = AWS_backup['host']
toPort = AWS_backup['port']

# Import list of tables; 2 columns. Column @update will read '1' if to be updated.
tableList = pd.read_excel(os.path.join(top_folder, tableList_nm)) #tableList_nm defined in settings file

for index, row in tableList.iterrows():
    if row['update']:
        print (row['table'])
        commandString = "pg_dump -C -h " + fromHost + " -U " + user + " -t " + row['table'] + " -p " + fromPort +  \
                    " " + fromdb + " | psql -h " + toHost + " -p " + toPort + " -U " + user + " " + todb
        os.system(commandString)
