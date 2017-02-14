'''
	This script copies tables between databases using postgres command line. The excel table contains two columns: table name and a 0 or 1 indicating which table will be copied.
	Column with table names is named 'table', column with 0 or 1 is 'update'.
'''

#External Modules
import os
import pandas as pd
import settings

user = AWS_localhost['user']

# internal modules
import DbImport 
from settings import *

# FROM #
fromdb = AWS_localhost['dbname']
fromHost = AWS_localhost['host']
fromPort = AWS_localhost['port']

# TO #
todb = AWS_backup['dbname']
toHost = AWS_backup['host']
toPort = AWS_backup['port']

tableList = pd.read_excelpth_tableList)

for index, row in tableList.iterrows():
    if row['update']:
        commandString = "pg_dump -C -h " + fromHost + " -U " + user + " -t " + row['table'] + " -p " + fromPort +  \
                    " " + fromdb + " | psql -h " + toHost + " -p " + toPort + " -U " + user + " " + todb
        os.system(commandString)
