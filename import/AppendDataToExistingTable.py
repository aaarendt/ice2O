'''
   script to import new data to the ice2O database
'''

# external modules
import pandas as pd
import numpy as np

# internal modules
import DbImport 
from settings import *

#Connect to the database
cs = AWS_localhost_sandbox
engine = DbImport.startEngine(cs)

inPath = ingest_names["pth"]

#Read in table for upload
df=pd.read_csv(inPath)
#Specify the table in the database to which it will be appended.
db_table = ingest_names["appendToTable"]

#Query database for column name of table primary key, and type (e.g. bigint, string, etc.)
res=DbImport.pkey_NameAndType(db_table, engine)
pkey=res['attname'][0] #name of primary key
pkey_type=res['data_type'][0] #type of primary key

#Check that primary key ID is numeric, and add sequential integers to fill (beginning at max(pkey in table) +1)
if pkey_type in ['smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision', 'smallserial', 'serial', 'bigserial']:
   print ("Primary Key = Numeric \nAdding the primary key and unique IDs to rows of table being appended")
   df=DbImport.add_sequential_IDs_to_pkey(df, db_table, engine)
else:
   print("Primary ID is not Numeric; must be updated manually")
   
#Extract format from existing table
types=DbImport.define_db_table_format(db_table, engine)

#Check to see if the columns in the new data frame match the ones in the database (order not important)
columns_match=set(list(types['attname'])) ==set(list(df))

#Create connection to the sandbox (where table will be uploaded)
cs = AWS_localhost_sandbox
engine = DbImport.startEngine(cs)

#Add new table
#APPENDING to existing table will automatically keep the column types intact!
if columns_match==True:
    df.to_sql(name=db_table, con=engine, index = False, if_exists='append')
else: print("ERROR: columns in uploaded table do not match those in DB")



