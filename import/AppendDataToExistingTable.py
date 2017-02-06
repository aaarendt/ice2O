import sys
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
sys.path.append("C:/Users/ehbaker/Documents/Python/Repos/ice2O") #Path to where DBImport.py is saved
import DbImport #This is a module that I have written, stored one directory up (cd ..)
import numpy as np
import settings

#User-supplied criteria:
pth=(r"Q:\Project Data\GlacierData\WOLVERINE\Draw_Wire\data\processed\draw_wire_database.csv") #path to csv for upload
db_table='draw_wire' #name of table in the database which you want to copy
sandbox_tab_name='draw_wire_ingest' #Name of table in database you want to append to. Data will match existing column types.

#Connect to the database
cs=settings.import_cs()
engine = create_engine('postgresql://' + cs['user'] + ':' + str(cs['password']) + '@' + cs['host'] + ':' + cs['port'] + '/' + cs['dbname'])

#Read in table for upload
df=pd.read_csv(r"Q:\Project Data\GlacierData\WOLVERINE\Draw_Wire\data\processed\draw_wire_database.csv")
#Specify the table in the database to which it will be appended.
db_table='draw_wire'

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
engine_sand = create_engine('postgresql://' + cs['user'] + ':' + str(cs['password']) + '@' + cs['host'] + ':' + cs['port'] + '/' + 'sandbox')

#Add new table
#APPENDING to existing table will automatically keep the column types intact!
if columns_match==True:
    df.to_sql(name=sandbox_tab_name, con=engine_sand, index = False, if_exists='append')
else: print("ERROR: columns in uploaded table do not match those in DB")



