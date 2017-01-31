"""
PURPOSE
------------------------------------------------------------------------------
Provide a variety of functions to facilitate easy import of external data to the Ice2O Cloud-hosted postgres database.

CREATION
------------------------------------------------------------------------------
Created by Emily Baker, 2017
"""

#Modules loaded internally with 'import DbImport' (this an Emily-defined module)
import os, sys
sys.path.append("C:\Users\ehbaker\Documents\Python\Modules")
import DbImport
import pandas as pd
import numpy as np
import glob 
import psycopg2
from sqlalchemy import create_engine
from geopandas import GeoSeries, GeoDataFrame

def pkey_NameAndType(db_table, engine):
	"""
	Function to return the primary key of a given table (db_name) in a postgres database.
	db_table: the name of the table you are looking attname
	engine: connection to database, initiated with create_engine inside sqlalchemy
	"""
	##Find primary key of the table
	##################################
	query ="""SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
	FROM   pg_index i
	JOIN   pg_attribute a ON a.attrelid = i.indrelid
						 AND a.attnum = ANY(i.indkey)
	WHERE  i.indrelid = """ + "'" + db_table+ "'" + """::regclass
	AND    i.indisprimary;"""
	res=pd.read_sql(query, engine)
	pkey=res['attname'][0]
	print("Primary key for " +db_table+ " is: " + pkey)
	return (res)	

def add_sequential_IDs_to_pkey(df, db_table, engine):
	"""
	Take a given pandas dataframe, see if it has a column with the primary key given as input
	"""
	#df, db_table engine are passsed into new function
	res=pkey_NameAndType(db_table, engine)
	#Find the maximum value of the numeric Primary Key (pkey) in the table already in database (db_table)
	pkey=res['attname'][0]
	pkey_type=res['data_type'][0]
	max_pkey_df=pd.read_sql("SELECT MAX(" + pkey+ ") FROM " + db_table, engine)
	max_pkey=max_pkey_df.iloc[0,0]
	#Set the primary ID column in incoming table to be sequentially increasing integer, begining where ID in current table ends.
	new_pkeys=range(max_pkey +1, max_pkey+ 1 + df.shape[0]) #Create list of subsequent integers to add to primary key column
	df[pkey]=new_pkeys
	return(df)

def define_db_table_format(df, db_table, engine):
    """
    For a given table in the database, return a dictionary with column_name:type. This can then be passed back to the table, for appending rows to an existing table.
    """
    #Find column name and type for exisitng DB table
    query="""SELECT attname, format_type(atttypid, atttypmod) AS type
     FROM   pg_attribute
     WHERE  attrelid = '%s'::regclass
     AND    attnum > 0
     AND    NOT attisdropped
     ORDER  BY attnum;""" %(db_table)
    types=pd.read_sql(query, engine)
    
    #Create a dictionary, which can be passed during table upload in df.to_sql()
    #types_dict=dict(zip(types.attname, types.type))
    
    #Check to see if the columns in the new data frame match the ones in the database
    #columns_match=set(list(tab_format['attname'])) ==set(list(df))
    
    #if (columns_match) == T:
        #print ('columns match')
    #else:
        #print("ERROR: Columns in data frame and database DO NOT match")
    
    return(types)

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
