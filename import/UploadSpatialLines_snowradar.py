'''
    script to import snowradar points and lines to the sandbox
    
    takes points fies (csv), makes lines
    geometry information must be in columns labeled 'lat', 'long'; geometry is created in column 'geom'
'''
######
# SETUP
######

#external modules
import sys
import os.path
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import numpy as np

#internal modules
thisfilelocation=os.getcwdu() #path for this file
sys.path.append(os.path.dirname(thisfilelocation)) #add a folder one level up to the system path; this is where modules are stored
import DbImport #User defined module in the folder added to path in line above.
from settings import *

#Read information on table and metadata location info for snow radar data
db_points_table=ingest_names_snowradar["copyFromTable"]#table with points in the db
db_lines_table=ingest_names_snowradar["copyFromTable_Lines"]#table with lines in the db
pth=ingest_names_snowradar["pth"] #path to csv of points for upload
pth_lines_meta=ingest_names_snowradar["pth_lines_meta"]

#Connect to the database
engine = DbImport.startEngine(AWS_localhost) #spatial_database
engine_sand = DbImport.startEngine(AWS_localhost_sandbox) #sandbox

#Read in csvs for upload
df_pts=pd.read_csv(pth) #read point data to dataframe
df_lin_meta=pd.read_csv(pth_lines_meta)

#Make name for table of points being uploaded
dbnamePts=db_points_table+'_ingest'
print(df_pts.shape)
print(dbnamePts + ': table of all points collected')

######
# ADD POINTS DATA TO DATABASE
######

#Remove any columns in the csv that should not be uploaded to the database
types=DbImport.define_db_table_format(db_points_table, engine)
cols_to_keep=list(set(list(df_pts)) -(set(list(df_pts)) -set(list(types['attname'])+ [u'long', u'lat', u'collection'])))
#Remove unwanted columns
df_pts=df_pts[cols_to_keep]
#Reorder columns to match order in DB, with the addition of lat/long, WITHOUT the geom column (will be created in SQL)
df_pts=df_pts[list(types[~types.attname.str.contains("geom")]['attname'])+[u'long', u'lat', u'collection']].copy() #copy neccesary to overwrite

#Add [a subset of] points dataframe to the DB
df_pts=df_pts.sample(3000) #subset of points, as a test to the sandbox
df_pts=df_pts.sort_values('trace') #sort by trace so that the lines are created sequentially, instead of random pattern.
df_pts.to_sql(dbnamePts, engine_sand, index=False, if_exists='replace')
print("Added all points as " + dbnamePts)

# create the geometry field
engine_sand.execute("""ALTER TABLE %s ADD COLUMN geom geometry(Point, 3338);""" %(dbnamePts)) 
# populate the geometry field
engine_sand.execute("""UPDATE %s SET geom = ST_Transform(ST_setSRID(ST_MakePoint(long,lat),4326),3338);""" %(dbnamePts))

######
# MAKE LINE
######

# Make the associated line file
dbnameLines= db_lines_table +'_temp_geometry'
engine_sand.execute("""CREATE TABLE %s (collection text, geom geometry(Linestring, 3338));""" %(dbnameLines))
query = ('WITH linecreation AS (SELECT collection, ST_MakeLine(geom) as geom FROM ' + dbnamePts + ' GROUP BY collection) INSERT INTO ' + dbnameLines + ' SELECT * FROM linecreation;')
engine_sand.execute(query)
print("Created EMPTY table: " + dbnameLines+ " ; to fill table, copy and paste text below directly into SQL window:")

print( '''

STOP! You must enter the following SQL query directly into Postgres; passing thru SQLAlchemy is not working.

After executing in Postgres, continue.

''')

#The last command is currently not workikng; must pass it in directly into SQL window. Copy and paste the text below:
engine_sand.execute(query)
print(query)

#Upload and join the metadata table to the geospatial lines table (already in the sandbox)
dbnameMeta = db_points_table +'_ingest_metadata'
df_lin_meta.to_sql(dbnameMeta, engine_sand, index=False, if_exists='replace')

#Use a join to create our desired table, from 2 initial uploads (points-> lines, and Metadata )
dbnameFinal= db_lines_table +'_ingest'
query= """CREATE TABLE %s AS
SELECT %s.collection, velocity, density, date, obs_type, geom
FROM %s LEFT OUTER JOIN %s 
ON (%s.collection =%s.collection)"""%(dbnameFinal,dbnameMeta, dbnameMeta, dbnameLines, dbnameMeta,dbnameLines)
engine_sand.execute(query)

######
# FORMAT LINE
######

#Primary Key
engine_sand.execute("""ALTER TABLE %s ADD COLUMN gid SERIAL;""" %(dbnameFinal))
engine_sand.execute("""UPDATE %s SET gid = nextval(pg_get_serial_sequence('%s','gid'));""" %(dbnameFinal, dbnameFinal))
engine_sand.execute("""ALTER TABLE %s ADD PRIMARY KEY(gid);""" %(dbnameFinal))

#List columns & types in the model database table (not in sandbox portion; in spatial_database)
types=DbImport.define_db_table_format(db_lines_table, engine)

#Set the type of these columns to match
DbImport.set_column_types_to_match_other_table(list(types.attname), list(types.type), dbnameFinal, engine_sand)

#Change owner
engine_sand.execute("ALTER TABLE %s OWNER TO administrator"%(dbnameLines))

#Queries that only work when passed DIRECTLY in SQL, and not thru Python SQLAlchemy
query1="GRANT SELECT ON TABLE %s TO reader;"%(dbnameFinal)
query2="GRANT ALL ON TABLE %s TO administrator;"%(dbnameFinal)
query3="ALTER TABLE %s OWNER TO administrator;"%(dbnameFinal)

print( '''

STOP! You must enter the following SQL query directly into Postgres; passing thru SQLAlchemy is not working.

After executing in Postgres, continue.

''')

print(query1)
print(query2)
print(query3)

######
# CLEAN WORKSPACE
######

#Delete temp tables that were created with upload, but are no longer needed:
engine_sand.execute("DROP TABLE %s"%(dbnameLines))
engine_sand.execute("DROP TABLE %s"%(dbnameMeta))
print("Deleted Temp Tables: %s %s"%(dbnameLines, dbnameMeta))

######
# REFORMAT POINTS TABLE
######

#Copy column formats from existing table in database
types=DbImport.define_db_table_format(db_points_table, engine)

#Set the type of these columns to match
DbImport.set_column_types_to_match_other_table(list(types.attname), list(types.type), dbnamePts, engine_sand)

#Remove columns in sandbox table which are not present in the model table

#List columns in current table
query="SELECT * FROM %s LIMIT 10"%(dbnamePts)
current_tab=pd.read_sql(query, engine_sand)

remove_cols=set(list(current_tab)) - set(list(types['attname']))

for col in remove_cols:
    print "Removing " + col
    rm_query="ALTER TABLE %s DROP COLUMN %s"%(dbnamePts, col)
    engine_sand.execute(rm_query)

    #Alter permissions
#Change owner
engine_sand.execute("ALTER TABLE %s OWNER TO administrator"%(dbnamePts))

#Queries that only work when passed DIRECTLY in SQL, and not thru Python SQLAlchemy
query1="GRANT SELECT ON TABLE %s TO reader;"%(dbnamePts)
query2="GRANT ALL ON TABLE %s TO administrator;"%(dbnamePts)
query3="ALTER TABLE %s OWNER TO administrator;"%(dbnamePts)

print( '''

STOP! You must enter the following SQL query directly into Postgres; passing thru SQLAlchemy is not working.

After executing in Postgres, continue.

''')

print(query1)
print(query2)
print(query3)