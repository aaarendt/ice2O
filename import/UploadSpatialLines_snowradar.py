# Upload Spatial Lines to sandbox
### Take points files (csv), convert to lines (by factored column), and make lines
#Note: geometry information must be in columns labeled "lat" and "long"; geometry is created in column "geom"

import sys
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
sys.path.append("C:/Users/ehbaker/Documents/Python/Repos/ice2O") #Path to where DBImport.py is saved
import DbImport #User defined module in the folder added to path in line above.
import numpy as np
import settings

### User supplied criteria
#For snow radar data
db_points_table='snowradar' #table with points in the db
db_lines_table='snowradar_lines' #table with lines in the db
pth=(r"Q:\Project Data\GlacierData\GPR\Wolverine\2016\Ice2ODatabase\Wolverine_2016.csv") #path to csv of points for upload
pth_lines_meta=(r"Q:\Project Data\GlacierData\GPR\Wolverine\2016\Ice2ODatabase\Wolverine_2016_meta_lines.csv")

#Connect to database and sandbox
cs=settings.import_cs() #user-defined module to store connection info
#Spatial_database
engine = create_engine('postgresql://' + cs['user'] + ':' + str(cs['password']) + '@' + cs['host'] + ':' + cs['port'] + '/' + cs['dbname'])
#Sandbox
engine_sand = create_engine('postgresql://' + cs['user'] + ':' + str(cs['password']) + '@' + cs['host'] + ':' + cs['port'] + '/' + 'sandbox')

#Read in csvs for upload
df_pts=pd.read_csv(pth) #read point data to dataframe
df_lin_meta=pd.read_csv(pth_lines_meta)

#Make name for table of points you're uploading
dbnamePts=db_points_table+'_points'
print(df_pts.shape)
print(dbnamePts + ': table of all points collected')

#Add [a subset of] points dataframe to the DB
df_pts=df_pts.sample(3000)
df_pts=df_pts.sort_values('trace')
df_pts.to_sql(dbnamePts, engine_sand, index=False)
print("Added all points as " + dbnamePts)

# create the geometry field
engine_sand.execute("""ALTER TABLE %s ADD COLUMN geom geometry(Point, 3338);""" %(dbnamePts)) 
# populate the geometry field
engine_sand.execute("""UPDATE %s SET geom = ST_Transform(ST_setSRID(ST_MakePoint(long,lat),4326),3338);""" %(dbnamePts))

# Make the associated line file
dbnameLines= db_lines_table +'_temp_geometry'
engine_sand.execute("""CREATE TABLE %s (collection text, geom geometry(Linestring, 3338));""" %(dbnameLines))
query = ('WITH linecreation AS (SELECT collection, ST_MakeLine(geom) as geom FROM ' + dbnamePts + ' GROUP BY collection) INSERT INTO ' + dbnameLines + ' SELECT * FROM linecreation;')
engine_sand.execute(query)
print("Created EMPTY table: " + dbnameLines+ " ; to fill table, see cell below")


#The last command is currently not workikng; must pass it in directly into SQL window. Copy and paste the text below:
engine_sand.execute(query)
print(query)

#Upload and join the metadata table to the geospatial lines table (already in the sandbox)
dbnameMeta = 'snowradar_ingest_metadata'
df_lin_meta.to_sql(dbnameMeta, engine_sand, index=False, if_exists='replace')

#Use a join to create our desired table, from 2 initial uploads (points-> lines, and Metadata )
dbnameFinal= db_lines_table +'_ingest_final'
query= """CREATE TABLE %s AS
SELECT %s.collection, velocity, density, date, obs_type, geom
FROM %s LEFT OUTER JOIN %s 
ON (snowradar_ingest_metadata.collection =%s.collection)"""%(dbnameFinal,dbnameMeta, dbnameMeta, dbnameLines, dbnameLines)
engine_sand.execute(query)

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
print("Enter queries directly into SQL window:")
print(query1)
print(query2)
print(query3)

#Delete temp tables that were created with upload, but are no longer needed:
engine_sand.execute("DROP TABLE %s"%(dbnameLines))
engine_sand.execute("DROP TABLE %s"%(dbnameMeta))
print("Deleted Temp Tables: %s %s"%(dbnameLines, dbnameMeta)
