import json
import pandas as pd
import sqlalchemy as sql

dbParams = json.load(open('database-research_ro.json'))
connectString = f'mysql+pymysql://{dbParams["USER"]}:{dbParams["PASSWORD"]}@{dbParams["HOST"]}:{dbParams["PORT"]}/{dbParams["NAME"]}'
print(connectString)
engine = sql.create_engine(connectString)

# FIXME: use query to get course IDs with reviews completed in last 6 months
courseQuery = 'select id, name FROM canvas_courses order by id desc limit 5'

with engine.connect() as connection:
    courseDF = pd.read_sql(courseQuery, connection)

print(courseDF)

with open('query.sql') as queryFile:
    reviewQuery = ''.join(queryFile.readlines())

# FIXME: query should contain a place to substitute parameters.
# I.e., one course ID at a time.

with engine.connect() as connection:
    reviewDF = pd.read_sql(reviewQuery, connection)

print(reviewDF)

# FIXME: format reviewDF as text file and store in GCP
# Preferrably, format data as TSV (CSV only if *necessary*).
# Upload data to GCP and store in bucket.
