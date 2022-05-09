import json
import sys

import pandas as pd
import sqlalchemy as sql

dbParams = json.load(open('database-research_ro.json'))
connectString = f'mysql+pymysql://{dbParams["USER"]}:{dbParams["PASSWORD"]}@{dbParams["HOST"]}:{dbParams["PORT"]}/{dbParams["NAME"]}'
engine = sql.create_engine(connectString)

# appropriate number of months to approximate the length of academic term
# by trial and error, 4 seems to be good
# FIXME: make this a configuration parameter
numberOfMonths = 4

courseQuery = f'''
SELECT
  DISTINCT ca.course_id as id,
  cc.name as name
FROM
  mwrite_peer_review.canvas_assignments ca
LEFT JOIN mwrite_peer_review.canvas_courses cc ON
  cc.id = ca.course_id
WHERE
  is_peer_review_assignment = 1
  AND ca.due_date_utc BETWEEN NOW() - INTERVAL {numberOfMonths} MONTH AND NOW()
ORDER BY
  ca.due_date_utc DESC
'''

with engine.connect() as connection:
    courseDF = pd.read_sql(courseQuery, connection)

with open('query.sql') as queryFile:
    reviewQuery = ''.join(queryFile.readlines())

for _, row in courseDF.iterrows():
    (courseID, courseName) = row
    outputFilename = f'{courseID} - {courseName}.tsv'
    print(outputFilename)

    with engine.connect() as connection:
        reviewDF = pd.read_sql(reviewQuery.format(courseID), connection)

    print(reviewDF)

    sys.stdout.flush()

    # FIXME: format reviewDF as text file and store in GCP
    # Preferrably, format data as TSV (CSV only if *necessary*).
    # Upload data to GCP and store in bucket.
