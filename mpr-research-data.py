import json
import pandas as pd
import sqlalchemy as sql
import os
import sys

dbParams = json.load(open('database-research_ro.json'))
connectString = f'mysql+pymysql://{dbParams["USER"]}:{dbParams["PASSWORD"]}@{dbParams["HOST"]}:{dbParams["PORT"]}/{dbParams["NAME"]}'
engine = sql.create_engine(connectString)


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


def courseFilter(courseDF):
    courseIDList = []

    for index,row in courseDF.iterrows():
        if '22' in row['name']:
            courseIDList.append(row['id'])

    return courseIDList
    
def queryMaker(courseIDList, templateQuery = 'testQuery.sql'):
    with open(templateQuery) as queryFile:
        queryLines = ''.join(queryFile.readlines())

    courseIDString = ',\n   '.join(map(str, courseIDList))+'\n'
    newQuery = queryLines.format(courseIDString)

    return newQuery

def queryBulkPull(query, engine):
  with engine.connect() as connection:   
      reviewDF = pd.read_sql(query, connection)

  return reviewDF


def saveQueries(courseDF, reviewDF, savePath='TSVs/'):

  if not os.path.exists(savePath):
    os.makedirs(savePath)

  for _, row in courseDF.iterrows():
      (courseID, courseName) = row
      outputFilename = f'{courseID} - {courseName}.tsv'

      saveDF = reviewDF[reviewDF['CourseID'] == courseID]
      saveDF.to_csv(savePath+outputFilename, sep="\t", quoting=3, quotechar="", escapechar="\\")


with engine.connect() as connection:
    courseDF = pd.read_sql(courseQuery, connection)
print('Courses retrieved...')

newQuery = queryMaker(courseDF['id'])
print('Query built...')

sys.stdout.flush()

print('Retrieving...')
reviewDF = queryBulkPull(newQuery, engine)

sys.stdout.flush()

print('Saving...')
saveQueries(courseDF, reviewDF)

print('All done!')