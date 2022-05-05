import json
import pandas as pd
import sqlalchemy as sql
import os
import sys
from google.cloud import storage


def queryRetriever(queryName, queryModifier = False, queryFolder = 'SQL Query Files/'):
    with open(queryFolder+queryName) as queryFile:
        queryLines = ''.join(queryFile.readlines())
    
    if queryModifier:
        queryLines = queryLines.format(queryModifier)

    return queryLines


def connectionSetup(credsDict, credsFolder = 'Credentials/'):

    try:
        dbParams = json.load(open(credsFolder+credsDict['db']))
        connectString = f'mysql+pymysql://{dbParams["USER"]}:{dbParams["PASSWORD"]}@{dbParams["HOST"]}:{dbParams["PORT"]}/{dbParams["NAME"]}'
        engine = sql.create_engine(connectString)
        print('DB connection established.')

    except Exception as e:
        print(f'Error Message: {e}')
        print('Failed to establish DB connection.')
        sys.exit("Exiting.")

        
 
    try:
        client = storage.Client.from_service_account_json(json_credentials_path=credsFolder+credsDict['gcp'])
        print('GCP connection established.')
    except Exception as e:
        print(f'Error Message: {e}')
        print('Failed to establish GCP connection.')
        sys.exit("Exiting.")

    return engine, client


def courseQueryMaker(courseQueryTemplate, monthsModifier, engine):

    try:
        courseQuery = queryRetriever(courseQueryTemplate, monthsModifier)
        with engine.connect() as connection:
            courseDF = pd.read_sql(courseQuery, connection)
        print('Courses retrieved...')

        return courseDF

    except Exception as e:
        print(f'Error Message: {e}')
        print('Failed to retrieve Course List.')
        sys.exit("Exiting.")



def retrieveQueryMaker(retrieveQueryTemplate, courseModifier, engine):

    try:
        courseIDString = ',\n   '.join(map(str, courseModifier))+'\n'
        retrieveQuery = queryRetriever(retrieveQueryTemplate, courseIDString)
        with engine.connect() as connection:
            retrieveDF = pd.read_sql(retrieveQuery, connection)
        print('Course Data retrieved...')

    except Exception as e:
        print(f'Error Message: {e}')
        print('Failed to retrieve Course Data.')
        sys.exit("Exiting.")

    return retrieveDF


def sliceAndPushToGCP(courseDF, retrieveDF, client, targetBucketName = 'mpr-research-data-uploads' ):

    bucket = client.bucket(targetBucketName)

    for _, row in courseDF.iterrows():
        (courseID, courseName) = row
        outputFilename = f'{courseID} - {courseName}.tsv'

        try:
            
            print(f'Slicing and saving: {outputFilename}')
            saveDF = retrieveDF[retrieveDF['CourseID'] == courseID]
            saveDF.to_csv(outputFilename, sep="\t", quoting=3, quotechar="", escapechar="\\")

        except Exception as e:
            print(f'Error Message: {e}')
            print(f'Failed to save Course Data for {outputFilename}.')
            continue

        try:
            blob = bucket.blob(outputFilename)
            blob.upload_from_filename(outputFilename)
            os.remove(outputFilename)
        except Exception as e:
            print(f'Error Message: {e}')
            print(f'Failed to upload Course Data for {outputFilename} to GCP.')
            continue

    return True