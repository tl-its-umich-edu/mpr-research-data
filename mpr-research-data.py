# LIBRARIES TO IMPORT
# --------------------------------------------------------------------------
import pandas as pd
import sqlalchemy as sql
import os
import sys
from google.cloud import storage


# SETUP CONFIG VARIABLES
# --------------------------------------------------------------------------
targetBucketName = os.getenv('GCLOUD_BUCKET', 'mpr-research-data-uploads')

numberOfMonths = os.getenv('NUMBER_OF_MONTHS', 4)

defaultQueryFolder = os.getenv('QUERY_FOLDER', 'queries')
queryTemplateDict = {'course': os.getenv('COURSE_QEURY', 'courseQuery.sql'),
                     'retrieve': os.getenv('RETRIEVE_QUERY', 'retrieveQuery.sql')}


# FUNCTIONS
# --------------------------------------------------------------------------

def getDBCreds():

    dbCredsDefaultDict = {'NAME': None, 'USER': None, 'PASSWORD': None, 
                          'HOST': None, 'PORT': 3306}
    dbCredsDict = {}

    allKeyPartsFound = True
    for credPart in dbCredsDefaultDict:
        dbCredsDict[credPart] = os.getenv('DB_'+credPart, dbCredsDefaultDict[credPart])

        if not dbCredsDict[credPart]:
            print(f'Did not find .env variable for M-Write Peer Review production DB key: {credPart}')
            allKeyPartsFound = False

    if not allKeyPartsFound:
        sys.exit('Exiting.')

    return dbCredsDict


def queryRetriever(queryName, queryModifier=False, 
                    queryFolder=defaultQueryFolder):
    with open(os.path.join(queryFolder, queryName)) as queryFile:
        queryLines = ''.join(queryFile.readlines())
    
    if queryModifier:
        queryLines = queryLines.format(queryModifier)

    return queryLines


def makeDBConnection(dbParams):

    try:
        connectString = f'mysql+pymysql://{dbParams["USER"]}:{dbParams["PASSWORD"]}@{dbParams["HOST"]}:{dbParams["PORT"]}/{dbParams["NAME"]}'
        engine = sql.create_engine(connectString)
        print('DB connection established.')
        return engine

    except Exception as e:
        print(f'Error Message: {e}')
        print('Failed to establish DB connection.')
        sys.exit('Exiting.')


def makeGCPConnection(gcpJSON):

    try:

        #I am not sure why this is how GCloud needs to accept a JSON String. Will look into alternatives.
        with open('gcpAccessKey.json', 'w') as jsonFile:
            jsonFile.write(gcpJSON)
        client = storage.Client.from_service_account_json('gcpAccessKey.json')
        os.remove('gcpAccessKey.json')
        print('GCP connection established.')
        return client

    except Exception as e:
        print(f'Error Message: {e}')
        print('Failed to establish GCP connection.')
        sys.exit('Exiting.')


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
        sys.exit('Exiting.')



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
        sys.exit('Exiting.')

    return retrieveDF


def sliceAndPushToGCP(courseDF, retrieveDF, client, targetBucketName=targetBucketName):

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


# RETRIEVE KEYS
# --------------------------------------------------------------------------

dbParams = getDBCreds()
gcpJSON = os.getenv('GCP_KEY')


# ESTABLISH CONNECTIONS
# --------------------------------------------------------------------------
sqlEngine = makeDBConnection(dbParams)
gcpClient = makeGCPConnection(gcpJSON)
# sys.stdout.flush()

# RETRIEVE COURSE INFO
# --------------------------------------------------------------------------
courseQueryDF = courseQueryMaker(queryTemplateDict['course'], numberOfMonths, sqlEngine)
# sys.stdout.flush()

# RETRIEVE COURSE DATA
# --------------------------------------------------------------------------
retrieveQueryDF = retrieveQueryMaker(queryTemplateDict['retrieve'], courseQueryDF['id'], sqlEngine)
# sys.stdout.flush()

# SEND TO GCP BUCKET
# --------------------------------------------------------------------------
sliceAndPushToGCP(courseQueryDF, retrieveQueryDF, gcpClient, targetBucketName)
# sys.stdout.flush()

print('All steps complete.')
