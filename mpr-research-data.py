import json
import logging
import os
import sys

import pandas as pd
import sqlalchemy as sql
from google.cloud import storage

logging.basicConfig(
    format="%(asctime)s %(levelname)-4s [%(filename)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    level=logging.INFO
)


# FUNCTIONS
# --------------------------------------------------------------------------
def makeDBConnection(dbParams):

    try:
        connectString = f'mysql+pymysql://{dbParams["USER"]}:{dbParams["PASSWORD"]}@{dbParams["HOST"]}:{dbParams["PORT"]}/{dbParams["NAME"]}'
        engine = sql.create_engine(connectString)
        logging.info('DB connection established.')
        return engine

    except Exception as e:
        logging.error(f'Error Message: {e}')
        logging.error('Failed to establish DB connection.')
        sys.exit('Exiting.')


def makeGCPConnection(gcpParams):

    try:
        client = storage.Client.from_service_account_info(gcpParams)
        logging.info('GCP connection established.')
        return client

    except Exception as e:
        logging.error(f'Error Message: {e}')
        logging.error('Failed to establish GCP connection.')
        sys.exit('Exiting.')


def queryRetriever(queryName, queryFolder, queryModifier=False,
                   ):
    with open(os.path.join(queryFolder, queryName)) as queryFile:
        queryLines = ''.join(queryFile.readlines())

    if queryModifier:
        queryLines = queryLines.format(queryModifier)

    return queryLines


def courseQueryMaker(courseQueryTemplate, monthsModifier, engine, defaultQueryFolder):

    try:
        courseQuery = queryRetriever(
            courseQueryTemplate, defaultQueryFolder, monthsModifier)
        with engine.connect() as connection:
            courseDF = pd.read_sql(courseQuery, connection)
        logging.info('Courses retrieved...')

        return courseDF

    except Exception as e:
        logging.error(f'Error Message: {e}')
        logging.error('Failed to retrieve Course List.')
        sys.exit('Exiting.')


def retrieveQueryMaker(retrieveQueryTemplate, courseIDs, engine, defaultQueryFolder):

    try:
        courseIDString = ','.join(map(str, courseIDs))
        retrieveQuery = queryRetriever(
            retrieveQueryTemplate, defaultQueryFolder, courseIDString)
        with engine.connect() as connection:
            retrieveDF = pd.read_sql(retrieveQuery, connection)
        logging.info('Course Data retrieved...')

    except Exception as e:
        logging.error(f'Error Message: {e}')
        logging.error('Failed to retrieve Course Data.')
        sys.exit('Exiting.')

    return retrieveDF


def sliceAndPushToGCP(courseDF, retrieveDF, client, targetBucketName):

    bucket = client.bucket(targetBucketName)

    for _, row in courseDF.iterrows():
        (courseID, courseName) = row
        outputFilename = f'{courseID} - {courseName}.tsv'

        try:
            logging.info(f'Slicing: {outputFilename}')
            saveDF = retrieveDF[retrieveDF['CourseID'] == courseID]
            saveDF.to_csv(outputFilename, sep="\t", quoting=3,
                          quotechar="", escapechar="\\")

        except Exception as e:
            logging.error(f'Error Message: {e}')
            logging.error(f'Failed to save Course Data for {outputFilename}.')
            continue

        try:
            logging.info(f'Saving to GCP: {outputFilename}')
            blob = bucket.blob(outputFilename)
            blob.upload_from_filename(outputFilename)
            os.remove(outputFilename)
        except Exception as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Failed to upload Course Data for {outputFilename} to GCP.')
            continue

    return True


class Config:
    def __init__(self):
        self.targetBucketName: str = 'mpr-research-data-uploads'
        self.numberOfMonths: int = 4
        self.defaultQueryFolder: str = 'queries'
        self.queryTemplateDict: str = {'course': 'courseQuery.sql',
                                       'retrieve': 'retrieveQuery.sql'}
        self.dbParams: str = None
        self.gcpParams: str = None

    def set(self, name, value):
        if name in self.__dict__:
            self.name = value
        else:
            raise NameError('Name not accepted in set() method')

    def setAndVerifySQLFile(self, queryType):
        try:
            self.queryTemplateDict[queryType] = str(
                os.getenv(f'{queryType.upper()}_QUERY', self.queryTemplateDict[queryType]))

            if self.queryTemplateDict[queryType].lower().endswith('.sql'):
                logging.info(f'Loading in SQL file - {self.queryTemplateDict[queryType]}')
            if not os.path.isfile(os.path.join(self.defaultQueryFolder, self.queryTemplateDict[queryType])):
                logging.error(
                    f'SQL Query file for {queryType} not found in query directory {self.defaultQueryFolder}.')
                return False
            return True
        except Exception as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid parameter passed for query type: {queryType}.')
            return False

    def setAndVerifyDBCreds(self):

        dbCredsDefaultDict = {'NAME': None,
                              'USER': None,
                              'PASSWORD': None,
                              'HOST': None,
                              'PORT': 3306}
        self.dbParams = {}

        allKeyPartsFound = True
        for credPart in dbCredsDefaultDict:
            try:
                self.dbParams[credPart] = str(os.getenv(
                    'DB_' + credPart, dbCredsDefaultDict[credPart]))
            except Exception as e:
                logging.error(f'Error Message: {e}')
                logging.error(
                    f'Invalid parameter passed for DB_{credPart}.')
                allKeyPartsFound = False

            if not self.dbParams[credPart]:
                logging.error(
                    f'Did not find .env variable for M-Write Peer Review production DB key: {credPart}.')
                allKeyPartsFound = False

        if not allKeyPartsFound:
            return False

    def setAndVerifyGCPCreds(self):
        try:
            self.gcpParams = json.loads(str(os.getenv('GCP_KEY')))
            return True
        except Exception as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid parameter passed for GCloud Service JSON Key.')
            return False

    def setFromEnv(self):

        envImportSuccess = True
        try:
            self.targetBucketName = str(
                os.getenv('GCLOUD_BUCKET', self.targetBucketName))
        except Exception as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid parameter passed for GCloud bucket name.')
            envImportSuccess = False

        try:
            self.numberOfMonths = str(
                os.getenv('NUMBER_OF_MONTHS', self.numberOfMonths))
            if not self.numberOfMonths.isnumeric():
                logging.error(
                    f'Non-integer passed for number of months.')
                envImportSuccess = False
        except Exception as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid parameter passed for Number of Months.')
            envImportSuccess = False

        try:
            self.defaultQueryFolder = str(
                os.getenv('QUERY_FOLDER', self.defaultQueryFolder))
            if not os.path.isdir(self.defaultQueryFolder):
                logging.error(
                    f'Default Query folder not found in repo directory.')
                envImportSuccess = False
            else:
                for queryType in self.queryTemplateDict:
                    envImportSuccess = self.setAndVerifySQLFile(queryType)
        except Exception as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid parameter passed for default Query folder.')
            envImportSuccess = False

        envImportSuccess = self.setAndVerifyDBCreds()
        envImportSuccess = self.setAndVerifyGCPCreds()

        if not envImportSuccess:
            sys.exit('Exiting.')
        else:
            logging.info('All config variables set up successfully.')


def main():
    # GET CONFIG VARIABLES
    # --------------------------------------------------------------------------
    config = Config()
    config.setFromEnv()

    # ESTABLISH CONNECTIONS
    # --------------------------------------------------------------------------
    sqlEngine = makeDBConnection(config.dbParams)
    gcpClient = makeGCPConnection(config.gcpParams)

    # RETRIEVE COURSE INFO
    # --------------------------------------------------------------------------
    courseQueryDF = courseQueryMaker(
        config.queryTemplateDict['course'], config.numberOfMonths, sqlEngine, config.defaultQueryFolder)

    # RETRIEVE COURSE DATA
    # --------------------------------------------------------------------------
    retrieveQueryDF = retrieveQueryMaker(
        config.queryTemplateDict['retrieve'], courseQueryDF['id'], sqlEngine, config.defaultQueryFolder)

    # SEND TO GCP BUCKET
    # --------------------------------------------------------------------------
    sliceAndPushToGCP(courseQueryDF, retrieveQueryDF,
                      gcpClient, config.targetBucketName)

    logging.info('All steps complete.')


if '__main__' == __name__:
    main()
