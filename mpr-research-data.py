import json
import logging
import os
import sys

import pandas as pd
import sqlalchemy as sql
from google.cloud import storage

logging.basicConfig(
    format='%(asctime)s %(levelname)-4s [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S%z',
    level='INFO'
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
            courseQueryTemplate, defaultQueryFolder, str(monthsModifier))
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
    allSliced, allSaved = True, True

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
            logging.error(
                f'Failed to save Course Data for {outputFilename}.')
            allSliced = False
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
            allSaved = False
            continue

    return allSliced, allSaved


class Config:
    def __init__(self):
        self.logLevel: str = 'INFO'
        self.targetBucketName: str = 'mpr-research-data-uploads'
        self.numberOfMonths: int = 4
        self.defaultQueryFolder: str = 'queries'
        self.queryTemplateDict: str = {'course': 'courseQuery.sql',
                                       'retrieve': 'retrieveQuery.sql'}
        self.dbParams: dict = {}
        self.gcpParams: dict = {}

    def set(self, name, value):
        if name in self.__dict__:
            self.name = value
        else:
            raise NameError('Name not accepted in set() method')

    def setAndVerifySQLFile(self, queryType):

        try:
            self.queryTemplateDict[queryType] = str(
                os.getenv(f'{queryType.upper()}_QUERY', self.queryTemplateDict[queryType]))

            # I would like to expand the checks here to allow for custom queries from a file, or as a string in the config file.
            if self.queryTemplateDict[queryType].lower().endswith('.sql'):
                logging.info(
                    f'Loading in SQL file - {self.queryTemplateDict[queryType]}')
            if not os.path.isfile(os.path.join(self.defaultQueryFolder, self.queryTemplateDict[queryType])):
                logging.error(
                    f'SQL Query file for {queryType} not found in query directory {self.defaultQueryFolder}.')
                return False
            return True

        except TypeError as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid type (expected str) passed for query type: {queryType}.')
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

            if credPart == 'PORT':
                try:
                    self.dbParams[credPart] = int(os.getenv(
                        'DB_' + credPart, dbCredsDefaultDict[credPart]))
                except ValueError as e:
                    logging.error(f'Error Message: {e}')
                    logging.error(
                        f'Invalid type (expected int) passed for DB_{credPart}.')
                    allKeyPartsFound = False

            else:
                try:
                    self.dbParams[credPart] = str(os.getenv(
                        'DB_' + credPart, dbCredsDefaultDict[credPart]))
                except TypeError as e:
                    logging.error(f'Error Message: {e}')
                    logging.error(
                        f'Invalid type (expected str) passed for DB_{credPart}.')
                    allKeyPartsFound = False

            if not self.dbParams.get(credPart, False):
                logging.error(
                    f'Did not find configuration parameter for M-Write Peer Review production DB key: {credPart}.')
                allKeyPartsFound = False

        return allKeyPartsFound

    def setAndVerifyGCPCreds(self):
        try:
            self.gcpParams = json.loads(str(os.getenv('GCP_KEY')))
            return True
        except json.JSONDecodeError as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid JSON format passed for GCloud Service JSON Key.')
            return False

    def setFromEnv(self):

        try:
            self.logLevel = str(os.environ.get(
                'LOG_LEVEL', self.logLevel)).upper()
        except TypeError as e:
            logging.warning(f'Error Message: {e}')
            logging.warning(
                'Incorrect type for configuration parameter for log level provided. Defaulting to INFO level.')
        try:
            logging.getLogger().setLevel(logging.getLevelName(self.logLevel))
        except ValueError as e:
            logging.warning(f'Error Message: {e}')
            logging.warning(
                'Invalid configuration parameter for log level provided. Defaulting to INFO level.')

        envImportSuccess = True
        try:
            self.targetBucketName = str(
                os.getenv('GCLOUD_BUCKET', self.targetBucketName))
        except TypeError as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid type (expected str) passed for GCloud bucket name.')
            envImportSuccess = False

        try:
            self.numberOfMonths = int(
                os.getenv('NUMBER_OF_MONTHS', self.numberOfMonths))

            if self.numberOfMonths < 1:
                logging.error(
                    f'Config parameter NUMBER_OF_MONTHS must be >= 1.')
                envImportSuccess = False

        except ValueError as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Non-integer passed for config parameter NUMBER_OF_MONTHS.')
            envImportSuccess = False

        try:
            self.defaultQueryFolder = str(
                os.getenv('QUERY_FOLDER', self.defaultQueryFolder))
            if not os.path.isdir(self.defaultQueryFolder):
                logging.error(
                    f'Default Query folder set by QUERY_FOLDER not found in repo directory.')
                envImportSuccess = False
            else:
                for queryType in self.queryTemplateDict:
                    if not self.setAndVerifySQLFile(queryType):
                        envImportSuccess = False
        except TypeError as e:
            logging.error(f'Error Message: {e}')
            logging.error(
                f'Invalid type (expected str) passed for configuration parameter QUERY_FOLDER.')
            envImportSuccess = False

        if not self.setAndVerifyDBCreds():
            envImportSuccess = False

        if not self.setAndVerifyGCPCreds():
            envImportSuccess = False

        if not envImportSuccess:
            sys.exit('Exiting due to configuration parameter import problems.')
        else:
            logging.info('All configuration parameters set up successfully.')


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

    if len(courseQueryDF) == 0:
        logging.info('No courses to be retrieved.')
        sys.exit('Exiting due to no courses being found in current configuration.')

    # RETRIEVE COURSE DATA
    # --------------------------------------------------------------------------
    retrieveQueryDF = retrieveQueryMaker(
        config.queryTemplateDict['retrieve'], courseQueryDF['id'], sqlEngine, config.defaultQueryFolder)

    # SEND TO GCP BUCKET
    # --------------------------------------------------------------------------
    allSliced, allSaved = sliceAndPushToGCP(courseQueryDF, retrieveQueryDF,
                                            gcpClient, config.targetBucketName)

    # This is because even if one course fails to save or upload
    # The code can still attempt to keep running for the other courses.
    # The warnings here adds a final message that there was an error
    # at some point in slicing and saving the data to GCP

    if not allSliced:
        logging.warning(
            'Not all course data could be sliced correctly.')
    if not allSaved:
        logging.warning(
            'Not all course data could be saved to GCP correctly.')


if '__main__' == __name__:
    main()
