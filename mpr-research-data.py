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
        sys.exit('Exiting due to failed DB connection.')


def makeGCPConnection(gcpParams):

    try:
        client = storage.Client.from_service_account_info(gcpParams)
        logging.info('GCP connection established.')
        return client

    except Exception as e:
        logging.error(f'Error Message: {e}')
        logging.error('Failed to establish GCP connection.')
        sys.exit('Exiting due to failed GCP connection.')


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
        sys.exit('Exiting due to failure in Course List retrieval.')


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
        sys.exit('Exiting due to failure in Course Data retrieval.')

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
        self.dbParams: dict = {'NAME': None,
                               'USER': None,
                               'PASSWORD': None,
                               'HOST': None,
                               'PORT': 3306}
        self.gcpParams: dict = {}

    def set(self, name, value):
        if name in self.__dict__:
            self.name = value
        else:
            raise NameError('Name not accepted in set() method')

    def configFetch(self, name, default=None, casting=None, validation=None, valErrorMsg=None):
        value = os.getenv(name, default)
        if (casting is not None):
            try:
                value = casting(value)
            except:
                errorMsg = f'Casting error for config item "{name}" value "{value}".'
                logging.error(errorMsg)
                return None
                #raise TypeError(errorMsg)
        if (validation is not None and not validation(value)):
            errorMsg = f'Validation error for config item "{name}" value "{value}".'
            logging.error(errorMsg)
            return None
            #raise ValueError(errorMsg)
        return value

    def setFromEnv(self):

        try:
            self.logLevel = str(os.environ.get('LOG_LEVEL', self.logLevel)).upper()
        except:
            warnMsg = f'Casting error for config item LOG_LEVEL value. Defaulting to {logging.getLevelName(self.logLevel)}.'
            logging.warning(warnMsg)
        try:
            logging.getLogger().setLevel(logging.getLevelName(self.logLevel))
        except:
            warnMsg = f'Validation error for config item LOG_LEVEL value. Defaulting to {logging.getLevelName(self.logLevel)}.'
            logging.warning(warnMsg)
        
        # Currently the code will check and validate all config variables before stopping.
        # Reduces the number of runs needed to validate the config variables.
        envImportSuccess = True

        self.targetBucketName = self.configFetch(
            'GCLOUD_BUCKET', self.targetBucketName, str)
        envImportSuccess = False if not self.targetBucketName or not envImportSuccess else True

        self.numberOfMonths = self.configFetch(
            'NUMBER_OF_MONTHS', self.numberOfMonths, int, lambda x: x > 0)
        envImportSuccess = False if not self.numberOfMonths or not envImportSuccess else True

        self.defaultQueryFolder = self.configFetch(
            'QUERY_FOLDER', self.defaultQueryFolder, str, lambda x: os.path.isdir(x))
        envImportSuccess = False if not self.defaultQueryFolder or not envImportSuccess else True

        if type(self.defaultQueryFolder) == str:
            for queryType in self.queryTemplateDict:
                self.queryTemplateDict[queryType] = self.configFetch(queryType.upper()+'_QUERY',
                                                                     self.queryTemplateDict[queryType], str,
                                                                     lambda x: os.path.isfile(os.path.join(self.defaultQueryFolder, x)))
                envImportSuccess = False if not self.queryTemplateDict[
                    queryType] or not envImportSuccess else True

        for credPart in self.dbParams:
            if credPart == 'PORT':
                self.dbParams[credPart] = self.configFetch(
                    'DB_'+credPart, self.dbParams[credPart], int, lambda x: x > 0)
            else:
                self.dbParams[credPart] = self.configFetch(
                    'DB_'+credPart, self.dbParams[credPart], str)
            envImportSuccess = False if not self.dbParams[credPart] or not envImportSuccess else True

        self.gcpParams = self.configFetch(
            'GCP_KEY', casting=lambda x: json.loads(str(x)))
        envImportSuccess = False if not self.gcpParams or not envImportSuccess else True

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
