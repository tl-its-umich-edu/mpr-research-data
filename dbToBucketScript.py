from dbToBucketLibrary import *


#CONFIG VARIABLES
#------------------------------------------------------------------------------------------
targetBucketName = os.getenv('GCLOUD_BUCKET')

numberOfMonths = os.getenv('NUMBER_OF_MONTHS')

queryTemplateDict = {'course': os.getenv('COURSE_QEURY'), 'retrieve': os.getenv('RETRIEVE_QUERY')}
credsDict = {'db': os.getenv('DB_KEY'), 'gcp': os.getenv('GCP_KEY')}
#------------------------------------------------------------------------------------------

sqlEngine, gcpClient = connectionSetup(credsDict)
sys.stdout.flush()

courseQueryDF = courseQueryMaker(queryTemplateDict['course'], numberOfMonths, sqlEngine)
sys.stdout.flush()

retrieveQueryDF = retrieveQueryMaker(queryTemplateDict['retrieve'], courseQueryDF['id'], sqlEngine)
sys.stdout.flush()

sliceAndPushToGCP(courseQueryDF, retrieveQueryDF, gcpClient, targetBucketName)
sys.stdout.flush()

print('All steps complete.')