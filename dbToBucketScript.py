from dbToBucketLibrary import *

targetProjectName = 'mwrite-a835'
targetBucketName = 'mpr-research-data-uploads'

numberOfMonths = 4

queryTemplateDict = {'course': 'courseQuery.sql', 'retrieve': 'retrieveQuery.sql'}
credsDict = {'db': 'database-research_ro.json', 'gcp': 'gcpAccessKey.json'}


os.environ.setdefault("GCLOUD_PROJECT", targetProjectName)

sqlEngine, gcpClient = connectionSetup(credsDict)
sys.stdout.flush()

courseQueryDF = courseQueryMaker(queryTemplateDict['course'], numberOfMonths, sqlEngine)
sys.stdout.flush()

retrieveQueryDF = retrieveQueryMaker(queryTemplateDict['retrieve'], courseQueryDF['id'], sqlEngine)
sys.stdout.flush()

sliceAndPushToGCP(courseQueryDF, retrieveQueryDF, gcpClient)
sys.stdout.flush()

print('All steps complete.')