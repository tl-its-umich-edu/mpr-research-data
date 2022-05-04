import json
import pandas as pd
import sqlalchemy as sql

dbParams = json.load(open('database-research_ro.json'))
connect_string = f'mysql+pymysql://{dbParams["USER"]}:{dbParams["PASSWORD"]}@{dbParams["HOST"]}:{dbParams["PORT"]}/{dbParams["NAME"]}'
print(connect_string)
engine = sql.create_engine(connect_string)
