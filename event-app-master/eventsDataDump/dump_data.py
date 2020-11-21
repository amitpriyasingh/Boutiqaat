import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL

db_url = {
    'database': 'events_db',
    'drivername': 'mysql+pymysql',
    'username': 'report',
    'password': 'simpass',
    'host': 'localhost',
    'query': {'charset': 'utf8'},
}

engine = create_engine(URL(**db_url), encoding="utf-8")


conn = engine.connect()
data =  pd.read_csv('events_data.csv',delimiter=',',encoding='utf-8')

print(data.head())
data['id']=np.array(range(1,len(data)+1))
data['event_id']=np.array(range(1,len(data)+1))

data.to_sql(name='events_data', con=conn, if_exists = 'append')


