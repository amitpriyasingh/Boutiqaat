import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
from datetime import datetime

db_url = {
    'database': 'data_lake',
    'drivername': 'mysql+pymysql',
    'username': 'report',
    'password': 'simpass',
    'host': 'localhost',
    'query': {'charset': 'utf8'},
}

engine = create_engine(URL(**db_url), encoding="utf-8")


conn = engine.connect()
data =  pd.read_csv('events_data.csv',delimiter=',',encoding='utf-8')
data.rename(columns={' id             ':'id', ' user_name      ':'user_name', ' celebrity_name ':'celebrity_name',
       ' generic        ':'generic', ' productid      ':'productid', ' event_portal   ':'event_portal',
       ' event_type     ':'event_type', ' event_class    ':'event_class', ' total_post     ':'total_post',
       ' bq_post        ':'bq_post', ' remark         ':'remark', ' created_at     ':'created_at',
       ' event_date     ':'event_date', ' updated_at     ':'updated_at', '  event_time      ':'event_time',
       '  celebrity_id    ':'celebrity_id', ' labelid        ':'labelid', ' event_id       ':'event_id'}, inplace=True)

print(data.head())
print(data.dtypes)
data['created_at'] = pd.to_datetime(data['created_at'])
data['event_date'] = [datetime.date(x) for x in pd.to_datetime(data['event_date'])]
data['updated_at'] = pd.to_datetime(data['updated_at'])

print("====================================")
print(data.dtypes)
print(data.head())


def f1(x):
	l=x.split(' ')
	return l[0]

def f2(x):
	if x is not None:
		l=x.split(' ')
		command1 = ("select ID from MK_CELEB where NAME_E like'%%"+l[0]+"%%' group by ID")
		print(command1)
		df_data=pd.read_sql(str(command1),conn)
		d=df_data.values.tolist()
		print('=========first list')
		print(d)
		if d !=[]:
			if len(d)>=2:
				x= str(l[0]+' '+l[1])
				print(x)
				command2 = ("select ID from MK_CELEB where NAME_E like'%%"+x+"%%' group by ID")
				print(command2)
				df_data=pd.read_sql(str(command2),conn)
				d=df_data.values.tolist()
				print('=========second list')
				print(d)
				if d !=[]:
					if len(d)>=3:
						x= str(l[0]+' '+l[1]+' '+l[2])
						command3 = ("select ID from MK_CELEB where NAME_E like'%%"+x+"%%' group by ID")
						df_data=pd.read_sql(str(command3),conn)
						d=df_data.values.tolist()
						print('=========third list')
						print(d)
						if d !=[]:
							return d[0][0]
						else:
							return None
					else:
						return d[0][0]
				else:
					return None
			else:
				return d[0][0]
		else:
			return None

	else:
		return None


#data['first_name'] = data['celebrity_name'].apply(f1)

data['celebrity_id'] = data['celebrity_name'].apply(f2)

print(data)

import math

def f(x):
	#x=int(x)
	if math.isnan(x):
		print('Yeeeessssss')
		return None
	else:
		command = ("select coordinator_id from events_db.celebrity_coordinator where celebrity_id = "+str(x)+"")
		print(command)
		df_data=pd.read_sql(str(command),conn)
		d=df_data.values.tolist()
		if d!=[]:
			return d[0][0]
		else:
			return None


data['account_manager'] = data['celebrity_id'].apply(f)



data = data.drop(columns='id')
print(data.columns)





#data['id']=np.array(range(1,len(data)+1))
#data['event_id']=np.array(range(1,len(data)+1))

data.to_sql(name='events_data', con=conn, if_exists = 'append', index=False)


