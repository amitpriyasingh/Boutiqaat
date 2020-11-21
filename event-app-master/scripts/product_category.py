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

#cur = conn.cursor()
#cur.execute('select * from ERP_SKU limit 10');
df_path = pd.read_sql('select cpe.sku,ccp.product_id,cpe.entity_id,cce.path from \
	events_db.catalog_product_entity cpe left join events_db.catalog_category_product ccp on \
	ccp.entity_id = cpe.entity_id left join events_db.catalog_category_entity cce on \
	cce.entity_id = cpe.entity_id',conn)

print(df_path)
l = []
for p in df_path['path']:
	if p !=None:
		c=p.split('/')
		print(type(c[0]))
		print(len(c))
		if len(c)>2:
			if len(c)==3:
				cat=[]
				if int(c[2])==6:
					cat.append(c[2])
					cat.append(None)
					cat.append(None)
					cat.append(None)
					cat.append(c[2])
					l.append(cat)
				else:
					cat.append(c[2])
					cat.append(None)
					cat.append(None)
					cat.append(None)
					cat.append(None)
					l.append(cat)

				print('Added first')
			if len(c)==4:
				cat=[]
				if int(c[2])==6:
					cat.append(c[3])
					cat.append(None)
					cat.append(None)
					cat.append(None)
					cat.append(c[3])
					l.append(cat)
				else:
					cat.append(c[2])
					cat.append(c[3])
					cat.append(None)
					cat.append(None)
					cat.append(None)
					l.append(cat)
				print('Added second')
			if len(c)==5:
				cat=[]
				if int(c[2])==6:
					cat.append(c[3])
					cat.append(c[4])
					cat.append(None)
					cat.append(None)
					cat.append(c[3])
					l.append(cat)
					print('Added Third')
				else:
					cat.append(c[2])
					cat.append(c[3])
					cat.append(c[4])
					cat.append(None)
					cat.append(None)
					l.append(cat)
					print('Added Third')
			if len(c)>=7:
				cat=[]
				if int(c[2])==6:
					cat.append(c[3])
					cat.append(c[4])
					cat.append(c[5])
					cat.append(c[6])
					cat.append(c[3])
					l.append(cat)
					print('Added Fourth')
				else:
					cat.append(c[2])
					cat.append(c[3])
					cat.append(c[4])
					cat.append(c[5])
					cat.append(None)
					l.append(cat)
					print('Added Fourth')
		else:
			l.append(c)
			print("AAAAAA")
	else:
		l.append([None,None,None,None,None])
print(l)
print(l)
df_n=pd.DataFrame(l,columns=['cat1','cat2','cat3','cat4','brand'])
print(df_n.dtypes)
#df_n['entity_id']=path.entity_id
#df_n[['cat1','cat2','cat3','cat4','brand']] = df_n[['cat1','cat2','cat3','cat4','brand']].apply(pd.to_numeric, errors='ignore')

print(df_n)
#df_n['cat1'].replace(6,np.nan,inplace=True)
#df_n['cat2'].replace(6,np.nan,inplace=True)
#df_n['cat3'].replace(6,np.nan,inplace=True)
#df_n['cat4'].replace(6,np.nan,inplace=True)



def f1(x):
	if x is not None:
		data=pd.read_sql('select value from events_db.catalog_category_entity_varchar where row_id="'+str(x)+'" and attribute_id="'+str(41)+'" limit 1',conn)
		d=data.values.tolist()
		print(d)
		if d:
			return d[0][0]
		else:
			return None
	else:
		return x

df_n['cat1'] = df_n['cat1'].apply(f1)
print(df_n)

def f2(x):
	if x is not None:
		data=pd.read_sql('select value from events_db.catalog_category_entity_varchar where row_id="'+str(x)+'" and attribute_id="'+str(41)+'" limit 1',conn)
		d=data.values.tolist()
		print(d)
		if d:
			return d[0][0]
		else:
			return None
	else:
		return x

df_n['cat2'] = df_n['cat2'].apply(f1)

def f3(x):
	if x is not None:
		data=pd.read_sql('select value from events_db.catalog_category_entity_varchar where row_id="'+str(x)+'" and attribute_id="'+str(41)+'" limit 1',conn)
		d=data.values.tolist()
		print(d)
		if d:
			return d[0][0]
		else:
			return None
	else:
		return x

df_n['cat3'] = df_n['cat3'].apply(f1)

def f4(x):
	if x is not None:
		data=pd.read_sql('select value from events_db.catalog_category_entity_varchar where row_id="'+str(x)+'" and attribute_id="'+str(41)+'" limit 1',conn)
		d=data.values.tolist()
		print(d)
		if d:
			return d[0][0]
		else:
			return None
	else:
		return x

df_n['cat4'] = df_n['cat4'].apply(f1)

def f5(x):
	if x is not None:
		data=pd.read_sql('select value from events_db.catalog_category_entity_varchar where row_id="'+str(x)+'" and attribute_id="'+str(41)+'" limit 1',conn)
		d=data.values.tolist()
		print(d)
		if d:
			return d[0][0]
		else:
			return None
	else:
		return x

df_n['brand'] = df_n['brand'].apply(f1)
print(df_n)



print(df_n)
df_path['cat1']=df_n.cat1
df_path['cat2']=df_n.cat2
df_path['cat3']=df_n.cat3
df_path['cat4']=df_n.cat4
df_path['brand']=df_n.brand

df_final = df_path.drop(columns='path')

print(df_final)

df_final.to_sql(name='product_category', con=conn, if_exists = 'append', index=False)






