import os
import requests
import datetime
from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow import AirflowException
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
'owner': 'amit',
'depends_on_past': False,
'start_date': days_ago(1),
'email': [
        'a.singh@boutiqaat.com', 
        'bi@boutiqaat.com'
        ],
'email_on_failure': True,
'email_on_retry': False,
'retries': 2,
'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
dag_id='AWO_BULK_UPLOAD',  
default_args=default_args,
catchup=False,
schedule_interval='30 2-19 * * *',
dagrun_timeout=datetime.timedelta(minutes=60),
tags=['REDSHIFT'])

def APItoS3():
  import airflow.hooks.S3_hook
  from airflow.exceptions import AirflowException
  from io import StringIO
  import pandas as pd
  from datetime import datetime as dt
  import pytz
  import urllib.request as urllib2
  url = 'https://caps.boutiqaat.com/catalogadmin/rest/v1/products/bulkSku/export/csv'
  bucket = 'btq-bi'
  key = 'awo_upload.csv'
  keys = [key]
  today_kwt = dt.now(pytz.timezone('Asia/Kuwait'))
  report_date_str = today_kwt.strftime("%Y-%m-%d")
  req = urllib2.Request(url)
  hook = airflow.hooks.S3_hook.S3Hook('s3connection')
  try:
    page = urllib2.urlopen(req)
    df_ = pd.read_csv(url)
    df_['report_date']=report_date_str
    csv_buf = StringIO()
    df_.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    filename = csv_buf.getvalue()
    hook.load_string(filename, key, bucket, replace=True)
  except urllib2.HTTPError as e:
    print (e.fp.read())
  return True 

def s3ToRedshift():
  import airflow.hooks.S3_hook
  from airflow.hooks.postgres_hook import PostgresHook
  schema = 'sandbox'
  table = 'sku_online_status_staging'
  s3_bucket = 'btq-bi'
  csv_file_name = 'awo_upload.csv'
  region = 'eu-west-1'
  s3_hook = airflow.hooks.S3_hook.S3Hook('s3connection')
  postgres_hook = PostgresHook(postgres_conn_id='redshift')
  credentials = s3_hook.get_credentials()
  
  copy_query = """ BEGIN;
            TRUNCATE table sandbox.sku_online_status_staging; 
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{csv_file}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}' 
            region '{region}'
            acceptinvchars 
            IGNOREHEADER 1
            CSV;
            COMMIT;
        """.format(schema=schema,
                   table=table,
                   s3_bucket=s3_bucket,
                   csv_file = csv_file_name,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   region=region
                   )
  postgres_hook.run(copy_query)
  return True
  
try:
  
  api_to_s3 = PythonVirtualenvOperator(
  task_id="task_api_to_s3",
  python_callable=APItoS3,
  requirements=[
      "pandas","pytz","boto3"
  ],
  system_site_packages=True,
  dag=(dag),
  )
  s3_to_staging = PythonVirtualenvOperator(
  task_id="task_s3_to_staging",
  python_callable=s3ToRedshift,
  requirements=[
      "pandas"
  ],
  system_site_packages=True,
  dag=(dag),
  )
  upsert = PostgresOperator(
  task_id='redshift-upsert',
  sql='queries/analytics/sku_online_status.sql',
  postgres_conn_id='redshift',
  dag=(dag))


  api_to_s3 >> s3_to_staging >> upsert

except Exception as e:
    raise AirflowException(e)

