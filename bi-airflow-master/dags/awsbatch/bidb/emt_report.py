from airflow import DAG
import boto3
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'omar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['bi@boutiqaat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

dag = DAG(
    dag_id='EMT_REPORT', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 3,9 * * *',
    dagrun_timeout=timedelta(minutes=120),
    tags=['BIDB'])

# generate_impressions daily_traffic generate_marketing_users page_loaded_ctr

try:
    emt_report = AWSBatchOperator(
                task_id='bidb-emt_report',
                job_name='bidb_emt_report',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C bidb emt_report']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

except Exception as e:
    emt_report.log.info(e)