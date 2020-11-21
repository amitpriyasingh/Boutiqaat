from airflow import DAG
import boto3
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'retry_delay': timedelta(minutes=120),
}

dag = DAG(
    dag_id='FIREBASE_DAILY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 8 * * *',
    dagrun_timeout=timedelta(minutes=120),
    tags=['FIREBASE'])

# generate_impressions daily_traffic generate_marketing_users page_loaded_ctr

try:

    start = DummyOperator(
                task_id='start',
                dag=dag)

    end = DummyOperator(
                task_id='end',
                dag=dag)
    
    next = DummyOperator(
                task_id='next',
                dag=dag)

    generate_impressions = AWSBatchOperator(
                task_id='firebase-generate_impressions',
                job_name='firebase_generate_impressions',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C firebase generate_impressions']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    daily_traffic = AWSBatchOperator(
                task_id='firebase-daily_traffic',
                job_name='firebase_daily_traffic',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C firebase daily_traffic']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    generate_marketing_users = AWSBatchOperator(
                task_id='firebase-generate_marketing_users',
                job_name='firebase_generate_marketing_users',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C firebase generate_marketing_users']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    page_loaded_ctr = AWSBatchOperator(
                task_id='firebase-page_loaded_ctr',
                job_name='firebase_page_loaded_ctr',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C firebase page_loaded_ctr']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    start >> generate_impressions >> [daily_traffic, generate_marketing_users, page_loaded_ctr] >> end

except Exception as e:
    generate_impressions.log.info(e)
