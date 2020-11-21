from airflow import DAG
import boto3
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'omar',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 20),
    'email': ['bi@boutiqaat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='REDSHIFT_DAILY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='30 3,6,8 * * *',
    dagrun_timeout=timedelta(minutes=120),
    tags=['REDSHIFT'])

# t1, t2, t3 and t4 are examples of tasks created using operators

try:
    t1 = AWSBatchOperator(
                task_id='redshift-daily-1',
                job_name='redshift_daily',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -j4 -C redshift daily']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                timeout={'attemptDurationSeconds': 7200},
                dag=(dag))
except Exception as e:
    t1.log.info(e)