from airflow import DAG
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Omar',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['bi@boutiqaat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='CELEBRITY_MASTER', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 6 * * *',
    dagrun_timeout=timedelta(minutes=15),
    tags=['AOI'])
    


celebrity_master = AWSBatchOperator(
                task_id='celebrity_master',
                job_name='celebrity_master',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI celebrity_master']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
                
celebrity_master_load = PostgresOperator(
                task_id='celebrity_master_load',
                sql='queries/celebrity_master_load.sql',
                postgres_conn_id='redshift',
                dag=(dag))

celebrity_master >> celebrity_master_load