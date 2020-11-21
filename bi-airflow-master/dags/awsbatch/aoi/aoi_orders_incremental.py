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
    dag_id='AOI_ORDERS_INCREMENTAL', 
    default_args=default_args,
    catchup=False,
    schedule_interval='*/5 3-16 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['AOI'])

# t1, t2, t3 and t4 are examples of tasks created using operators


try:

    t2 = AWSBatchOperator(
                task_id='aoi-order_details_incremental-1',
                job_name='aoi_order_details_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI order_details_lastmodified']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
except Exception as e:
    t2.log.info(e)