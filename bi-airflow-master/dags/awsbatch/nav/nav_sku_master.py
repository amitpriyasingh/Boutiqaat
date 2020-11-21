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
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='NAV_SKU_MASTER', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 8 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['NAV'])

try:

    nav_sku_master = AWSBatchOperator(
                task_id='nav-sku_master',
                job_name='nav_sku_master',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV nav_sku_master_full']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
except Exception as e:
    nav_sku_master.log.info(e)