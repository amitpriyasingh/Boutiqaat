from airflow import DAG
import boto3
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Muthu',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['bi@boutiqaat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='MAGENTO_TASKS',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['MAGENTO','ML'])

try:
    start = DummyOperator(
                task_id='start',
                dag=dag)
   
    end = DummyOperator(
                task_id='end',
                dag=dag)
 
    payment_gateway_report = AWSBatchOperator(
                    task_id='magento-payment_gateway_report',
                    job_name='magento_payment_gateway_report',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C ML payment_gateway_report']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    start >> [payment_gateway_report] >> end
except Exception as e:
    start.log.info(e)