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
    dag_id='AOI_HOURLY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='20 2-20 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['AOI'])

# t1, t2, t3 and t4 are examples of tasks created using operators


try:
    t0 = AWSBatchOperator(
                task_id='aoi-consolidated-sku-stock-1',
                job_name='aoi_consolidated_sku_stock',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI consolidated_sku_stock']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    t1 = AWSBatchOperator(
                task_id='aoi-inventory-health-1',
                job_name='aoi_inventory_health',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI inventory_health']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    t2 = AWSBatchOperator(
                task_id='aoi-sku_stock-1',
                job_name='aoi_sku_stock',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI sku_stock']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    t3 = AWSBatchOperator(
                task_id='aoi-dsp_shipment_status',
                job_name='aoi_dsp_shipment_status',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI dsp_shipment_status']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    t0.set_upstream(t2)
    t1.set_upstream(t2)
    t3.set_upstream(t2)
except Exception as e:
    t1.log.info(e)