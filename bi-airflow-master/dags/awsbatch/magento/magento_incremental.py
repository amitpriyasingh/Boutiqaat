from airflow import DAG
import boto3
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Muthu',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 20),
    'email': ['bi@boutiqaat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='MAGENTO_INCREMENTAL', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2,4,10,12 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['MAGENTO','ML'])

# catalog_product_flat_1 disabled_skus eav_attribute eav_attribute_option_swatch sales_order sales_order_item magento_customerbalance magento_customerbalance_history celebrity_am_log magento_product_catalog sku_brands sku_categories customer_demographic_info notify_out_of_stock order_gen_mapped

try:
    start = DummyOperator(
                task_id='start',
                dag=dag)
   
    end = DummyOperator(
                task_id='end',
                dag=dag)
 
    pg_report_incremental = AWSBatchOperator(
                task_id='magento-pg_report_incremental',
                job_name='magento_pg_report_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C ML payment_gateway_report_incremental']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
    start >> [pg_report_incremental] >> end

    """
    order_gen_mapped = AWSBatchOperator(
                task_id='magento-order_gen_mapped_incremental',
                job_name='magento_order_gen_mapped_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento order_gen_mapped_incremental']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
    """
except Exception as e:
    start.log.info(e)