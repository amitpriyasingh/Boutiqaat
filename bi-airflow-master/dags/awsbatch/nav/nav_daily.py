from airflow import DAG
import boto3
from airflow.operators.dummy_operator import DummyOperator
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
    dag_id='NAV_DAILY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 8 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['NAV'])

# item warehouse_entry stock_details location item_ledger_entry nav_sku_master sales_invoice_line item_vendor_discount purchase_header item_vendor nav_product_category

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

    item = AWSBatchOperator(
                task_id='nav-item',
                job_name='nav_item',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV item']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    warehouse_entry = AWSBatchOperator(
                task_id='nav-warehouse_entry',
                job_name='nav_warehouse_entry',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV warehouse_entry']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    stock_details = AWSBatchOperator(
                task_id='nav-stock_details',
                job_name='nav_stock_details',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV stock_details']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    location = AWSBatchOperator(
                task_id='nav-location',
                job_name='nav_location',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV location']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    item_ledger_entry = AWSBatchOperator(
                task_id='nav-item_ledger_entry',
                job_name='nav_item_ledger_entry',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV item_ledger_entry']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    sales_invoice_line = AWSBatchOperator(
                task_id='nav-sales_invoice_line',
                job_name='nav_sales_invoice_line',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV sales_invoice_line']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    item_vendor_discount = AWSBatchOperator(
                task_id='nav-item_vendor_discount',
                job_name='nav_item_vendor_discount',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV item_vendor_discount']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    purchase_header = AWSBatchOperator(
                task_id='nav-purchase_header',
                job_name='nav_purchase_header',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV purchase_header']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    item_vendor = AWSBatchOperator(
                task_id='nav-item_vendor',
                job_name='nav_item_vendor',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV item_vendor']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    nav_product_category = AWSBatchOperator(
                task_id='nav-product_category',
                job_name='nav_product_category',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV nav_product_category_full']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    nav_sku_cost = AWSBatchOperator(
                task_id='nav-sku_cost',
                job_name='nav_sku_cost',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV nav_sku_cost']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    start >> [item_ledger_entry, item, warehouse_entry, stock_details, location] >> next >> [sales_invoice_line, purchase_header, item_vendor, item_vendor_discount, nav_product_category, nav_sku_cost] >> end

except Exception as e:
    item.log.info(e)