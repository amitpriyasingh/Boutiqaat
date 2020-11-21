from airflow import DAG
import boto3
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.operators.dummy_operator import DummyOperator
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
    dag_id='OFS_DAILY',  
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2,4,8,10,13,15 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['OFS'])

# t1, t2, t3 and t4 are examples of tasks created using operators

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

    ofs_status_master = AWSBatchOperator(
                task_id='ofs-status-master-1',
                job_name='ofs_status_master',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS status_master']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_crm_orders = AWSBatchOperator(
                task_id='ofs-crm-orders-1',
                job_name='ofs_crm_orders',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS crm_orders']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_inbound_sales_header = AWSBatchOperator(
                task_id='ofs-inbound-sales-header-1',
                job_name='ofs_inbound_sales_header',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS inbound_sales_header']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_inbound_payment_line = AWSBatchOperator(
                task_id='ofs-inbound-payment-line-1',
                job_name='ofs_inbound_payment_line',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS inbound_payment_line']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_inbound_sales_line = AWSBatchOperator(
                task_id='ofs-inbound-sales-line-1',
                job_name='ofs_inbound_sales_line',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS inbound_sales_line']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_order_batch_details = AWSBatchOperator(
                task_id='ofs-order-batch-details-1',
                job_name='ofs_order_batch_details',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS order_batch_details']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_inbound_order_address = AWSBatchOperator(
                task_id='ofs-inbound-order-address-1',
                job_name='ofs_inbound_order_address',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS inbound_order_address']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_order_status = AWSBatchOperator(
                task_id='ofs-order-status-1',
                job_name='ofs_order_status',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS order_status']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_hold_orders = AWSBatchOperator(
                task_id='ofs-hold-orders-1',
                job_name='ofs_hold_orders',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS hold_orders']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    ofs_cancelled_orders = AWSBatchOperator(
                task_id='ofs-cancelled-orders-1',
                job_name='ofs_cancelled_orders',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS cancelled_orders']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    start >> ofs_status_master >> [ofs_crm_orders, ofs_inbound_sales_header, ofs_inbound_payment_line, ofs_inbound_sales_line, ofs_order_batch_details, ofs_inbound_order_address, ofs_order_status, ofs_hold_orders, ofs_cancelled_orders] >> end

except Exception as e:
    ofs_status_master.log.info(e)