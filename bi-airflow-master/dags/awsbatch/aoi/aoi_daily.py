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
    dag_id='AOI_DAILY',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 3,6,12 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['AOI'])

# t1, t2, t3 and t4 are examples of tasks created using operators

try:

    start = DummyOperator(
                task_id='start',
                dag=dag)

    end = DummyOperator(
                task_id='end',
                dag=dag)

    bi_celebrity_master = AWSBatchOperator(
                task_id='aoi-bi-celebrity-master',
                job_name='aoi_bi_celebrity_master',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI bi_celebrity_master']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    order_items = AWSBatchOperator(
                task_id='aoi-order-items',
                job_name='aoi_order_items',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI order_items']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    order_details = AWSBatchOperator(
                task_id='aoi-order-details',
                job_name='aoi_order_details',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI order_details']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    # events_report = AWSBatchOperator(
    #             task_id='aoi-events-report',
    #             job_name='aoi_events_report',
    #             job_queue='batch-job-queue-b',
    #             job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
    #             max_retries=4200,
    #             overrides={'command': ['make -C AOI events_report']},
    #             aws_conn_id=None,
    #             array_properties={},
    #             parameters={},
    #             status_retries=10,
    #             region_name='eu-west-1',
    #             dag=(dag))

    brand_sales = AWSBatchOperator(
                task_id='aoi-brand-sales',
                job_name='aoi_brand_sales',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI brand_sales']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    brand_target_sales_master_details = AWSBatchOperator(
                task_id='aoi-brand-target-sales-master-details',
                job_name='aoi_brand_target_sales_master_details',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI brand_target_sales_master_details']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    celebrity_master = AWSBatchOperator(
                task_id='aoi-celebrity-master',
                job_name='aoi_celebrity_master',
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

    celebrity_mtd_sale_tab = AWSBatchOperator(
                task_id='aoi-celebrity-mtd-sale-tab',
                job_name='aoi_celebrity_mtd_sale_tab',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI celebrity_mtd_sale_tab']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    celebrity_daily_target = AWSBatchOperator(
                task_id='aoi-celebrity-daily-target',
                job_name='aoi_celebrity_daily_target',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI celebrity_daily_target']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    soh_report = AWSBatchOperator(
                task_id='aoi-soh-report',
                job_name='aoi_soh_report',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C AOI soh_report']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    start >> bi_celebrity_master >> [order_items, soh_report, order_items, brand_target_sales_master_details, celebrity_daily_target, brand_target_sales_master_details, celebrity_master, celebrity_mtd_sale_tab] >> end
    
except Exception as e:
    bi_celebrity_master.log.info(e)