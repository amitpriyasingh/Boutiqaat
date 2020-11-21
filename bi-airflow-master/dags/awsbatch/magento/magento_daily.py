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
    dag_id='MAGENTO_DAILY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2,4,10,12 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['MAGENTO'])

# catalog_product_flat_1 disabled_skus eav_attribute eav_attribute_option_swatch sales_order sales_order_item magento_customerbalance magento_customerbalance_history celebrity_am_log magento_product_catalog sku_brands sku_categories customer_demographic_info notify_out_of_stock order_gen_mapped

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

    catalog_product_flat_1 = AWSBatchOperator(
                task_id='magento-catalog_product_flat_1',
                job_name='magento_catalog_product_flat_1',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento catalog_product_flat_1']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    disabled_skus = AWSBatchOperator(
                task_id='magento-disabled_skus',
                job_name='magento_disabled_skus',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento disabled_skus']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    eav_attribute = AWSBatchOperator(
                task_id='magento-eav_attribute',
                job_name='magento_eav_attribute',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento eav_attribute']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    eav_attribute_option_swatch = AWSBatchOperator(
                task_id='magento-eav_attribute_option_swatch',
                job_name='magento_eav_attribute_option_swatch',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento eav_attribute_option_swatch']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    sales_order = AWSBatchOperator(
                task_id='magento-sales_order',
                job_name='magento_sales_order',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento sales_order']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    sales_order_item = AWSBatchOperator(
                task_id='magento-sales_order_item',
                job_name='magento_sales_order_item',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento sales_order_item']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    magento_customerbalance = AWSBatchOperator(
                task_id='magento-magento_customerbalance',
                job_name='magento_magento_customerbalance',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento magento_customerbalance']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    magento_customerbalance_history = AWSBatchOperator(
                task_id='magento-magento_customerbalance_history',
                job_name='magento_magento_customerbalance_history',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento magento_customerbalance_history']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    celebrity_am_log = AWSBatchOperator(
                task_id='magento-celebrity_am_log',
                job_name='magento_celebrity_am_log',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento celebrity_am_log']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    magento_product_catalog = AWSBatchOperator(
                task_id='magento-magento_product_catalog',
                job_name='magento_magento_product_catalog',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento magento_product_catalog']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    sku_brands = AWSBatchOperator(
                task_id='magento-sku_brands',
                job_name='magento_sku_brands',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento sku_brands']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    sku_categories = AWSBatchOperator(
                task_id='magento-sku_categories',
                job_name='magento_sku_categories',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento sku_categories']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    customer_demographic_info = AWSBatchOperator(
                task_id='magento-customer_demographic_info',
                job_name='magento_customer_demographic_info',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento customer_demographic_info']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    notify_out_of_stock = AWSBatchOperator(
                task_id='magento-notify_out_of_stock',
                job_name='magento_notify_out_of_stock',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento notify_out_of_stock']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    order_gen_mapped = AWSBatchOperator(
                task_id='magento-order_gen_mapped',
                job_name='magento_order_gen_mapped',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento order_gen_mapped']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    start >> catalog_product_flat_1 >> [disabled_skus, eav_attribute, eav_attribute_option_swatch, sales_order, sales_order_item] >> magento_customerbalance >> [magento_customerbalance_history, celebrity_am_log, magento_product_catalog, sku_brands] >> sku_categories >> [customer_demographic_info, notify_out_of_stock, order_gen_mapped] >> end

except Exception as e:
    catalog_product_flat_1.log.info(e)