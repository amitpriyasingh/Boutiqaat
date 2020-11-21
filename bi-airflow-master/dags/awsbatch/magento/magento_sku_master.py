from airflow import DAG
import boto3
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'omar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': [
        'o.alkhairat@boutiqaat.com',
        'v.garg@boutiqaat.com',
        'a.singh@boutiqaat.com',
        'y.raghav@boutiqaat.com',
        'm.selvam@boutiqaat.com'
        ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='MAGENTO_SKU_MASTER', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2,4,10,12 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['MAGENTO'])

# catalog_product_flat_1 disabled_skus eav_attribute eav_attribute_option_swatch sales_order sales_order_item magento_customerbalance magento_customerbalance_history celebrity_am_log magento_product_catalog sku_brands sku_categories customer_demographic_info notify_out_of_stock order_gen_mapped

try:

    # magento_sku_master = AWSBatchOperator(
    #             task_id='magento_sku_master',
    #             job_name='magento_sku_master',
    #             job_queue='batch-job-queue-b',
    #             job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
    #             max_retries=4200,
    #             overrides={'command': ['make -C magento sku_master']},
    #             aws_conn_id=None,
    #             array_properties={},
    #             parameters={},
    #             status_retries=10,
    #             region_name='eu-west-1',
    #             dag=(dag))
    start = DummyOperator(
                task_id='start',
                dag=dag)


    magento_sku_master = AWSBatchOperator(
                task_id='redshift-magento-sku-master-1',
                job_name='redshift_magento_sku_master',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C redshift magento_sku_master']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    sku_status_history = AWSBatchOperator(
                task_id='magento-sku-status-history-1',
                job_name='magento_sku_status_history',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento sku_status_history']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    catalog_product_entity_decimal = AWSBatchOperator(
                task_id='magento-catalog-product-entity-decimal-1',
                job_name='magento_catalog_product_entity_decimal',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento catalog_product_entity_decimal']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    celebrity_product = AWSBatchOperator(
                task_id='magento-celebrity-product-1',
                job_name='magento_celebrity_product',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento celebrity_product']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    eav_attribute_option_value = AWSBatchOperator(
                task_id='magento-eav-attribute-option-value-1',
                job_name='magento_eav_attribute_option_value',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento eav_attribute_option_value']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    catalog_product_entity_int = AWSBatchOperator(
                task_id='magento-catalog-product-entity-int-1',
                job_name='magento_catalog_product_entity_int',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento catalog_product_entity_int']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    catalog_product_entity = AWSBatchOperator(
                task_id='magento-catalog-product-entity-1',
                job_name='magento_catalog_product_entity',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento catalog_product_entity']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    catalog_product_relation = AWSBatchOperator(
                task_id='magento-catalog-product-relation-1',
                job_name='magento_catalog_product_relation',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento catalog_product_relation']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    catalog_product_entity_text = AWSBatchOperator(
                task_id='magento-catalog-product-entity-text-1',
                job_name='magento_catalog_product_entity_text',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento catalog_product_entity_text']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    catalog_product_entity_varchar = AWSBatchOperator(
                task_id='magento-catalog-product-entity-varchar-1',
                job_name='magento_catalog_product_entity_varchar',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento catalog_product_entity_varchar']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
            
    start >> [sku_status_history, catalog_product_entity_decimal, celebrity_product, eav_attribute_option_value, catalog_product_entity_int, catalog_product_entity, catalog_product_relation, catalog_product_entity_text, catalog_product_entity_varchar] >> magento_sku_master

except Exception as e:
    magento_sku_master.log.info(e)