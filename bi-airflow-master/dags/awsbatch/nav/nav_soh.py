import logging
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow import AirflowException

default_args = {
    'owner': 'omar',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['bi@boutiqaat.com', 
                'erp.india@boutiqaat.com',
                'y.raghav@boutiqaat.com',
                'v.garg@boutiqaat.com',
                'a.singh@boutiqaat.com',
                'm.selvaraj@boutiqaat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='NAV_SOH',  
    default_args=default_args,
    catchup=False,
    schedule_interval='0 3,4,5,7,14,18 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['NAV'])



def execute_from_file(connection_name, query):
    try:
        logging.info('Executing: ' + str(query))
        hook = MsSqlHook(mssql_conn_id=connection_name)
        hook.run(str(query))
    except Exception as e:
        raise AirflowException(e)


try:
    master_sync = PythonOperator(
        task_id='nav-master_sync',
        python_callable=execute_from_file,
        op_kwargs={'connection_name': 'nav_master_db', 'query':'exec [Boutiqaat_Live].[dbo].[USP_GetItemStock]'},
        dag=dag,
    )

    aoi_consolidated_sku_stock  = AWSBatchOperator(
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

    nav_sku_master = AWSBatchOperator(
                task_id='nav-sku-master-full-1',
                job_name='nav_sku_master_full',
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

    stock_agg = AWSBatchOperator(
                task_id='stock-agg-full-1',
                job_name='stock_agg_full',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C NAV stock_agg_full']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    nav_product_category = AWSBatchOperator(
                task_id='nav-product-category-full-1',
                job_name='nav_product_category_full',
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

    make_soh_report = AWSBatchOperator(
                task_id='redshift-soh-report-1',
                job_name='redshift_soh_report',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C redshift soh_report']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    unload_soh_report = AWSBatchOperator(
                task_id='redshift-soh-unload-1',
                job_name='unload_soh_report',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C redshift unload_soh_report']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
    unload_product_catalog_report = AWSBatchOperator(
                task_id='redshift-product-catalog-unload-1',
                job_name='unload_product_catalog_report',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C redshift unload_product_catalog_report']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
    
    magento_sku_master = AWSBatchOperator(
                task_id='magento_sku_master',
                job_name='magento_sku_master',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C magento sku_master']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    master_sync >> [aoi_consolidated_sku_stock, magento_sku_master, nav_sku_master, nav_product_category, stock_agg] >> make_soh_report >> unload_soh_report >> unload_product_catalog_report

except Exception as e:
    nav_sku_master.log.info(e)
