import os
from airflow import DAG
import boto3
from airflow.operators.postgres_operator import PostgresOperator
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
    dag_id='SANDBOX_DAILY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='30 3,6,8 * * *',
    dagrun_timeout=timedelta(minutes=120),
    tags=['REDSHIFT'])

try:
    #Sandbox tasks
    sandbox = DummyOperator(
                task_id='sandbox',
                dag=dag)

    end = DummyOperator(
                task_id='end',
                dag=dag)

    customer_retention = PostgresOperator(
                task_id='redshift-customer_retention',
                sql='queries/sandbox/customer_retention.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    emt_events_report_ext = PostgresOperator(
                task_id='redshift-emt_events_report_ext',
                sql='queries/sandbox/emt_events_report_ext.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    aging_sell_through_table = PostgresOperator(
                task_id='redshift-aging_sell_through_table',
                sql='queries/sandbox/aging_sell_through_table.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    firebase_page_loaded_ctr_summary = PostgresOperator(
                task_id='redshift-firebase_page_loaded_ctr_summary',
                sql='queries/sandbox/firebase_page_loaded_ctr_summary.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    instock_disabled_sku = PostgresOperator(
                task_id='redshift-instock_disabled_sku',
                sql='queries/sandbox/instock_disabled_sku.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    mkt_order_summary = PostgresOperator(
                task_id='redshift-mkt_order_summary',
                sql='queries/sandbox/mkt_order_summary.sql',
                postgres_conn_id='redshift',
                dag=(dag))
    
    sandbox >> [customer_retention, emt_events_report_ext, aging_sell_through_table, firebase_page_loaded_ctr_summary, instock_disabled_sku, mkt_order_summary] >> end 

except Exception as e:
    sandbox.log.info(e)