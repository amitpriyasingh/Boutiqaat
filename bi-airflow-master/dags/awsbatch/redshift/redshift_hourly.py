from airflow import DAG
import boto3
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
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
    dag_id='REDSHIFT_HOURLY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='30 2-19 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['REDSHIFT'])

# hourly: nav_item_sku_master order_details aoi_update_sku_sales aoi_sku_not_sellable nav_sku_stock_location purch_hourly sales_stats_per_hour nav_item_sales_price_history nav_not_sellable_items_qty nav_total_items_qty ofs_sales_orders nav_soh_entry_log

try:

    nav_sku_stock_location = PostgresOperator(
                task_id='redshift-nav_sku_stock_location',
                sql='queries/analytics/nav_sku_stock_location.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    mk_sales_order_items = PostgresOperator(
                task_id='redshift-mk_sales_order_items',
                sql='queries/analytics/mk_sales_order_items.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    sales_stats_monthly_weekly = PostgresOperator(
                task_id='redshift-sales_stats_monthly_weekly',
                sql='queries/analytics/sales_stats_monthly_weekly.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    magento_celeb_prod = PostgresOperator(
                task_id='redshift-magento_celeb_prod',
                sql='queries/analytics/magento_celeb_prod.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    parent_child_sku_mapping = PostgresOperator(
                task_id='redshift-parent_child_sku_mapping',
                sql='queries/analytics/parent_child_sku_mapping.sql',
                postgres_conn_id='redshift',
                dag=(dag))


    sales_stats_per_hour = PostgresOperator(
                task_id='redshift-sales_stats_per_hour',
                sql='queries/analytics/sales_stats_per_hour.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    nav_item_sales_price_history = PostgresOperator(
                task_id='redshift-nav_item_sales_price_history',
                sql='queries/analytics/nav_item_sales_price_history.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    
    start = DummyOperator(
                task_id='start',
                dag=dag)

    end = DummyOperator(
                task_id='end',
                dag=dag)
    
    start >> mk_sales_order_items >> [sales_stats_per_hour,sales_stats_monthly_weekly] >> end

    start >> [nav_sku_stock_location,magento_celeb_prod,parent_child_sku_mapping,nav_item_sales_price_history] >> end

except Exception as e:
    t1.log.info(e)