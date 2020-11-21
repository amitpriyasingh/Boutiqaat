from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'amit',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 20),
    'email': ['bi@boutiqaat.com','a.singh@boutiqaat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ANALYTICS_DAILY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='30 3,6,8 * * *',
    dagrun_timeout=timedelta(minutes=120),
    tags=['REDSHIFT'])

def SOHDailyToS3():
    import airflow.hooks.S3_hook
    from airflow.hooks.postgres_hook import PostgresHook
    import pandas as pd 
    from io import StringIO
    import datetime as dt  
    postgres_hook = PostgresHook(postgres_conn_id='redshift')
    s3_hook = airflow.hooks.S3_hook.S3Hook('s3connection')
    bucket = 'btq-bi'
    key = 'soh_'+str(dt.datetime.now().strftime('%d%m%y'))+'.csv'
    query = """select sku,config_sku,brand,gender,category1,category2,category3,category4,category_manager,bar_code,boutiqaat_exclusive,
            vendor_item_no, vendor_no,contract_type,payment_term_code,country_code,enable_date,last_selling_price,special_price,
            last_item_cost,last_item_cost_currency,shipping_cost_per_unit,first_grn_date,last_grn_date,total_grn_qty,total_grn_value,
            total_sellable_qty,toal_nav_non_sellable,soh,crs_available,nav2crs_total,full_pending_open_po_qty,partially_pending_open_po_qty,
            partial_pending_open_po_total_qty,partial_pending_open_po_received_qty,sku_avg_cost_2020,
            COALESCE(stock_refreshed_datetime,(select distinct stock_refreshed_datetime from analytics.soh_report where stock_refreshed_datetime is not null limit 1)) as stock_refreshed_datetime,
            report_time::date 
            from analytics.soh_report sr;"""

    df_ = postgres_hook.get_pandas_df(query)
    csv_buf = StringIO()
    df_.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    filename = csv_buf.getvalue()
    s3_hook.load_string(filename, key, bucket, replace=True)
    return True 


try:
    customer_retention = PostgresOperator(
                task_id='redshift-customer_retention',
                sql='queries/analytics/customer_retention.sql',
                postgres_conn_id='redshift',
                dag=(dag))

    soh_to_s3 = PythonVirtualenvOperator(
      task_id="task_api_to_s3",
      python_callable=SOHDailyToS3,
      requirements=[
          "pandas"
      ],
      system_site_packages=True,
      dag=(dag),
      )
                
    customer_repeat_rate = PostgresOperator(
                task_id='redshift-customer_repeat_rate',
                sql='queries/analytics/customer_repeat_rate.sql',
                postgres_conn_id='redshift',
                dag=(dag))
                
                
    start = DummyOperator(
                task_id='start',
                dag=dag)

    end = DummyOperator(
                task_id='end',
                dag=dag)

    
    start >> [customer_retention,soh_to_s3] >> customer_repeat_rate >> end 

except Exception as e:
    start.log.info(e)
