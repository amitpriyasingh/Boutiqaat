from airflow import DAG
import boto3
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
    dag_id='REDSHIFT_TASKS',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['REDSHIFT'])

try:
    sku_live_status_report = AWSBatchOperator(
                    task_id='redshift-sku_live_status_report',
                    job_name='redshift_sku_live_status_report',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift sku_live_status_report']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    brand_performance = AWSBatchOperator(
                    task_id='redshift-brand_performance',
                    job_name='redshift_brand_performance',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift brand_performance']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    in_stock_remark_report = AWSBatchOperator(
                    task_id='redshift-in_stock_remark_report',
                    job_name='redshift_in_stock_remark_report',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift in_stock_remark_report']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    mkt_roi_automation = AWSBatchOperator(
                    task_id='redshift-mkt_roi_automation',
                    job_name='redshift_mkt_roi_automation',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift mkt_roi_automation']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    global_score_report_daily = AWSBatchOperator(
                    task_id='redshift-global_score_report_daily',
                    job_name='redshift_global_score_report_daily',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift global_score_report_daily']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    seven_days_attr_report = AWSBatchOperator(
                    task_id='redshift-7days_attr_report',
                    job_name='redshift_7days_attr_report',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift 7days_attr_report']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    sale_traffic_ranking = AWSBatchOperator(
                    task_id='redshift-sale_traffic_ranking',
                    job_name='redshift_sale_traffic_ranking',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift sale_traffic_ranking']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    customer_retention = AWSBatchOperator(
                    task_id='redshift-customer_retention',
                    job_name='redshift_customer_retention',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift customer_retention']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    firebase_page_loaded_ctr_summary = AWSBatchOperator(
                    task_id='redshift-firebase_page_loaded_ctr_summary',
                    job_name='redshift_firebase_page_loaded_ctr_summary',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift firebase_page_loaded_ctr_summary']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    nav_item_sku_master = AWSBatchOperator(
                    task_id='redshift-nav_item_sku_master',
                    job_name='redshift_nav_item_sku_master',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift nav_item_sku_master']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    order_details = AWSBatchOperator(
                    task_id='redshift-order_details',
                    job_name='redshift_order_details',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift order_details']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    aoi_update_sku_sales = AWSBatchOperator(
                    task_id='redshift-aoi_update_sku_sales',
                    job_name='redshift_aoi_update_sku_sales',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift aoi_update_sku_sales']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    aoi_sku_not_sellable = AWSBatchOperator(
                    task_id='redshift-aoi_sku_not_sellable',
                    job_name='redshift_aoi_sku_not_sellable',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift aoi_sku_not_sellable']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    nav_sku_stock_location = AWSBatchOperator(
                    task_id='redshift-nav_sku_stock_location',
                    job_name='redshift_nav_sku_stock_location',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift nav_sku_stock_location']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    sales_stats_per_hour = AWSBatchOperator(
                    task_id='redshift-sales_stats_per_hour',
                    job_name='redshift_sales_stats_per_hour',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift sales_stats_per_hour']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    nav_item_sales_price_history = AWSBatchOperator(
                    task_id='redshift-nav_item_sales_price_history',
                    job_name='redshift_nav_item_sales_price_history',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift nav_item_sales_price_history']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    nav_not_sellable_items_qty = AWSBatchOperator(
                    task_id='redshift-nav_not_sellable_items_qty',
                    job_name='redshift_nav_not_sellable_items_qty',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift nav_not_sellable_items_qty']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    nav_total_items_qty = AWSBatchOperator(
                    task_id='redshift-nav_total_items_qty',
                    job_name='redshift_nav_total_items_qty',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift nav_total_items_qty']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    ofs_sales_orders = AWSBatchOperator(
                    task_id='redshift-ofs_sales_orders',
                    job_name='redshift_ofs_sales_orders',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift ofs_sales_orders']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    nav_soh_entry_log = AWSBatchOperator(
                    task_id='redshift-nav_soh_entry_log',
                    job_name='redshift_nav_soh_entry_log',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift nav_soh_entry_log']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    # inventory_health = AWSBatchOperator(
    #                 task_id='redshift-inventory_health',
    #                 job_name='redshift_inventory_health',
    #                 job_queue='batch-job-queue-b',
    #                 job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
    #                 max_retries=4200,
    #                 overrides={'command': ['make -C redshift inventory_health']},
    #                 aws_conn_id=None,
    #                 array_properties={},
    #                 parameters={},
    #                 status_retries=10,
    #                 region_name='eu-west-1',
    #                 dag=(dag))

    stock_balance_by_store_loc = AWSBatchOperator(
                    task_id='redshift-stock_balance_by_store_loc',
                    job_name='redshift_stock_balance_by_store_loc',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift stock_balance_by_store_loc']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    supplier_stock_movement = AWSBatchOperator(
                    task_id='redshift-supplier_stock_movement',
                    job_name='redshift_supplier_stock_movement',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift supplier_stock_movement']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    mk_sales_order_items = AWSBatchOperator(
                    task_id='redshift-mk_sales_order_items',
                    job_name='redshift_mk_sales_order_items',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift mk_sales_order_items']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    sales_stats_per_hour = AWSBatchOperator(
                    task_id='redshift-sales_stats_per_hour',
                    job_name='redshift_sales_stats_per_hour',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift sales_stats_per_hour']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    sales_stats_monthly_weekly = AWSBatchOperator(
                    task_id='redshift-sales_stats_monthly_weekly',
                    job_name='redshift_sales_stats_monthly_weekly',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift sales_stats_monthly_weekly']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    magento_celeb_prod = AWSBatchOperator(
                    task_id='redshift-magento_celeb_prod',
                    job_name='redshift_magento_celeb_prod',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift magento_celeb_prod']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    parent_child_sku_mapping = AWSBatchOperator(
                    task_id='redshift-parent_child_sku_mapping',
                    job_name='redshift_parent_child_sku_mapping',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift parent_child_sku_mapping']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))
                    
    aging_sell_through_table = AWSBatchOperator(
                    task_id='redshift-aging_sell_through_table',
                    job_name='redshift_aging_sell_through_table',
                    job_queue='batch-job-queue-b',
                    job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                    max_retries=4200,
                    overrides={'command': ['make -C redshift aging_sell_through_table']},
                    aws_conn_id=None,
                    array_properties={},
                    parameters={},
                    status_retries=10,
                    region_name='eu-west-1',
                    dag=(dag))

    sku_live_status_report >> [brand_performance, in_stock_remark_report, mkt_roi_automation, global_score_report_daily, seven_days_attr_report, sale_traffic_ranking, customer_retention, firebase_page_loaded_ctr_summary] >>  nav_item_sku_master >> order_details >> aoi_update_sku_sales >> [aoi_sku_not_sellable, nav_sku_stock_location, sales_stats_per_hour, nav_item_sales_price_history, nav_not_sellable_items_qty, nav_total_items_qty, ofs_sales_orders, nav_soh_entry_log, stock_balance_by_store_loc, supplier_stock_movement, mk_sales_order_items, sales_stats_per_hour, sales_stats_monthly_weekly, magento_celeb_prod, aging_sell_through_table] >> parent_child_sku_mapping
except Exception as e:
    sku_live_status_report.log.info(e)
