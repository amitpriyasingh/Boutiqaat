import boto3
import logging
from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow import AirflowException
from datetime import datetime, timedelta

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
    dag_id='OFS_INCREMENTAL',  
    default_args=default_args,
    catchup=False,
    schedule_interval='*/5 4-18 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['OFS'])

def get_max_mysql(connection_name, schema,table_name, column):
    logging.info('Executing: SELECT max(' + str(column) + ') FROM ' +str(schema)+'.'+str(table_name))
    hook = MySqlHook(mysql_conn_id=connection_name)
    output = hook.get_records('SELECT max(' + str(column) + ') FROM ' +str(schema)+'.'+str(table_name))
    logging.info(output[0][0])
    return output[0][0]

def get_max_redshift(connection_name, schema,table_name, column):
    logging.info('Executing: SELECT max(' + str(column) + ') FROM ' +str(schema)+'.'+str(table_name))
    hook = PostgresHook(postgres_conn_id=connection_name)
    output = hook.get_records('SELECT max(' + str(column) + ') FROM ' +str(schema)+'.'+str(table_name))
    logging.info(output[0][0])
    return output[0][0]

def compare(mysql_table_name, redshift_table_name, mysql_column, redshift_column):
    """Print the Airflow context and ds variable from the context."""
    max_mysql = get_max_mysql(connection_name='ofs_db', schema='OFS', table_name=mysql_table_name,column=mysql_column)
    max_redshift = get_max_redshift(connection_name='redshift', schema='OFS', table_name=redshift_table_name,column=redshift_column)
    logging.info('Comparing: '+str(max_mysql)+ ' TO '+str(max_redshift))
    if max_mysql==max_redshift:
        return 0
    else:
        return 1
        
def branch_func(input_task, cont_task, stop_task, **kwargs):
    ti = kwargs['ti']
    xcom_value = int(ti.xcom_pull(task_ids='{}'.format(input_task)))
    if xcom_value == 1:
        return '{}'.format(cont_task)
    else:
        return '{}'.format(stop_task)


try:
    start = DummyOperator(
                task_id='start',
                dag=dag)

    end = DummyOperator(
                task_id='end',
                dag=dag)

    check_isl = PythonOperator(
        task_id='check_inbound_sales_line',
        python_callable=compare,
        op_kwargs={'mysql_table_name':'InboundSalesLine', 
                'mysql_column':'ItemId',
                'redshift_table_name':'inbound_sales_line',
                'redshift_column':'item_id'},
        dag=dag,
    )
    
    branch_isl = BranchPythonOperator(
    task_id='branch_isl',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={'input_task':'check_inbound_sales_line','cont_task':'ofs-inbound-sales-line-incremental-1', 'stop_task':'end'},
    dag=dag)

    isl_inc = AWSBatchOperator(
                task_id='ofs-inbound-sales-line-incremental-1',
                job_name='ofs_inbound_sales_line_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS inbound_sales_line_incremental']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    check_isl >> branch_isl >> [isl_inc, end]
    isl_inc >> end
    
    check_ioa = PythonOperator(
        task_id='check_inbound_order_address',
        python_callable=compare,
        op_kwargs={'mysql_table_name':'InboundOrderAddress',
                'mysql_column':'Id',
                'redshift_table_name':'inbound_order_address',
                'redshift_column':'id'},
        dag=dag,
    )
    
    branch_ioa = BranchPythonOperator(
    task_id='branch_ioa',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={'input_task':'check_inbound_order_address','cont_task':'ofs-inbound-order-address-incremental-1', 'stop_task':'end'},
    dag=dag)

    ioa_inc = AWSBatchOperator(
                task_id='ofs-inbound-order-address-incremental-1',
                job_name='ofs_inbound_order_address_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS inbound_order_address_incremental']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    check_ioa >> branch_ioa >> [ioa_inc, end]
    ioa_inc >> end
    
    check_os = PythonOperator(
        task_id='check_order_status',
        python_callable=compare,
        op_kwargs={'mysql_table_name':'OrderStatus',
                'mysql_column':'Id',
                'redshift_table_name':'order_status',
                'redshift_column':'id'},
        dag=dag,
    )
    
    branch_os = BranchPythonOperator(
    task_id='branch_os',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={'input_task':'check_order_status','cont_task':'ofs-order-status-incremental-1', 'stop_task':'end'},
    dag=dag)

    os_inc = AWSBatchOperator(
                task_id='ofs-order-status-incremental-1',
                job_name='ofs_order_status_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS order_status_incremental']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))
    
    check_os >> branch_os >> [os_inc, end]
    os_inc >> end
    
    check_obd = PythonOperator(
        task_id='check_order_batch_details',
        python_callable=compare,
        op_kwargs={'mysql_table_name':'OrderBatchDetails',
                'mysql_column':'InsertedOn',
                'redshift_table_name':'order_batch_details',
                'redshift_column':'inserted_on'},
        dag=dag,
    )
    
    branch_obd = BranchPythonOperator(
    task_id='branch_obd',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={'input_task':'check_order_batch_details','cont_task':'ofs-order-batch-details-incremental-1', 'stop_task':'end'},
    dag=dag)

    obd_inc = AWSBatchOperator(
                task_id='ofs-order-batch-details-incremental-1',
                job_name='ofs_order_batch_details_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS order_batch_details_incremental']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    check_obd >> branch_obd >> [obd_inc, end]
    obd_inc >> end
    
    check_ish = PythonOperator(
        task_id='check_inbound_sales_header',
        python_callable=compare,
        op_kwargs={'mysql_table_name':'InboundSalesHeader',
                'mysql_column':'Id',
                'redshift_table_name':'inbound_sales_header',
                'redshift_column':'id'},
        dag=dag,
    )
    
    branch_ish = BranchPythonOperator(
    task_id='branch_ish',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={'input_task':'check_inbound_sales_header','cont_task':'ofs-inbound-sales-header-incremental-1', 'stop_task':'end'},
    dag=dag)

    ish_inc = AWSBatchOperator(
                task_id='ofs-inbound-sales-header-incremental-1',
                job_name='ofs_inbound_sales_header_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS inbound_sales_header_incremental']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    check_ish >> branch_ish >> [ish_inc, end]
    ish_inc >> end
    
    check_ipl = PythonOperator(
        task_id='check_inbound_payment_line',
        python_callable=compare,
        op_kwargs={'mysql_table_name':'InboundPaymentLine',
                'mysql_column':'Id',
                'redshift_table_name':'inbound_payment_line',
                'redshift_column':'id'},
        dag=dag,
    )
    
    branch_ipl = BranchPythonOperator(
    task_id='branch_ipl',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={'input_task':'check_inbound_payment_line','cont_task':'ofs-inbound-payment-line-incremental-1', 'stop_task':'end'},
    dag=dag)

    ipl_inc = AWSBatchOperator(
                task_id='ofs-inbound-payment-line-incremental-1',
                job_name='ofs_inbound_payment_line_incremental',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C OFS inbound_payment_line_incremental']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    check_ipl >> branch_ipl >> [ipl_inc, end]
    ipl_inc >> end

    start >> [check_ipl, check_ioa, check_ish, check_isl, check_obd, check_os]

except Exception as e:
    start.log.info(e)