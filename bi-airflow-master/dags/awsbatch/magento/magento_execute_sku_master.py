
import os
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow import AirflowException

args = {
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
    dag_id='MAGENTO_EXECUTE_SKU_MASTER',
    default_args=args,
    schedule_interval='30 3 * * *',
    tags=['MAGENTO']
)


def execute_from_file(connection_name, query_file):
    try:
        with open(query_file, 'r') as myfile:
            data = myfile.read()
            logging.info('Executing: ' + str(data))
            hook = MySqlHook(mysql_conn_id=connection_name)
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(str(data))
    except Exception as e:
        raise AirflowException(e)

execute_magento_sku_master = PythonOperator(
    task_id='execute_magento_sku_master',
    python_callable=execute_from_file,
    op_kwargs={'connection_name': 'magento_db', 'query_file':'{}/dags/awsbatch/magento/queries/execute_sku_master.sql'.format(os.getcwd())},
    dag=dag,
)