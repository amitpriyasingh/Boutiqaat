
import logging
from pprint import pprint

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow import AirflowException

args = {
    'owner': 'test',
    'start_date': days_ago(0),
    'email': ['bi@boutiqaat.com',
                'y.raghav@boutiqaat.com',
                'v.garg@boutiqaat.com',
                'a.singh@boutiqaat.com',
                'm.selvaraj@boutiqaat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='EXAMPLE_DAG',
    default_args=args,
    schedule_interval=None,
    tags=['branch']
)

start_op = BashOperator(
    task_id='alert_test',
    bash_command="task False",
    xcom_push=True,
    dag=dag)
