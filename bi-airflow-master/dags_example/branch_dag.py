
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
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='branch_dag',
    default_args=args,
    schedule_interval=None,
    tags=['branch']
)


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

def compare():
    """Print the Airflow context and ds variable from the context."""
    max_mysql = get_max_mysql(connection_name='ofs_slave', schema='aoi', table_name='order_details',column='order_at')
    max_redshift = get_max_redshift(connection_name='redshift', schema='aoi', table_name='order_details',column='order_at')
    pprint(max_mysql)
    pprint(max_redshift)
    return max_mysql==max_redshift


def branch_func(input_task, cont_task, stop_task,**kwargs):
    ti = kwargs['ti']
    xcom_value = int(ti.xcom_pull(task_ids='{}'.format(input_task)))
    if xcom_value == False:
        return '{}'.format(cont_task)
    else:
        return ''.format(stop_task)

start_op = BashOperator(
    task_id='start_task',
    bash_command="echo False",
    xcom_push=True,
    dag=dag)

start_py = PythonOperator(
    task_id='start_py',
    python_callable=compare,
    #op_kwargs={'connection_name': 'redshift', 'schema':'aoi', 'table_name':'order_details', 'column':'order_date'},
    dag=dag,
)

branch_op = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_func,
    op_kwargs={'input_task':'start_py', 'cont_task':'continue_task','stop_task':'stop_task'},
    dag=dag)

continue_op = DummyOperator(task_id='continue_task', dag=dag)
stop_op = DummyOperator(task_id='stop_task', dag=dag)

start_py >> branch_op >> [continue_op, stop_op]