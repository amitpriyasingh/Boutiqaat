"""Example DAG demonstrating the usage of the PythonOperator."""

import time
import logging
from pprint import pprint
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow import AirflowException

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_python_operator',
    default_args=args,
    schedule_interval=None,
    tags=['example']
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
    max_mysql = get_max_mysql(connection_name='ofs_slave', schema='aoi', table_name='order_details',column='order_date')
    max_redshift = get_max_redshift(connection_name='redshift', schema='aoi', table_name='order_details',column='order_date')
    pprint(max_mysql)
    pprint(max_redshift)
    if max_mysql==max_redshift:
        raise AirflowException('Tables are up to date')
      

# [START howto_operator_python]
def print_context(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


run_only_this = PythonOperator(
    task_id='check_mysql_conn',
    python_callable=compare,
    #op_kwargs={'connection_name': 'redshift', 'schema':'aoi', 'table_name':'order_details', 'column':'order_date'},
    dag=dag,
)

# run_this = PythonOperator(
#     task_id='print_the_context',
#     python_callable=print_context,
#     dag=dag,
# )
# [END howto_operator_python]


# [START howto_operator_python_kwargs]
def my_sleeping_function(random_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)


# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
for i in range(5):
    task = PythonOperator(
        task_id='sleep_for_' + str(i),
        python_callable=my_sleeping_function,
        op_kwargs={'random_base': float(i) / 10},
        dag=dag,
    )

    run_only_this >> task
# [END howto_operator_python_kwargs]


def callable_virtualenv():
    """
    Example function that will be performed in a virtual environment.
    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    from colorama import Fore, Back, Style
    from time import sleep
    print(Fore.RED + 'some red text')
    print(Back.GREEN + 'and with a green background')
    print(Style.DIM + 'and in dim text')
    print(Style.RESET_ALL)
    for _ in range(10):
        print(Style.DIM + 'Please wait...', flush=True)
        sleep(10)
    print('Finished')


virtualenv_task = PythonVirtualenvOperator(
    task_id="virtualenv_python",
    python_callable=callable_virtualenv,
    requirements=[
        "colorama==0.4.0"
    ],
    system_site_packages=False,
    dag=dag,
)

also_run_this = BashOperator(
    task_id='also_run_this',
    bash_command='aws s3 ls s3://btq-etl/test/garima/',
    dag=dag,
)