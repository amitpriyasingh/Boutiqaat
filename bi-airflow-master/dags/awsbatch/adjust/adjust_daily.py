from airflow import DAG
import boto3
import logging
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.operators.bash_operator import BashOperator
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
    dag_id='ADJUST_DAILY', 
    default_args=default_args,
    catchup=False,
    schedule_interval='0 6 * * *',
    dagrun_timeout=timedelta(minutes=120),
    tags=['ADJUST'])

# s3_sync_adjust redshift_load_external_adjust network_partners transactions attributes_actions

try:
    today = datetime.today()
    yday = today - timedelta(days=1)

    # s3_sync_adjust = BashOperator(
    #     task_id='adjust-s3_sync_adjust',
    #     bash_command="aws s3 sync --exclude '*' --include '*{}*' --exclude '{}/*' s3://adjustclickstream/ s3://btq-etl/adjust/csv/{}/{}/{}".format(
    #             yday.strftime('%Y-%m-%d'), 
    #             yday.strftime('%Y%m'), 
    #             yday.strftime('%Y'),
    #             yday.strftime('%m'),
    #             yday.strftime('%d')
    #         ),
    #     xcom_push=True,
    #     dag=(dag)
    # )

    s3_sync_adjust = AWSBatchOperator(
                task_id='adjust-s3_sync_adjust',
                job_name='adjust_s3_sync_adjust',
                job_queue='batch-job-queue-a',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C adjust s3_sync_adjust']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    redshift_load_external_adjust = AWSBatchOperator(
                task_id='adjust-redshift_load_external_adjust',
                job_name='adjust_redshift_load_external_adjust',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C adjust redshift_load_external_adjust']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    network_partners = AWSBatchOperator(
                task_id='adjust-network_partners',
                job_name='adjust_network_partners',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C adjust network_partners']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    transactions = AWSBatchOperator(
                task_id='adjust-transactions',
                job_name='adjust_transactions',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C adjust transactions']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    attributes_actions = AWSBatchOperator(
                task_id='adjust-attributes_actions',
                job_name='adjust_attributes_actions',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C adjust attributes_actions']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))

    sessions = AWSBatchOperator(
                task_id='adjust-sessions',
                job_name='adjust_sessions',
                job_queue='batch-job-queue-b',
                job_definition='arn:aws:batch:eu-west-1:652586300051:job-definition/boutiqaat-etl-jobs-airflow:1',
                max_retries=4200,
                overrides={'command': ['make -C adjust sessions']},
                aws_conn_id=None,
                array_properties={},
                parameters={},
                status_retries=10,
                region_name='eu-west-1',
                dag=(dag))


    s3_sync_adjust >> redshift_load_external_adjust >> [network_partners, transactions, attributes_actions] >> sessions
except Exception as e:
    attributes_actions.log.info(e)