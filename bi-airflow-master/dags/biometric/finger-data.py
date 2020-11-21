import os
import requests
import datetime
from pprint import pprint
from os.path import join, dirname
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow import AirflowException


default_args = {
    'owner': 'Sanjay',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': [
            's.shah@boutiqaat.com', 
            'm.varghese@boutiqaat.com'
            ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    dag_id='BIO_FINGER',  
    default_args=default_args,
    catchup=False,
    schedule_interval='0 3 * * *',
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['ODOO'])


def update_finger_data():
    import pandas as pd
    import os
    import requests
    import datetime
    from pprint import pprint
    from os.path import join, dirname
    from airflow.hooks.mssql_hook import MsSqlHook
    
    try:
        yest = datetime.datetime.now() - datetime.timedelta(days=1)
        dt_string = yest.date().strftime("%Y-%m-%d")
        
        hook = MsSqlHook(mssql_conn_id="odoo_finger")
        
        conn = hook.get_conn()
        # what is the output of conn ?
        df = pd.read_sql("SELECT max(bsevtdt) as checkout,min(bsevtdt) as checkin ,user_id from TA.dbo.punchlog where CONVERT (date,createdAt)=CONVERT(date, GETDATE()-1) GROUP by user_id;", conn)
        # catch read_sql connection errors
        attendances=[]
        for line in range(0,len(df)):
            attendances.append({ 'check_in' : df['checkin'][line].isoformat() , 'check_out' :df['checkout'][line].isoformat(),'emp_code' : df['user_id'][line],'index':line})
        DOMAIN = "http://10.0.1.49/b/v1"
        ADD_ATT = DOMAIN + "/attendance/add"
        json_data = {
                'attendances': attendances,
                'tz': 'Asia/Kuwait',
                'name': dt_string,
                'db': 'Boutiquaat_Test',
                'login': 'admin',
                'pswd': 'admin',
        }

        print (json_data,"PPPPPPPPPPPPPP")
        response = requests.post(ADD_ATT, json=json_data)
        print('__________ Response : ')
        pprint(response.json())
        
    except Exception as e:
        raise AirflowException(e)


virtualenv_task = PythonVirtualenvOperator(
    task_id="Odoo_Bioumetric",
    python_callable=update_finger_data,
    requirements=[
        "pandas"
    ],
    system_site_packages=True,
    dag=(dag),
)
