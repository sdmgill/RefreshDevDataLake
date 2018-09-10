"""
DAG to Reresh Dev DataLake with data from the Parquet area of Prod
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 16),
    'email': ['sean.gill@portsamerica.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('RefreshDevDataLake', default_args=default_args, schedule_interval= '0 0 * * *')

clear_command = "/home/ubuntu/airflow/bash_scripts/refreshdevdatalake/clearstaledatafromdevdatalake.sh"
if os.path.exists(clear_command):
    t1 = BashOperator(
        task_id='clear_stale_data',
        bash_command='clear_command',
        dag=dag)
else:
    raise Exception("Cannot locate {}".format(clear_command))

copy_command = "/home/ubuntu/airflow/bash_scripts/refreshdevdatalake/rehydratedevdatalake.sh"
if os.path.exists(clear_command):
    t2 = BashOperator(
        task_id='rehydrate_dev_datalake',
        bash_command='copy_command',
        dag=dag)
else:
    raise Exception("Cannot locate {}".format(copy_command))


t1.set_downstream(t2)