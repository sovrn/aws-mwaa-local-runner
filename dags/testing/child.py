from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG('child_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    child_1 = DummyOperator(
        task_id='child_task_1'
    )

    child_2 = DummyOperator(
        task_id='child_task_2',
    )

    child_1 >> child_2