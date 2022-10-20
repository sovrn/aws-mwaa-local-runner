from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG('trigger_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    parent = DummyOperator(
        task_id='parent_task',
    )

    trigger_target = TriggerDagRunOperator(
            task_id='parent_trigger',
            trigger_dag_id='child_dag'
        )

    parent >> trigger_target