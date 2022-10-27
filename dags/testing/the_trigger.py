from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG('the_trigger', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    the_trigger = TriggerDagRunOperator(
        task_id='the_trigger',
        trigger_dag_id='the_target',
        conf={'message': 'Hello, world'}
    )

    the_trigger