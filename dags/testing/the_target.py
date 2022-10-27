from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

@task(task_id="run_this")
def run_this_func(dag_run=None):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param dag_run: The DagRun object
    """
    print(f"Remotely received value of {dag_run.conf.get('message')} for key=message")

with DAG('the_target', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Here is the message: $message"',
        env={'message': '{{ dag_run.conf.get("message") }}'},
    )

    run_this_func() >> bash_task