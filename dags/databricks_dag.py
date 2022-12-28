from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

default_args = {
  'owner': 'airflow'
}

exec_date = "{{ yesterday_ds_nodash }}"
hour = "{{ '{:02}'.format(execution_date.hour) }}"
exec_hour="{0}{1}".format(exec_date,hour)

with DAG('databricks_dag',
  start_date = days_ago(2),
  schedule_interval = '55 * * * *',
  default_args = default_args,
  catchup=False
  ) as dag:

  opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_now',
    notebook_params= {"date_time": exec_hour},
    databricks_conn_id = 'databricks_default',
    job_id = 659505510517638
  )