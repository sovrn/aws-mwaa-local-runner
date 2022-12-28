from typing import Iterable

from util.aws.dynamo_db import DynamoDB

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.models import Variable

default_args = {
  'owner': 'airflow'
}

exec_date = "{{ yesterday_ds_nodash }}"
hour = "{{ '{:02}'.format(execution_date.hour) }}"
exec_hour="{0}{1}".format(exec_date,hour)

ON_CALL_SNS = Variable.get("dsol_splunk_urgent")

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

  send_alert_failure = SnsPublishOperator(
    task_id='send_sns_fail',
    target_arn=ON_CALL_SNS,
    message="""{
    "AlarmName":"Databricks-Delivery-"""+'{{ dag_run.conf.get("dt_hour") }}'+"""\",
    "NewStateValue":"ALARM",
    "NewStateReason":"failure",
    "StateChangeTime":"2022-10-14T01:00:00.000Z",
    "AlarmDescription":"Databricks delivery failed."}""",
    trigger_rule='one_failed'
  )

  send_alert_success = SnsPublishOperator(
    task_id='send_sns_success',
    target_arn=ON_CALL_SNS,
    message="""{
    "AlarmName":"Databricks-Delivery"""+'{{ dag_run.conf.get("dt_hour") }}'+"""\",
    "NewStateValue":"OK",
    "NewStateReason":"success",
    "StateChangeTime":"2022-10-14T01:00:00.000Z",
    "AlarmDescription":"Databricks delivery succeeded."}""",
    trigger_rule='all_success'
  )

  opr_run_now >> [send_alert_failure, send_alert_success]