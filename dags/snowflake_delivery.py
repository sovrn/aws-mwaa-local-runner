from datetime import datetime
from typing import Iterable

from util.aws.dynamo_db import DynamoDB

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'DATA_EXPORT_WH'

SQL_TEXT_DELIVERIES = "CALL SP_DELIVER_WEBLOG_DATA('{dt_hour}', '{view}')"

DYANMO_TABLE_NAME = 'snowflake-delivery-customer-settings'

on_call_sns = Variable.get("splunk_sns_arn")

def get_snowflake_customers() -> Iterable:
    dynamo_db = DynamoDB()

    return dynamo_db.get_table_items(DYANMO_TABLE_NAME)

def get_delivery_error_status(cursor):
    for row in cursor:
        if 'failed' in str(row).lower():
            raise AirflowException('Delivery failed with error '+str(row))

def deliver_data(customer):
    view = customer['view']

    deliver_data = SnowflakeOperator(
        snowflake_conn_id = 'snowflake_conn',
        task_id=f"deliver_{view.lower()}",
        sql=SQL_TEXT_DELIVERIES.format(dt_hour='{{ dag_run.conf.get("dt_hour") }}', view=view),
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=get_delivery_error_status
    )
    
    deliver_data.set_upstream(run_deliveries)
    
    deliver_data.set_downstream(send_alert_failure)
    deliver_data.set_downstream(send_alert_success)

    return deliver_data

with DAG(
    'snowflake_delivery',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    run_deliveries = DummyOperator(
        task_id='run_deliveries'
    )

    send_alert_failure = SnsPublishOperator(
        task_id='send_sns_fail',
        target_arn=on_call_sns,
        message="""{
        "AlarmName":"Snowflake-Delivery-"""+'{{ dag_run.conf.get("dt_hour") }}'+"""\",
        "NewStateValue":"ALARM",
        "NewStateReason":"failure",
        "StateChangeTime":"2022-10-14T01:00:00.000Z",
        "AlarmDescription":"Snowflake-Delivery failed."}""",
        trigger_rule='one_failed'
    )

    send_alert_success = SnsPublishOperator(
        task_id='send_sns_success',
        target_arn=on_call_sns,
        message="""{
        "AlarmName":"Snowflake-Delivery"""+'{{ dag_run.conf.get("dt_hour") }}'+"""\",
        "NewStateValue":"OK",
        "NewStateReason":"success",
        "StateChangeTime":"2022-10-14T01:00:00.000Z",
        "AlarmDescription":"Snowflake-Delivery failed."}""",
        trigger_rule='all_success'
    )

    run_deliveries

    # Airflow dynamic tasks should be used here (v2.3.0), but this workaround is necessary for Airflow v2.2.2
    for customer in get_snowflake_customers():

        if customer['active']:
            deliver_data(customer)