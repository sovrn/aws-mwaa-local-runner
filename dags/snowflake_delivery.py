from datetime import datetime
from typing import Iterable

from util.aws.dynamo_db import DynamoDB

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.exceptions import AirflowException

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'DATA_EXPORT_WH'

SQL_TEXT_DELIVERIES = "CALL SP_DELIVER_WEBLOG_DATA('{dt_hour}', '{view}')"

DYANMO_TABLE_NAME = 'snowflake-delivery-customer-settings'

def get_snowflake_customers() -> Iterable:
    dynamo_db = DynamoDB()

    return dynamo_db.get_table_items(DYANMO_TABLE_NAME)

# Ideally this function should come from utils, but not sure if the airflow templates are available from outside a dag
def get_dt_hour():
    exec_date = "{{ ds_nodash }}"
    hour = "{{ '{:02}'.format(execution_date.hour) }}"
    dt_hour = "{0}{1}".format(exec_date, hour)

    return dt_hour

def get_delivery_error_status(cursor):
    for row in cursor:
        if 'failed' in str(row).lower():
            raise AirflowException('Delivery failed with error '+str(row))

def deliver_data(dt_hour, customer):
    view = customer['view']

    deliver_data = SnowflakeOperator(
        snowflake_conn_id = 'snowflake_conn',
        task_id=f"deliver_{view.lower()}",
        sql=SQL_TEXT_DELIVERIES.format(dt_hour=dt_hour, view=view),
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=get_delivery_error_status
    )
    
    deliver_data.set_upstream(run_deliveries)
    deliver_data.set_downstream(trigger_alert)

    return deliver_data

def log_failure():
    print('One or more delivery tasks failed')

with DAG(
    'snowflake_delivery',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    run_deliveries = DummyOperator(
        task_id='run_deliveries'
    )

    # This task will show green (succeeded) when at least one delivery fails or orange (skipped) otherwise
    trigger_alert = PythonOperator(
        task_id='trigger_alert',
        trigger_rule='one_failed',
        python_callable=log_failure
    )

    run_deliveries

    # Airflow dynamic tasks should be used here (v2.3.0), but this workaround is necessary for Airflow v2.2.2
    for customer in get_snowflake_customers():
        dt_hour = get_dt_hour()

        if customer['active']:
            deliver_data(dt_hour, customer)