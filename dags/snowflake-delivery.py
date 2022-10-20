from re import L
import string
from datetime import datetime
from typing import Iterable

from util.util import Util
from util.aws.boto3 import Boto3

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'DATA_EXPORT_WH'

SQL_TEXT_STAGES = "SHOW STAGES LIKE '%_GDPR'"
SQL_TEXT_VIEWS = "SHOW VIEWS LIKE '%_GDPR'"
SQL_TEXT_DELIVERIES = "CALL SP_DELIVER_WEBLOG_DATA('{dt_hour}', '{view}')"

DYANMO_TABLE_NAME = 'snowflake-delivery-customer-settings'
DYNAMO_KEY_FIELD = 'staging_prefix'

# Returns a dictionary of GDPR stages and with their corresponding URLs, which will eventually
# be parsed into DyanmoDB keys
def save_stage_urls(results):
    stage_dict = {}

    for row in results:
        stage_name = str(row['name'])
        url = str(row['url'])

        stage_dict[stage_name] = url

    return stage_dict

# Returns a dictionary of GDPR views and the DDL that was used to create them. This DDL will be
# checked for the correct compliance filters
def save_view_ddl(results):
    view_dict={}

    for row in results:
        view_name = str(row["name"])
        ddl_string = str(row["text"])
        ddl_string = ddl_string.translate( #Remove all whitespace and convert to lowercase
            {ord(c): None for c in string.whitespace}
        ).lower()

        view_dict[view_name] = ddl_string
    
    return view_dict

def get_snowflake_customers() -> Iterable:
    boto3 = Boto3('dynamodb')

    return boto3.get_dynamo_table_items(DYANMO_TABLE_NAME)

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
    deliver_data.set_upstream(delivery_grouping)

    return deliver_data

with DAG(
    'snowflake-delivery',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    get_gdpr_stages = SnowflakeOperator(
        snowflake_conn_id = 'snowflake_conn',
        task_id='get_gdpr_stages',
        sql=f'{SQL_TEXT_STAGES}',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=save_stage_urls
    )

    get_gdpr_views = SnowflakeOperator(
        snowflake_conn_id = 'snowflake_conn',
        task_id='get_gdpr_views',
        sql=f'{SQL_TEXT_VIEWS}',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=save_view_ddl
    )

    delivery_grouping = DummyOperator(
        task_id='run_deliveries',
        trigger_rule='all_done'
    )

    @task
    def check_for_gdpr_validation(ti=None):
        stage_dict = ti.xcom_pull(task_ids='get_gdpr_stages', key='return_value')
        view_dict = ti.xcom_pull(task_ids='get_gdpr_views', key='return_value')

        fail_check = False

        for stage_name, url in stage_dict.items():
            view_name = Util.get_view_from_stage(stage_name)

            dynamo_key = Util.parse_for_key(url)
            ddl_string = view_dict[view_name]

            if "gdpr=true" and "audience_approved=true" in ddl_string:
                pass
            else:
                # Deactivates the customer's delivery
                boto3 = Boto3('dynamodb')

                boto3.set_dynamo_attr_false(
                    DYANMO_TABLE_NAME,
                    DYNAMO_KEY_FIELD,
                    dynamo_key,
                    'active'
                )
                # Prepares an alert to be sent
                fail_check = True
        
        if fail_check:
            raise AirflowException("GDPR or Audience check failed")

    [get_gdpr_stages, get_gdpr_views] >> check_for_gdpr_validation() >> delivery_grouping

    # Airflow dynamic tasks should be used here (v2.3.0), but this workaround is necessary for Airflow v2.2.2
    for customer in get_snowflake_customers():
        dt_hour = get_dt_hour()

        if customer['active']:
            deliver_data(dt_hour, customer)