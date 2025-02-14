import string
from datetime import datetime

from util.util import Util
from util.aws.dynamo_db import DynamoDB

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

SQL_TEXT_STAGES = "SHOW STAGES LIKE '%_GDPR'"
SQL_TEXT_VIEWS = "SHOW VIEWS LIKE '%_GDPR'"

DYANMO_TABLE_NAME = 'dsol_splunk_urgent'
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

with DAG(
    'gdpr_validation',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
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
                dynamo_db = DynamoDB()

                dynamo_db.set_attr_false(
                    DYANMO_TABLE_NAME,
                    DYNAMO_KEY_FIELD,
                    dynamo_key,
                    'active'
                )
                # Prepares an alert to be sent
                fail_check = True
        
        if fail_check:
            raise AirflowException("GDPR or Audience check failed")

    trigger_snowflake_delivery = TriggerDagRunOperator(
        task_id='trigger_snowflake_delivery',
        trigger_rule='all_done',
        trigger_dag_id='snowflake_delivery',
        # purely to pass dt_hour from the superset to the delivery
        conf={'dt_hour': '{{ dag_run.conf.get("dt_hour") }}'}
    )

    [get_gdpr_stages, get_gdpr_views] >> check_for_gdpr_validation() >> trigger_snowflake_delivery