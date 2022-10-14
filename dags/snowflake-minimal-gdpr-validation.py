import string
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.dynamodb import AwsDynamoDBHook
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

DYANMO_TABLE_NAME = 'snowflake-delivery-customer-settings'

def set_inactive(dynamo_key):
    client = AwsDynamoDBHook(client_type='dynamodb').conn
    
    client.update_item(
        TableName=DYANMO_TABLE_NAME,
        Key={'staging_prefix': {
            'S': f'{dynamo_key}'
        }},
        UpdateExpression='SET #active = :value',
        ExpressionAttributeNames={
            '#active' : 'active'
        },
        ExpressionAttributeValues={
            ':value': {'BOOL': False}
        }
    )

def parse_for_key(url):
    key_start = url.find('/snowflake')
    key = url[key_start:]

    return key

def get_view_from_stage(stage_name):
    if 'DAILY' in stage_name:
        return stage_name.replace('STG_DAILY', 'VW')
    elif 'INACTIVE' in stage_name:
        return stage_name.replace('STG_INACTIVE', 'VW')
    else:
        return stage_name.replace('STG', 'VW')

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
    'snowflake-minimal-gdpr-validation',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    get_gdpr_stages_task = SnowflakeOperator(
        snowflake_conn_id = 'snowflake_conn',
        task_id='get_gdpr_stages_task',
        sql=f'{SQL_TEXT_STAGES}',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=save_stage_urls,
        dag=dag
    )

    get_gdpr_views_task = SnowflakeOperator(
        snowflake_conn_id = 'snowflake_conn',
        task_id='get_gdpr_views_task',
        sql=f'{SQL_TEXT_VIEWS}',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=save_view_ddl,
        dag=dag
    )

    @task
    def check_for_gdpr_validation(ti=None):
        stage_dict = ti.xcom_pull(task_ids='get_gdpr_stages_task', key='return_value')
        view_dict = ti.xcom_pull(task_ids='get_gdpr_views_task', key='return_value')

        fail_check = False

        for stage_name, url in stage_dict.items():
            view_name = get_view_from_stage(stage_name)

            dynamo_key = parse_for_key(url)
            ddl_string = view_dict[view_name]

            if "gdpr=true" and "audience_approved=true" in ddl_string:
                pass
            else:
                set_inactive(dynamo_key)
                fail_check = True
        
        if fail_check:
            raise AirflowException("GDPR or Audience check failed")

    [get_gdpr_stages_task, get_gdpr_views_task] >> check_for_gdpr_validation()