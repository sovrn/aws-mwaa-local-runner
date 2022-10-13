import string
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.decorators import task

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

SQL_TEXT = "SHOW VIEWS LIKE '%_GDPR'"

def get_ddl(results):
    ddl_dict={}

    for row in results:
        view_name = str(row["name"])
        ddl_string = str(row["text"])
        ddl_string = ddl_string.translate( #Remove all whitespace and convert to lowercase
            {ord(c): None for c in string.whitespace}
        ).lower()

        ddl_dict[view_name] = ddl_string
    
    return ddl_dict

@task
def check_for_gdpr_validation(ti=None):
    ddl_dict = ti.xcom_pull(task_ids="get_ddl_task", key="return_value")

    for view_name, ddl_string in ddl_dict.items():
        if "gdpr=true" in ddl_string:
            print(view_name, "GDPR Check is present")
        else:
            print(view_name, "GDPR Check not present")

with DAG(
    'snowflake-minimal-gdpr-validation',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    get_ddl_task = SnowflakeOperator(
            snowflake_conn_id = 'snowflake_conn',
            task_id='get_ddl_task',
            sql=f'{SQL_TEXT}',
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
            handler=get_ddl,
            dag=dag
        )

    get_ddl_task >> check_for_gdpr_validation()