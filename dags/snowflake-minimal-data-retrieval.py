from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DAG_ID = "snowflake-minimal-data-retrieval"

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

SQL_TEXT = 'SELECT 1 AS COL1;'

def print_result(cursor):
    for row in cursor:
        print('the sql result is: '+str(row["COL1"]))

with DAG(
    DAG_ID,
    default_args={'snowflake_conn_id': f'{SNOWFLAKE_CONN_ID}'},
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    retrieve_data_task = SnowflakeOperator(
        task_id='retrieve_data_task',
        sql=f'{SQL_TEXT}',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=print_result
    )

    retrieve_data_task

dag