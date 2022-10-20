from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DAG_ID = "spawn_multiple_tasks"

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

SQL_TEXT = 'CALL SP_AIRFLOW_TEST_SLEEP()'

def give_me_a_list():
    return [1,2,3]

def run_sql(i):
    return SnowflakeOperator(
        task_id=f'run_sql_{i}',
        sql=SQL_TEXT,
        autocommit=False,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=print_result
    )

def print_result(cursor):
    for row in cursor:
        print(str(row))

with DAG(
    DAG_ID,
    default_args={'snowflake_conn_id': f'{SNOWFLAKE_CONN_ID}'},
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:

    for i in give_me_a_list():
        run_sql(i)

dag