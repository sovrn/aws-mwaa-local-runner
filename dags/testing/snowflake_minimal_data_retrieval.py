from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.exceptions import AirflowException

DAG_ID = "snowflake_minimal_data_retrieval"

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

SQL_TEXT = 'CALL SP_AIRFLOW_TEST();'

def print_result(cursor):
    for row in cursor:
        print(str(row))

        if 'failed' in str(row).lower():
            raise AirflowException('Delivery failed with error '+str(row))

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