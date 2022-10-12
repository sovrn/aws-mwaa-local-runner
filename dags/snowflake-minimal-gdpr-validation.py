import string
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DAG_ID = "snowflake-minimal-gdpr-validation"

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

SNOWFLAKE_VIEW = 'VW_WEB_SHARETHIS_GDPR'
SQL_TEXT = "SHOW VIEWS LIKE '%_GDPR'"

def validate_gdpr(cursor):
    for row in cursor:
        ddl_string = str(row["text"]) #text column contains DDL used to create the view
        ddl_string = ddl_string.translate({ord(c): None for c in string.whitespace}).lower() #Remove all whitespace and convert to lc
        print(ddl_string)
        if "gdpr=true" in ddl_string:
            print("GDPR Check is present")
        else:
            print("GDPR check not present") 
        
with DAG(
    DAG_ID,
    default_args={'snowflake_conn_id': f'{SNOWFLAKE_CONN_ID}'},
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    gdpr_validation_task = SnowflakeOperator(
        task_id='gdpr_validation_task',
        sql=f'{SQL_TEXT}',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        handler=validate_gdpr
    )

    gdpr_validation_task

dag