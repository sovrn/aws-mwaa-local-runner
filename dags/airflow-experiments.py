from __future__ import annotations
import string
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.models.xcom_arg import XComArg
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'SOVRN'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

SNOWFLAKE_VIEW = 'VW_WEB_SHARETHIS_GDPR'
SQL_TEXT = "SHOW VIEWS LIKE '%_GDPR'"


def save_ddl_dict(cursor):
    ddl_dict={}

    for row in cursor:
        view_name = str(row["name"])
        ddl_string = str(row["text"])
        ddl_string = ddl_string.translate( #Remove all whitespace and convert to lowercase
            {ord(c): None for c in string.whitespace}
        ).lower()

        ddl_dict[view_name] = ddl_string
    Variable.set(key='gdpr_check_dict', value = ddl_dict)

def print_greeting(view_name):
    print(view_name)

with DAG(
    'test_taskflow',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:
    get_ddl_dict = SnowflakeOperator(
            snowflake_conn_id = 'snowflake_conn',
            task_id='gdpr_validation_task',
            sql=f'{SQL_TEXT}',
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
            handler=save_ddl_dict,
            dag=dag
        )

    ddl_dict = Variable.get('gdpr_check_dict', default_var=['default_city'])

    # for view_name, ddl_string in ddl_dict:
    say_hello = PythonOperator(
        task_id=f'say_hello_from_{ddl_dict}',
        python_callable=print_greeting,
        op_kwargs={'view_name': ddl_dict}
    )