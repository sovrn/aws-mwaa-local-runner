from datetime import datetime

from dags.util.aws.dynamo_db import DynamoDB

from airflow import DAG
from airflow.decorators import task

with DAG(
    'dynamodb_interaction',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    @task
    def get_snowflake_customer_settings():
        dynamo_db = DynamoDB()
        print(dynamo_db.get_table_items('snowflake-delivery-customer-settings'))

    get_snowflake_customer_settings()