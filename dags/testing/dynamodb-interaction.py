from datetime import datetime

from util.aws.boto3 import Boto3

from airflow import DAG
from airflow.decorators import task

with DAG(
    'dynamodb-interaction',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:

    @task
    def get_snowflake_customer_settings():
        boto3 = Boto3('dynamodb')
        print(boto3.get_dynamo_table_items('snowflake-delivery-customer-settings'))

    get_snowflake_customer_settings()