from datetime import datetime

from util.aws.sns import SNS

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

with DAG(
    'sns_interaction',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    @task
    def publish_to_sns_topic():
        sns = SNS()
        sns.publish_to_target(
            target_arn=f'{Variable.get("splunk_sns_arn")}',
            alarm_name='Snowflake delivery failure',
            new_state_value='ALARM',
            new_state_reason='failure',
            description='One or more customers failed to receive data from Snowflake'
        )

    publish_to_sns_topic()