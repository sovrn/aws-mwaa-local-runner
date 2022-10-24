from datetime import datetime

from util.aws.sns import SNS

from airflow import DAG
from airflow.decorators import task

with DAG(
    'sns_interaction',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    @task
    def publish_to_sns_topic():
        sns = SNS()
        sns.hook.publish_to_target(
            target_arn="arn:aws:sns:us-east-2:713386270820:splunk-on-call",
            message="""{
                "AlarmName":"VictorOps - CloudWatch Integration TEST",
                "NewStateValue":"ALARM",
                "NewStateReason":"failure",
                "StateChangeTime":"1970-01-01T01:00:00.000Z",
                "AlarmDescription":"VictorOps - CloudWatch Integration TEST"
            }"""
        )

    publish_to_sns_topic()