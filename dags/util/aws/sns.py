from airflow.providers.amazon.aws.hooks.sns import AwsSnsHook

#AWS_CONN_ID = 'aws_ops_access'

class SNS:
    def __init__(self):
        self.hook = AwsSnsHook()