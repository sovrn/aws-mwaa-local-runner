from airflow.providers.amazon.aws.hooks.dynamodb import AwsDynamoDBHook

AWS_CONN_ID = 'aws_ops_access'

class Boto3:
    def set_dynamo_attr_false(table_name, key_field, key, attr_name):
        client = AwsDynamoDBHook(
            aws_conn_id=AWS_CONN_ID,
            client_type='dynamodb'
        ).conn
        
        client.update_item(
            TableName=table_name,
            Key={f'{key_field}': {
                'S': f'{key}'
            }},
            UpdateExpression='SET #attr = :value',
            ExpressionAttributeNames={
                '#attr' : f'{attr_name}'
            },
            ExpressionAttributeValues={
                ':value': {'BOOL': False}
            }
        )