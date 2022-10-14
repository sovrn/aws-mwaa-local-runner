from airflow.providers.amazon.aws.hooks.dynamodb import AwsDynamoDBHook

class Boto3:
    def set_dynamo_attr_false(table_name, key_field, key, attr_name):
        client = AwsDynamoDBHook(client_type='dynamodb').conn
        
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