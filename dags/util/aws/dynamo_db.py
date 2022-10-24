from airflow.providers.amazon.aws.hooks.dynamodb import AwsDynamoDBHook

AWS_CONN_ID = 'aws_ops_access'

class DynamoDB:
    def __init__(self):
        self.client_conn = AwsDynamoDBHook(aws_conn_id=AWS_CONN_ID).conn

    def set_attr_false(self, table_name, key_field, key, attr_name):
        self.client_conn.update_item(
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

    # boto3 API calls return a bunch of data types that aren't needed
    def strip_item_data_types(self, items):
        stripped_items = []

        for item in items:
            info = {}

            info['staging_prefix'] = item['staging_prefix']['S'] # partition key
            info['active'] = item['active']['BOOL'] if 'active' in item.keys() else None
            info['s3_uri'] = item['s3_uri']['S'] if 's3_url' in item.keys() else None
            info['success_file_uri'] = item['success_file_uri']['S'] if 'success_file_uri' in item.keys() else None
            info['view'] = item['view']['S'] if 'view' in item.keys() else None

            stripped_items.append(info)

        return stripped_items

    def get_table_items(self, table_name):
        res = self.client_conn.scan(TableName=table_name)
        items = res['Items']
        items = self.strip_data_types(items)
        
        return items
