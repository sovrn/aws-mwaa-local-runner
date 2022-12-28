from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3_list_prefixes import S3ListPrefixesOperator

from airflow import DAG

exec_date = "{{ ds_nodash }}"
hour = "{{ '{:02}'.format(execution_date.hour) }}"
dt_hour = str(exec_date+hour)

def print_prefixes(**kwargs):
    print(kwargs['list'])

with DAG(
    's3_list_objects',
    start_date=datetime(1970, 1, 1),
    catchup=False,
) as dag:
    list_s3_prefix = S3ListPrefixesOperator(
        task_id='list_s3_prefix',
        bucket='sovrn-rollups',
        prefix='third_party_id/dt='+dt_hour+'/',
        delimiter='/',
        aws_conn_id='aws_dat_access'
    )

    print_result = PythonOperator(
        task_id="print_result",
        python_callable=print_prefixes,
        op_kwargs={'list': list_s3_prefix.output}
    )

    list_s3_prefix >> print_result