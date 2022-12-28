import os
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_modify_cluster import EmrModifyClusterOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
from airflow.models import Variable, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.state import State

JOB_FLOW_ROLE = os.getenv('EMR_JOB_FLOW_ROLE', 'EMR_EC2_DefaultRole')
SERVICE_ROLE = os.getenv('EMR_SERVICE_ROLE', 'EMR_DefaultRole')

execution_date = """{{ tomorrow_ds_nodash }}"""

SPARK_STEPS = [
    {
        'Name': 'Enable Debugging',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        "Name": "Mobile Data Enrichment",
        'HadoopJarStep': {
            "Args": [
                "spark-submit",
                "--class", "ProviderDataEnrichment",
                "--deploy-mode", "cluster",  # must be 'cluster' for auto-termination
                "s3://{code_bucket}/dd/provider_data_enrichment/provider-data-enrichment-LATEST.jar".format(
                    code_bucket=Variable.get('ENV_code_bucket')),
                "{}".format(execution_date),  # param: Day
                "s3://sovrn-daas-scanbuy", # param source S3 bucket
                "mobile_data", # param inputPrefix
                "mobile_data_enriched" # param outputPrefix
            ],
            "MainClass": "",
            "Properties": [],
            "Jar": "command-runner.jar"
        },
        "ActionOnFailure": "TERMINATE_JOB_FLOW"
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'scanbuy-data-enrichment-spark',
    'LogUri' :'s3://dsol-mwaa-content-prd-ue2/emr-logs/data-enrichment-dev',
    'Configurations': [
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "true"}
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.submit.deployMode": "cluster",
                "spark.driver.maxResultSize": "20G",
                "spark.yarn.maxAppAttempts": "1",
                "spark.sql.shuffle.partitions": "250",
                "spark.sql.broadcastTimeout": "60m",
                "spark.sql.autoBroadcastJoinThreshold": "-1",
                "spark.broadcast.blockSize": "256m",
                "spark.shuffle.service.enabled": "false",
                "spark.dynamicAllocation.enabled": "false"
            }
        },
        {
            "Classification": "emrfs-site",
            "Properties": {
                "fs.s3.canned.acl": "BucketOwnerFullControl"
            }
        },
        {
            "Classification": "capacity-scheduler",
            "Properties": {
                "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
            },
        }
    ],
    'Tags': [{'Key': 'Name',
              'Value': 'scanbuy-enrichment-spark-prd'},
             {'Key': 'Account', 'Value': 'Sovrn'},
             {'Key': 'Environment', 'Value':'prd'},
             {'Key': 'Application', 'Value': 'ScanbuyDataEnrichment'},
             {'Key': 'Owner', 'Value': 'dd'},
             {'Key': 'BusinessUnit', 'Value': 'DaaS Business'},  # what is the actual business unit we want?
             {'Key': 'Compliance', 'Value': 'None'},
             {'Key': 'Country', 'Value': 'us'},
             {'Key': 'Product', 'Value': 'Data'},
             {'Key': 'LifeCycle', 'Value': 'hourly'}
             ],
    'ReleaseLabel': 'emr-5.20.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceFleets': [
            {
                "Name": "MasterFleet",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": [{"InstanceType": "r5.4xlarge"}]
            },
            {
                "Name": "CoreFleet",
                "InstanceFleetType": "CORE",
                "TargetSpotCapacity": 250,
                "LaunchSpecifications": {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": 15,
                        "TimeoutAction": "SWITCH_TO_ON_DEMAND"
                    }
                },
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": "r5.4xlarge",
                        "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 128, "VolumeType" : "gp2"}}]},
                        "BidPriceAsPercentageOfOnDemandPrice": 100,
                        "WeightedCapacity": 16
                    },
                    {
                        "InstanceType": "r4.4xlarge",
                        "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 128, "VolumeType" : "gp2"}}]},
                        "BidPriceAsPercentageOfOnDemandPrice": 100,
                        "WeightedCapacity": 16
                    },
                    {
                        "InstanceType": "r5.12xlarge",
                        "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 384, "VolumeType" : "gp2"}}]},
                        "BidPriceAsPercentageOfOnDemandPrice": 100,
                        "WeightedCapacity": 48
                    },
                    {
                        "InstanceType": "r4.8xlarge",
                        "EbsConfiguration": {"EbsBlockDeviceConfigs": [{"VolumeSpecification": {"SizeInGB": 256, "VolumeType" : "gp2"}}]},
                        "BidPriceAsPercentageOfOnDemandPrice": 100,
                        "WeightedCapacity": 32
                    }
                ]
            }
        ],
        'Ec2SubnetId': 'subnet-0c8ce8d0a597cfb25',
        'KeepJobFlowAliveWhenNoSteps': False,
    },
    'JobFlowRole': JOB_FLOW_ROLE,
    'ServiceRole': SERVICE_ROLE,
}

with DAG(
        dag_id='scanbuy_data_enrichment_spark_v2',
        schedule_interval='0 15 * * *',
        start_date=datetime(2021, 1, 1),
        catchup=False,
) as dag:
    def s3_data_missing(**context):
        s3_task = data_enrichment_check
        if TaskInstance(s3_task, context['execution_date']).current_state() != State.SUCCESS:
            return 'skip_emr_processing'
        else:
            return 'create_job_flow'

    data_enrichment_check = S3PrefixSensor(
        task_id="data_enrichment_check_incoming",
        bucket_name='sovrn-daas-scanbuy',
        prefix='mobile_data/{{ tomorrow_ds_nodash }}',
        poke_interval=60,
        timeout=60,
        aws_conn_id = 'aws_dat_access',
        dag=dag)

    job_branching = BranchPythonOperator(
        task_id='job_branch',
        python_callable=s3_data_missing,
        provide_context=True,
        trigger_rule='all_done',
        retries=0,
        dag=dag
    )

    job_finished = DummyOperator(
        task_id='skip_emr_processing',
        dag=dag
    )

    job_flow_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id = 'aws_prd_access'
    )
    
    job_sensor = EmrJobFlowSensor(
        task_id='check_job_flow',
        job_flow_id=job_flow_creator.output,
        aws_conn_id = 'aws_prd_access',
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=job_flow_creator.output,
        aws_conn_id = 'aws_prd_access',
        steps=SPARK_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=job_flow_creator.output,
        aws_conn_id = 'aws_prd_access',
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id=job_flow_creator.output,
        aws_conn_id = 'aws_prd_access'
    )
    
data_enrichment_check >> job_branching >> job_flow_creator >> step_adder >> step_checker >> cluster_remover
job_branching >> job_finished

# data_enrichment_check.set_downstream(job_branching)
# job_flow_creator.set_upstream(job_branching)
# job_finished.set_upstream(job_branching)

# job_flow_creator.set_downstream(step_adder)
# step_adder.set_downstream(step_checker)
# step_checker.set_downstream(cluster_remover)
