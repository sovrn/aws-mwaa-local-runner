import os
import time
from datetime import datetime
import logging

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_modify_cluster import EmrModifyClusterOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
from airflow.models import Variable, TaskInstance, BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State

JOB_FLOW_ROLE = os.getenv('EMR_JOB_FLOW_ROLE', 'EMR_EC2_DefaultRole')
SERVICE_ROLE = os.getenv('EMR_SERVICE_ROLE', 'EMR_DefaultRole')
exec_date = "{{ dag_run.conf.get('exec_date') }}"
dash_date = "{{ ds }}"
hour = "{{ dag_run.conf.get('hour') }}"
dt_hour = str(exec_date+hour)
aws_env = str(Variable.get('ENV_aws_account'))
on_call_sns = str(Variable.get('splunk_sns_arn'))

AWS_CONN_ID = 'aws_'+aws_env+'_access'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 12, 12, 55, 0),
    'retries': 360,
    'params': {
        'repo_name': 'daas-feeds-spark',
        's3_region_name': 'us-east-2',
        'emr_configs_all_envs': {
            'timeout': int(60 * 60 * 5),  # 5 hour max runtime
            'poke_interval': 15  # 15 seconds
        },
        'data_in_full_paths': {
            'commerce_weblog': 's3://sovrn-ingest/weblog',
            'cookie_store': 's3://sovrn-prd-ue2-general-exchange/cookie_store',
            'signal': 's3://sovrn-ingest/signal-pages',
            'requests': "s3://sovrn-daas-prd-ue2/ad_activity_databiz",
            'hashed_email_service': "s3://sovrn-rollups/hashed_email_service",
            'url_category': 's3://sovrn-data-prd/reference.db/page_categorization',
            'countries_ref': 's3://sovrn-data-prd/reference.db/countries',
            'cookie_he_prs_staging': 's3://sovrn-daas-temp/daas.db/cookie_he_prs_staging'

        },
        'data_in': {
            'commerce_weblog': {
                'bucket': 'sovrn-ingest',
                'prefix': 'weblog'
            }
        }
    }
}


repo_name = 'daas-feeds-spark'


# environment-dependent params (featuring data output locations for spark b/c we are not using airflow variables for
# that anymore))
env_vars = {
    'dev': {
        'exec_hour':"{0}{1}".format(exec_date,hour),
        'third_party_id_lookback': "1",  # lookback hours for creating reduced 3pid output
        'third_party_id_reduced_lookback': "1",  # loookback hours to use in the superset from the reduced set
        'hashed_email_lookback': "1",
        'data_out_3pid_reduced': {
            'bucket': 'simplifi-test-bucket',
            'prefix': '3pid-reduced'
        },
        'data_out_3pid_flattened': {
            'bucket': 'simplifi-test-bucket',
            'prefix': '3pid-flattened'
        },
        'data_out_superset': {
            'bucket': 'simplifi-test-bucket',
            'prefix': 'daas-feeds-spark'
        },
        'emr_configs': {
            'fleet_units': 300
        },
        'healthcheck_retry_delay': 30
    },
    'prd': {
        'exec_hour':"{0}{1}".format(exec_date,hour),
        'third_party_id_lookback': "2160",  # lookback hours for creating reduced 3pid output
        'third_party_id_reduced_lookback': "294",  # lookback hours to use in the superset from the reduced set
        'hashed_email_lookback': "168",
        'emr_configs': {
            'fleet_units': 129  # total nodes
        },
        'data_out_3pid_reduced': {
            'bucket': 'sovrn-prd-ue2-general-data',
            'prefix': '3pid-reduced'
        },
        'data_out_3pid_flattened': {
            'bucket': 'sovrn-prd-ue2-general-data',
            'prefix': '3pid-flattened'
        },
        'data_out_superset': {
            'bucket': 'sovrn-prd-ue2-general-data',
            'prefix': 'weblog-superset'
        },
        'healthcheck_retry_delay': 60,
    }
}

STEP_SETUP = [
    {
        'Name': 'Enable Debugging',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    }
]

# [START howto_operator_emr_steps_config]
SPARK_STEPS_3PID = [

    {
        "Name": "Reduce and flatten 3pid data",
        'HadoopJarStep': {
            "Args": [
                "spark-submit",
                # TODO: Check spark class name matches
                "--class", "sovrn.daas.feeds.Reduce3PidsStep",
                "--deploy-mode", "cluster",  # must be 'cluster' for auto-termination
                "--master", "yarn",
                "--driver-cores", "8",
                "--driver-memory", "50G",
                "--executor-cores", "4",
                "--num-executors", "256",
                "--executor-memory", "23G",
                "s3://sovrn-code-{}/dd/daas-feeds-spark/daas-feeds-spark-LATEST.jar".format(aws_env), # path (s3) where jar is located
                env_vars[aws_env]['exec_hour'],  # param: Hour.
                "2",  # this controls the partitioning of the data. since the IDs are hex, this will result in 16^nPC partitions
                env_vars[aws_env]['third_party_id_lookback'],
                env_vars[aws_env]['third_party_id_reduced_lookback'],
                "s3://{0}/{1}".format(env_vars[aws_env]['data_out_3pid_reduced']['bucket'],env_vars[aws_env]['data_out_3pid_reduced']['prefix']),  # path (s3) where application will write reduced data,
                "s3://{0}/{1}".format(env_vars[aws_env]['data_out_3pid_flattened']['bucket'],env_vars[aws_env]['data_out_3pid_flattened']['prefix']),  # path (s3) where application will write reduced data,
                 # path (s3) where application will write reduced data,
                # path (s3) where application will write flattened data
            ],
            "MainClass": "",
            "Properties": [],
            "Jar": "command-runner.jar"
        },
        "ActionOnFailure": "TERMINATE_JOB_FLOW"
    },
]

SPARK_STEPS_SUPERSET = [

    {
        "Name": "Create Superset",
        'HadoopJarStep': {
            "Args": [
                "spark-submit",
                # TODO: Check spark class name matches
                "--class", "sovrn.daas.feeds.DataFeedsMain",
                "--deploy-mode", "cluster",  # must be 'cluster' for auto-termination
                "--master", "yarn",
                "--driver-cores", "8",
                "--driver-memory", "47G",
                "--executor-cores", "4",
                "--num-executors", "256",
                "--executor-memory", "20G",
                "s3://sovrn-code-{}/dd/daas-feeds-spark/daas-feeds-spark-LATEST.jar".format(aws_env), # path (s3) where jar is located
                env_vars[aws_env]['exec_hour'],  # param: Hour.
                env_vars[aws_env]['third_party_id_reduced_lookback'],
                env_vars[aws_env]['hashed_email_lookback'],
                "s3://{0}/{1}".format(env_vars[aws_env]['data_out_superset']['bucket'],env_vars[aws_env]['data_out_superset']['prefix']),# path (s3) where application will write (output) data
                "40000000",  # minPageViews - the min number of records before manual salting kicks in
                "10",  #partitionMultiplier - the number of partitions to split the rows that have more than minPageViews
            ],
            "MainClass": "",
            "Properties": [],
            "Jar": "command-runner.jar"
        },
        "ActionOnFailure": "TERMINATE_JOB_FLOW"
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'daas-feeds-spark-mwaa-{0}{1}'.format(exec_date,hour),
    'LogUri' :'s3://dsol-mwaa-content-prd-ue2/emr-logs/daas-feeds-spark',
    'Configurations': [
        {
            "Classification": "spark",
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.submit.deployMode": "cluster",
                "spark.driver.maxResultSize": "0",
                "spark.yarn.maxAppAttempts": "1",
                "spark.sql.shuffle.partitions": "990"
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
              'Value': 'daas-feeds-spark'},
             {'Key': 'Account', 'Value': 'Sovrn'},
             {'Key': 'Environment', 'Value':'prd'},
             {'Key': 'Application', 'Value': 'daas-feeds-spark'},
             {'Key': 'Owner', 'Value': 'dd'},
             {'Key': 'BusinessUnit', 'Value': 'DaaS Business'},  # what is the actual business unit we want?
             {'Key': 'Compliance', 'Value': 'None'},
             {'Key': 'Country', 'Value': 'us'},
             {'Key': 'Product', 'Value': 'Data'},
             {'Key': 'LifeCycle', 'Value': 'hourly'}
             ],
    'ReleaseLabel': 'emr-6.3.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceFleets': [
            {
                "Name": "MasterFleet",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": [{"InstanceType": "r5.2xlarge"}]
            },
            {
                "Name": "CoreFleet",
                "InstanceFleetType": "CORE",
                "TargetSpotCapacity": 129,
                "LaunchSpecifications": {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": 5,
                        "TimeoutAction": "SWITCH_TO_ON_DEMAND"
                    }
                },
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": "r5.2xlarge",  # m5.large is not supported
                        "BidPriceAsPercentageOfOnDemandPrice": 100,
                        "WeightedCapacity": 1,
                        "EbsConfiguration": {
                            "EbsOptimized": True,
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "VolumeType": "gp2",
                                        "SizeInGB": 100
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        ],
        'Ec2SubnetId': 'subnet-0c8ce8d0a597cfb25',
        'KeepJobFlowAliveWhenNoSteps': False,
    },
    'Steps':STEP_SETUP,
    'JobFlowRole': JOB_FLOW_ROLE,
    'ServiceRole': Variable.get('ENV_emr_service_role'),
}
# [END howto_operator_emr_steps_config]
def check_task_status(task_to_check,
                      downstream_task_if_success,
                      downstream_task_if_failure,
                      downstream_task_if_skipped = None,
                      **context):
    """
    Checks status of task
    :param downstream_task_if_skipped:
    :param downstream_task_if_failure:
    :param downstream_task_if_success:
    :param task_to_check:
    :param context:
    :return: downstream task IDs
    """
    logging.info("task to check:" + str(task_to_check))
    logging.info("downstream_task_if_success:" + downstream_task_if_success)
    logging.info("downstream_task_if_failure:" + downstream_task_if_failure)
    logging.info("downstream_task_if_skipped:" + str(downstream_task_if_skipped))

    ti = TaskInstance(task_to_check, context["{0}{1}".format(exec_date,hour)])

    if ti.current_state() == State.SKIPPED :
        if downstream_task_if_skipped is not None:
            return downstream_task_if_skipped
        else:
            return downstream_task_if_failure
    elif ti.current_state() == State.FAILED :
        return downstream_task_if_failure
    elif ti.current_state() == State.SUCCESS :
        return downstream_task_if_success



with DAG(
        dag_id='daas_feeds_spark_rerun',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
) as dag:
    # [START howto_operator_emr_create_job_flow]
    job_flow_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule='all_done'

    )
    # [END howto_operator_emr_create_job_flow]

    # [START howto_sensor_emr_job_flow]
    job_sensor = EmrJobFlowSensor(
        task_id='check_job_flow',
        aws_conn_id=AWS_CONN_ID,
        job_flow_id=job_flow_creator.output,
    )
    # [END howto_sensor_emr_job_flow]

    # [START howto_operator_emr_modify_cluster]
    # cluster_modifier = EmrModifyClusterOperator(
    #     task_id='modify_cluster', cluster_id=job_flow_creator.output, step_concurrency_level=1
    # )
    # [END howto_operator_emr_modify_cluster]

    # [START howto_operator_emr_add_steps]


    step_adder_3pid = EmrAddStepsOperator(
        task_id='add_steps_3pid',
        job_flow_id=job_flow_creator.output,
        steps=SPARK_STEPS_3PID,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule='one_failed'
    )

    step_adder_superset = EmrAddStepsOperator(
        task_id='add_steps_superset',
        job_flow_id=job_flow_creator.output,
        steps=SPARK_STEPS_SUPERSET,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule='all_done'
    )
    # [END howto_operator_emr_add_steps]

    # [START howto_sensor_emr_step]
    step_checker_3pid = EmrStepSensor(
        task_id='watch_step_3pid',
        job_flow_id=job_flow_creator.output,
        aws_conn_id=AWS_CONN_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps_3pid', key='return_value')[0] }}",
    )
    step_checker_superset = EmrStepSensor(
        task_id='watch_step_superset',
        job_flow_id=job_flow_creator.output,
        aws_conn_id=AWS_CONN_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps_superset', key='return_value')[0] }}",
    )


    # [END howto_sensor_emr_step]

    # [START howto_operator_emr_terminate_job_flow]
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        aws_conn_id=AWS_CONN_ID,
        job_flow_id=job_flow_creator.output,
    )
    # [END howto_operator_emr_terminate_job_flow]

    commerce_health_check = S3KeySensor(
        task_id="commerce_health_check",
        bucket_name='sovrn-ingest',
        bucket_key="weblog/{0}-{1}/_SUCCESS".format(dash_date,hour),
    )

    # job_finished = DummyOperator(
    #     task_id='skip_emr_processing',
    #     dag=dag
    # )


clear_s3_output_prefix = S3DeleteObjectsOperator(
    task_id='clear_s3_output_prefix',
    bucket='sovrn-prd-ue2-general-data',
    prefix='weblog-superset',
    trigger_rule='one_failed'
)


s3_3pid_check_flattened = S3PrefixSensor(
    task_id="s3_3pid_check_flattened",
    bucket_name='sovrn-prd-ue2-general-data',
    prefix='3pid-flattened/dt={0}{1}'.format(exec_date,hour),
    trigger_rule='all_done',
    timeout=1
)

s3_3pid_check_reduced = S3PrefixSensor(
    task_id="s3_3pid_check_reduced",
    bucket_name='sovrn-prd-ue2-general-data',
    prefix='3pid-reduced/dt={0}{1}'.format(exec_date,hour),
    trigger_rule='all_done',
    timeout=1

)

clear_s3_reduced_prefix = S3DeleteObjectsOperator(
    task_id='clear_s3_reduced_prefix',
    bucket='sovrn-prd-ue2-general-data',
    prefix='3pid-reduced/dt={0}{1}'.format(exec_date,hour),
    trigger_rule='one_failed'
)
clear_s3_flattened_prefix = S3DeleteObjectsOperator(
    task_id='clear_s3_flattened_prefix',
    bucket='sovrn-prd-ue2-general-data',
    prefix='3pid-flattened/dt={0}{1}'.format(exec_date,hour),
    trigger_rule='one_failed'
)


emr_succeeded = SnsPublishOperator(
    task_id='emr_succeeded',
    target_arn=on_call_sns,
    message="""{
    "AlarmName":"Daas-Feeds-Spark-"""+exec_date+hour+ """\",
    "NewStateValue":"OK",
    "NewStateReason":"success",
    "StateChangeTime":"2022-10-14T01:00:00.000Z",
    "AlarmDescription":"Daas-feed-spark succeeded."}""",
    trigger_rule='one_success'
)

emr_failed = SnsPublishOperator(
    task_id='emr_failed',
    target_arn=on_call_sns,
    message="""{
    "AlarmName":"Daas-Feeds-Spark-"""+exec_date+hour+"""\",
    "NewStateValue":"ALARM",
    "NewStateReason":"failure",
    "StateChangeTime":"2022-10-14T01:00:00.000Z",
    "AlarmDescription":"Daas-feed-spark failed."}""",

    trigger_rule='one_failed'
)

trigger_gdpr_validation = TriggerDagRunOperator(
    task_id='trigger_gdpr_validation',
    trigger_rule='all_success',
    trigger_dag_id='gdpr_validation',
    conf={"dt_hour": dt_hour}
)

# [third_party_id_health_check, commerce_health_check, one_success, countries_chk, urlcat_chk] >> launch_emr_job_flow

# Once EMR is launched, add steps, and monitor the job
commerce_health_check >> job_flow_creator >> [s3_3pid_check_flattened, s3_3pid_check_reduced] >> step_adder_3pid >> step_adder_superset
step_adder_3pid >> step_checker_3pid
step_adder_superset >> step_checker_superset >> cluster_remover 
# Check status of initial EMR feed
# emr_succeeded >> copy_across_regions
step_checker_superset >> emr_succeeded >> trigger_gdpr_validation
step_checker_superset >> emr_failed

# data_enrichment_check.set_downstream(job_branching)
# job_flow_creator.set_upstream(job_branching)
# job_finished.set_upstream(job_branching)
#
# job_flow_creator.set_downstream(step_adder)
# step_adder.set_downstream(step_checker)
# step_checker.set_downstream(cluster_remover)
#

