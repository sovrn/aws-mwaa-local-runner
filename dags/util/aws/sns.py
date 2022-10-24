from airflow.providers.amazon.aws.hooks.sns import AwsSnsHook

class SNS:
    def __init__(self):
        self.hook = AwsSnsHook()

    def publish_to_target(self, target_arn, alarm_name, new_state_value, new_state_reason, description):
        self.hook.publish_to_target(
            target_arn=f"{target_arn}",
            message=f"""{{
                "AlarmName":"{alarm_name}",
                "NewStateValue":"{new_state_value}",
                "NewStateReason":"{new_state_reason}",
                "StateChangeTime":"1970-01-01T01:00:00.000Z",
                "AlarmDescription":"{description}"
            }}"""
        )