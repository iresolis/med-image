import boto3
import logging
import datetime

class MedicalDeviceSupportSystem:
    def __init__(self):
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.sns_client = boto3.client('sns')
        self.lambda_client = boto3.client('lambda')
        self.logs_client = boto3.client('logs')
        self.setup_logging()

    def setup_logging(self):
        """Setup the logging configuration."""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def log_event(self, message, level='info'):
        """Log events into the system log."""
        if level == 'info':
            logging.info(message)
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            logging.error(message)
        elif level == 'critical':
            logging.critical(message)
        else:
            logging.debug(message)

    def create_cloudwatch_alarm(self, alarm_name, metric_name, threshold, comparison_operator='GreaterThanThreshold', evaluation_periods=1):
        """Create a CloudWatch alarm for specific metrics."""
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace='AWS/DICOM',
                Statistic='Average',
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                EvaluationPeriods=evaluation_periods,
                Period=300,
                AlarmActions=['arn:aws:sns:us-west-2:123456789012:alert-topic'],
                Unit='Count'
            )
            self.log_event(f"CloudWatch Alarm {alarm_name} created for metric {metric_name}.")
        except Exception as e:
            self.log_event(f"Error creating CloudWatch alarm: {str(e)}", 'error')

    def send_notification(self, message, topic_arn='arn:aws:sns:us-west-2:123456789012:alert-topic'):
        """Send notifications to an SNS topic."""
        try:
            response = self.sns_client.publish(
                TopicArn=topic_arn,
                Message=message
            )
            self.log_event(f"Notification sent to {topic_arn}: {message}")
            return response
        except Exception as e:
            self.log_event(f"Error sending notification: {str(e)}", 'error')

    def monitor_system_health(self):
        """Monitor system health and generate alerts."""
        response = self.cloudwatch_client.describe_alarms(
            StateValue='ALARM'
        )
        for alarm in response['MetricAlarms']:
            self.send_notification(f"Alarm {alarm['AlarmName']} triggered", alarm['AlarmActions'][0])

    def audit_system_access(self):
        """Audit system access and log unusual access patterns."""
        try:
            query = "fields @timestamp, @message | filter eventName like /ConsoleLogin/ and errorCode like /Failure/"
            start_query_response = self.logs_client.start_query(
                logGroupName='/aws/lambda/dicom-handler',
                startTime=int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp()),
                endTime=int(datetime.datetime.now().timestamp()),
                queryString=query,
            )
            query_id = start_query_response['queryId']
            response = self.logs_client.get_query_results(
                queryId=query_id
            )
            for result in response['results']:
                self.log_event(f"Unusual access pattern detected: {result}", 'warning')
        except Exception as e:
            self.log_event(f"Error auditing system access: {str(e)}", 'error')

    def auto_resolve_issues(self, function_name, payload):
        """Automatically resolve issues by invoking AWS Lambda functions."""
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='Event',
                Payload=payload
            )
            self.log_event(f"Invoked Lambda function {function_name} for automatic issue resolution.")
            return response
        except Exception as e:
            self.log_event(f"Error invoking Lambda function: {str(e)}", 'error')

# Example usage
support_system = MedicalDeviceSupportSystem()
support_system.create_cloudwatch_alarm('HighCPUUtilization', 'CPUUtilization', 80)
support_system.monitor_system_health()
support_system.send_notification("System health check completed.", 'arn:aws:sns:us-west-2:123456789012:alert-topic')
support_system.audit_system_access()
support_system.auto_resolve_issues('auto-resolution-handler', '{"issue_type": "reboot_instance"}')
