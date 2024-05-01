import boto3
import json
import os
import logging
from datetime import datetime

class AdvancedDICOMManagementSystem:
    def __init__(self, config_file_path):
        self.load_configuration(config_file_path)
        self.s3 = boto3.client('s3', region_name=self.config['aws_region'])
        self.ec2 = boto3.resource('ec2', region_name=self.config['aws_region'])
        self.sagemaker = boto3.client('sagemaker', region_name=self.config['aws_region'])
        self.rds = boto3.client('rds', region_name=self.config['aws_region'])
        self.efs = boto3.client('efs', region_name=self.config['aws_region'])
        self.dynamodb = boto3.client('dynamodb', region_name=self.config['aws_region'])
        self.cloudwatch = boto3.client('cloudwatch', region_name=self.config['aws_region'])
        self.setup_logging()

    def load_configuration(self, file_path):
        """Load configuration from a JSON file."""
        with open(file_path, 'r') as config_file:
            self.config = json.load(config_file)
        logging.info("Configuration loaded successfully.")

    def setup_logging(self):
        """Configure the logging."""
        logging.basicConfig(level=logging.DEBUG, filename='dicom_system.log', filemode='a',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def create_s3_buckets(self):
        """Create S3 buckets for DICOM data storage."""
        for bucket in self.config['s3_buckets']:
            try:
                self.s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={
                    'LocationConstraint': self.config['aws_region']
                })
                logging.info(f"Bucket {bucket} created successfully.")
            except Exception as e:
                logging.error(f"Failed to create bucket {bucket}: {str(e)}")

    def launch_ec2_instances(self):
        """Launch EC2 instances for DICOM processing."""
        try:
            instances = self.ec2.create_instances(
                ImageId=self.config['ec2_image_id'],
                MinCount=self.config['ec2_min_count'],
                MaxCount=self.config['ec2_max_count'],
                InstanceType=self.config['ec2_instance_type'],
                KeyName=self.config['ec2_key_pair'],
                SecurityGroupIds=self.config['ec2_security_groups'],
                SubnetId=self.config['ec2_subnet_id'],
                TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'DICOMProcessingInstance'}]}]
            )
            for instance in instances:
                logging.info(f"Launched EC2 instance {instance.id}.")
        except Exception as e:
            logging.error(f"Failed to launch EC2 instances: {str(e)}")

    def configure_rds_instance(self):
        """Configure RDS instance for metadata storage."""
        try:
            response = self.rds.create_db_instance(
                DBName=self.config['rds_db_name'],
                DBInstanceIdentifier='dicomdb',
                AllocatedStorage=20,
                DBInstanceClass='db.t2.micro',
                Engine='postgres',
                MasterUsername='admin',
                MasterUserPassword='admin123',
                VpcSecurityGroupIds=[self.config['rds_vpc_security_group_id']],
                Tags=[{'Key': 'Name', 'Value': 'DICOMDatabase'}]
            )
            logging.info(f"RDS instance {response['DBInstance']['DBInstanceIdentifier']} created.")
        except Exception as e:
            logging.error(f"Failed to create RDS instance: {str(e)}")

    def deploy_efs_system(self):
        """Deploy EFS for shared file storage among EC2 instances."""
        try:
            response = self.efs.create_file_system(
                CreationToken='dicomEfs',
                PerformanceMode='generalPurpose',
                Encrypted=True,
                Tags=[{'Key': 'Name', 'Value': 'DICOMEFS'}]
            )
            logging.info(f"EFS system {response['FileSystemId']} created.")
        except Exception as e:
            logging.error(f"Failed to create EFS system: {str(e)}")

    def setup_dynamodb_for_sessions(self):
        """Setup DynamoDB for session management."""
        try:
            self.dynamodb.create_table(
                TableName='DICOMSessions',
                KeySchema=[{'AttributeName': 'session_id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'session_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
            )
            logging.info("DynamoDB table for DICOM sessions created.")
        except Exception as e:
            logging.error(f"Failed to create DynamoDB table: {str(e)}")

    def monitor_system_health(self):
        """Monitor system health and setup CloudWatch alarms."""
        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName='HighCPUUtilization',
                MetricName='CPUUtilization',
                Namespace='AWS/EC2',
                Statistic='Average',
                Period=300,
                EvaluationPeriods=2,
                Threshold=75.0,
                ComparisonOperator='GreaterThanThreshold',
                AlarmActions=['arn:aws:sns:us-west-2:123456789012:alarm-action'],
                InsufficientDataActions=[],
                TreatMissingData='missing'
            )
            logging.info("CloudWatch alarm for high CPU utilization set up.")
        except Exception as e:
            logging.error(f"Failed to set up CloudWatch alarm: {str(e)}")

# Example usage
config_path = 'path/to/config.json'
dicom_system = AdvancedDICOMManagementSystem(config_path)
dicom_system.create_s3_buckets()
dicom_system.launch_ec2_instances()
dicom_system.configure_rds_instance()
dicom_system.deploy_efs_system()
dicom_system.setup_dynamodb_for_sessions()
dicom_system.monitor_system_health()
