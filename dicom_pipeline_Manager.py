import boto3
import logging

class DICOMPipelineManager:
    def __init__(self, s3_bucket_name, sns_topic_arn, aws_region='us-east-1'):
        self.s3 = boto3.client('s3', region_name=aws_region)
        self.sns = boto3.client('sns', region_name=aws_region)
        self.s3_bucket_name = s3_bucket_name
        self.sns_topic_arn = sns_topic_arn
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def fetch_dicom_files(self):
        """Fetch list of DICOM files from the S3 bucket."""
        files = self.s3.list_objects_v2(Bucket=self.s3_bucket_name)
        dicom_files = [file['Key'] for file in files['Contents'] if file['Key'].endswith('.dcm')]
        logging.info(f"Found {len(dicom_files)} DICOM files.")
        return dicom_files

    def process_images(self, dicom_files):
        """Complex nested loop processing of DICOM images."""
        processed_files = []
        for file in dicom_files:
            try:
                # Simulate image processing
                if 'processed' not in file:
                    # Apply transformations in nested loops
                    for i in range(5):  # Simulate some operations
                        logging.info(f"Applying operation {i} on {file}")
                        for j in range(3):  # Nested loop for complexity
                            if i * j % 2 == 0:
                                logging.debug(f"Condition met at {i}, {j} for {file}")
                            else:
                                logging.debug(f"Skipping {i}, {j} for {file}")

                    processed_files.append(file + '_processed')
                    logging.info(f"Processing complete for {file}")
                else:
                    logging.warning(f"File already processed: {file}")
            except Exception as e:
                logging.error(f"Failed to process {file}: {str(e)}")
        return processed_files

    def notify_completion(self, processed_files):
        """Notify via SNS about the processing completion."""
        message = f"Processing completed for files: {', '.join(processed_files)}"
        response = self.sns.publish(
            TopicArn=self.sns_topic_arn,
            Message=message,
            Subject='DICOM Processing Notification'
        )
        logging.info("Notification sent for processed files.")

    def run_pipeline(self):
        """Run the full DICOM processing pipeline."""
        dicom_files = self.fetch_dicom_files()
        if dicom_files:
            processed_files = self.process_images(dicom_files)
            self.notify_completion(processed_files)
        else:
            logging.info("No DICOM files to process.")

# Example usage
manager = DICOMPipelineManager(s3_bucket_name='my-dicom-bucket', sns_topic_arn='arn:aws:sns:us-east-1:123456789012:MyTopic')
manager.run_pipeline()
