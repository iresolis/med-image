import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class DICOMImageProcessor:
    def __init__(self, s3_bucket_name, sagemaker_endpoint_name, athena_database, athena_table):
        self.s3 = boto3.client('s3')
        self.sagemaker_runtime = boto3.client('sagemaker-runtime')
        self.athena = boto3.client('athena')
        self.s3_bucket = s3_bucket_name
        self.sagemaker_endpoint = sagemaker_endpoint_name
        self.athena_database = athena_database
        self.athena_table = athena_table

    def upload_dicom_image(self, file_path, object_key):
        """Upload a DICOM image to an S3 bucket."""
        try:
            with open(file_path, 'rb') as file:
                self.s3.upload_fileobj(file, self.s3_bucket, object_key)
                print(f"Uploaded {object_key} to S3 bucket {self.s3_bucket}.")
        except FileNotFoundError:
            print("The file was not found.")
        except NoCredentialsError:
            print("Credentials not available for AWS S3.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def analyze_image_with_sagemaker(self, s3_object_key):
        """Invoke SageMaker endpoint to analyze a DICOM image."""
        try:
            response = self.sagemaker_runtime.invoke_endpoint(
                EndpointName=self.sagemaker_endpoint,
                ContentType='image/jpeg',
                Body=self.s3.get_object(Bucket=self.s3_bucket, Key=s3_object_key)['Body'].read()
            )
            print(f"Analysis results for {s3_object_key}: {response['Body'].read().decode('utf-8')}")
        except Exception as e:
            print(f"Failed to analyze image: {e}")

    def query_dicom_metadata(self):
        """Query metadata from AWS Athena for the DICOM images stored."""
        try:
            query = f"SELECT * FROM {self.athena_table}"
            execution = self.athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': self.athena_database
                },
                ResultConfiguration={
                    'OutputLocation': f"s3://{self.s3_bucket}/query-results/"
                }
            )
            print(f"Query execution ID: {execution['QueryExecutionId']}")
        except Exception as e:
            print(f"Query execution failed: {e}")

    def retrieve_and_process_images(self):
        """Retrieve DICOM images from S3 and process them."""
        try:
            response = self.s3.list_objects_v2(Bucket=self.s3_bucket)
            for item in response.get('Contents', []):
                print(f"Processing {item['Key']}")
                self.analyze_image_with_sagemaker(item['Key'])
        except PartialCredentialsError:
            print("Partial credentials provided. Check your AWS configuration.")
        except Exception as e:
            print(f"Failed to retrieve or process images: {e}")

# Example usage
dicom_processor = DICOMImageProcessor(
    s3_bucket_name='my-medical-dicom-bucket',
    sagemaker_endpoint_name='dicom-image-analysis-endpoint',
    athena_database='dicom_database',
    athena_table='dicom_metadata'
)
dicom_processor.upload_dicom_image('path/to/dicom/image.dcm', 'image1.dcm')
dicom_processor.analyze_image_with_sagemaker('image1.dcm')
dicom_processor.query_dicom_metadata()
dicom_processor.retrieve_and_process_images()
