import configparser
import os

# Load configuration
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.conf'))




# AWS Configuration
AWS_CONN_ID = config.get("aws", "aws_conn_id")
AWS_DATA_BUCKET_NAME = config.get("aws", "aws_data_bucket_name")
AWS_SCRIPT_BUCKET_NAME = config.get("aws", "aws_script_bucket_name")
AWS_REGION=config.get("aws", "aws_region")

# Glue Configuration
GLUE_DATABASE = config.get("glue", "glue_database")
GLUE_TABLE = config.get("glue", "glue_table")
GLUE_TABLE_PROCESSED = config.get("glue", "glue_table_processed")
GLUE_JOB_NAME = config.get("glue", "glue_job_name")
GLUE_CRAWLER = config.get("glue", "glue_crawler")
IAM_ROLE = config.get("glue", "iam_role")
S3_INPUT_TARGET_PATH = config.get("glue", "s3_input_target_path")
S3_OUTPUT_TARGET_PATH = config.get("glue", "s3_output_target_path")
S3_SCRIPT_LOCATION = config.get("glue", "s3_script_location")