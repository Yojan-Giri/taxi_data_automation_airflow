
import boto3
from airflow.hooks.base import BaseHook
import time
def get_s3_client(aws_conn_id):

    try:
        # Fetch connection details from Airflow
        conn = BaseHook.get_connection(aws_conn_id)

        # Extract region from Airflow connection (if set)
        region_name = conn.extra_dejson.get("region_name")  # Default to us-east-1

        # Initialize a Boto3 session with credentials from Airflow
        session = boto3.session.Session(
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name=region_name
        )

        # Return the S3 client
        return session.client("s3")

    except Exception as e:
        print(f"Error creating S3 client: {e}")
        return None

def create_bucket_if_not_exists(s3_client, bucket_name, region):

    try:
        response = s3_client.list_buckets()
        existing_buckets = [bucket["Name"] for bucket in response.get("Buckets", [])]

        if bucket_name not in existing_buckets:
            if region == "us-east-1":
                s3_client.create_bucket(Bucket=bucket_name)  # No location constraint needed for us-east-1
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": region}
                )
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        print(f"Error creating bucket: {e}")

def upload_to_s3(s3_client, file_path, bucket_name, s3_folder_name, s3_file_name):

    try:
        s3_client.upload_file(file_path, bucket_name, f"{s3_folder_name}/{s3_file_name}")
        print(f"File '{file_path}' uploaded to S3 bucket '{bucket_name}' as '{s3_folder_name}/{s3_file_name}'.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"Error uploading file: {e}")


def wait_for_s3_file(s3_client, bucket_name, s3_folder_name,s3_file_name, max_wait_time=1800, check_interval=20):

    start_time = time.time()
    last_size = -1

    while time.time() - start_time < max_wait_time:
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=f"{s3_folder_name}/{s3_file_name}")
            file_size = response["ContentLength"]

            if file_size == last_size:
                print(f"File '{s3_folder_name}/{s3_file_name}' is fully uploaded.")
                break
                
            else:
                print(f"File '{s3_folder_name}/{s3_file_name}' is still uploading... Current size: {file_size} bytes")
                last_size = file_size

        except s3_client.exceptions.NoSuchKey:
            print(f"File '{s3_folder_name}/{s3_file_name}' not found. Waiting...")
        
        time.sleep(check_interval)

    print(f"Timeout: File '{s3_folder_name}/{s3_file_name}' did not stabilize within {max_wait_time} seconds.")
    