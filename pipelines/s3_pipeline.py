from etl.s3_upload import get_s3_client, create_bucket_if_not_exists, upload_to_s3, wait_for_s3_file
from utils.constants import AWS_DATA_BUCKET_NAME, AWS_CONN_ID, AWS_REGION, AWS_SCRIPT_BUCKET_NAME
def upload_data_s3_pipeline():
    file_path="/home/ubuntu/airflow/data/data_taxi.csv"
    s3_folder_name="taxi_raw_data"
    s3=get_s3_client(AWS_CONN_ID)
    create_bucket_if_not_exists(s3, AWS_DATA_BUCKET_NAME, AWS_REGION)
    upload_to_s3(s3, file_path, AWS_DATA_BUCKET_NAME,s3_folder_name,file_path.split('/')[-1])
    wait_for_s3_file(s3,AWS_DATA_BUCKET_NAME,s3_folder_name,file_path.split('/')[-1])


def upload_script_s3_pipeline():
    file_path="/home/ubuntu/airflow/glue_script/transform_data.py"
    s3_folder_name="taxi_code"
    s3=get_s3_client(AWS_CONN_ID)
    create_bucket_if_not_exists(s3, AWS_SCRIPT_BUCKET_NAME, AWS_REGION)
    upload_to_s3(s3, file_path, AWS_SCRIPT_BUCKET_NAME,s3_folder_name,file_path.split('/')[-1])
    wait_for_s3_file(s3,AWS_SCRIPT_BUCKET_NAME,s3_folder_name,file_path.split('/')[-1])