from airflow.hooks.base import BaseHook
import boto3
import time


def get_glue_client(aws_conn_id):

    conn = BaseHook.get_connection(aws_conn_id)


    # Extract region from Airflow connection
    region_name = conn.extra_dejson.get("region_name")  # No default, fetches from connection

    session = boto3.session.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=region_name  # Default to us-east-1
    )

    return session.client("glue")

def glue_job_exists( aws_conn_id, job_name):
    glue_client=get_glue_client(aws_conn_id)
    """Check if the Glue job already exists."""
    try:
        glue_client.get_job(JobName=job_name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False

def create_glue_job(aws_conn_id, GLUE_JOB_NAME,IAM_ROLE , S3_SCRIPT_LOCATION):
    glue_client=get_glue_client(aws_conn_id)

    """Create the AWS Glue job if it does not exist."""
    job_config = {
        "Name": GLUE_JOB_NAME,
        "Role": IAM_ROLE,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": S3_SCRIPT_LOCATION,
            "PythonVersion": "3"
        },
        "GlueVersion": "3.0",
        "NumberOfWorkers": 5,
        "WorkerType": "G.1X",
        "Timeout": 30,
    }

    response = glue_client.create_job(**job_config)
    print(f"Glue job '{GLUE_JOB_NAME}' created successfully.")
    return response

def start_glue_job(aws_conn_id, GLUE_JOB_NAME,GLUE_DATABASE,GLUE_TABLE, GLUE_TABLE_PROCESSED, S3_OUTPUT_TARGET_PATH):
    glue_client=get_glue_client(aws_conn_id)
    """Start the Glue job and return the JobRunId."""
    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
                Arguments={
            "--DATABASE_NAME": GLUE_DATABASE,
            "--RAW_TABLE_NAME": GLUE_TABLE,
            "--PROCESSED_TABLE_NAME": GLUE_TABLE_PROCESSED,
            "--OUTPUT_S3_PATH": S3_OUTPUT_TARGET_PATH
        }
    )
    job_run_id = response["JobRunId"]
    print(f"Glue job '{GLUE_JOB_NAME}' started. JobRunId: {job_run_id}")
    return job_run_id



def wait_for_job_completion(aws_conn_id, job_run_id, GLUE_JOB_NAME):

    glue_client=get_glue_client(aws_conn_id)
    
    """Wait for the Glue job to complete."""
    while True:
        response = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
        job_status = response["JobRun"]["JobRunState"]
        print(f"Job Status: {job_status}")

        if job_status in ["SUCCEEDED", "FAILED", "STOPPED"]:
            break
        time.sleep(30)  # Wait for 30 seconds before checking again

    if job_status == "SUCCEEDED":
        print("Glue job completed successfully.")
    else:
        print(f"Glue job failed or was stopped. Status: {job_status}")
