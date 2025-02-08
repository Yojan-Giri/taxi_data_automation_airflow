from etl.glue_job_run import glue_job_exists, create_glue_job, start_glue_job, wait_for_job_completion
from utils.constants import GLUE_JOB_NAME, GLUE_DATABASE, GLUE_TABLE, AWS_CONN_ID, S3_SCRIPT_LOCATION, S3_OUTPUT_TARGET_PATH, IAM_ROLE, S3_INPUT_TARGET_PATH , GLUE_DATABASE, GLUE_TABLE_PROCESSED

def run_glue_pipeline():
    if not glue_job_exists(AWS_CONN_ID,GLUE_JOB_NAME):
        create_glue_job(AWS_CONN_ID, GLUE_JOB_NAME, IAM_ROLE, S3_SCRIPT_LOCATION)
    
    job_run_id = start_glue_job(AWS_CONN_ID, GLUE_JOB_NAME,GLUE_DATABASE, GLUE_TABLE, GLUE_TABLE_PROCESSED, S3_OUTPUT_TARGET_PATH)
    wait_for_job_completion(AWS_CONN_ID, job_run_id, GLUE_JOB_NAME)

