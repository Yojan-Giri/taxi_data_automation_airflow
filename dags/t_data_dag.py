import os
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.s3_pipeline import upload_data_s3_pipeline
from pipelines.s3_pipeline import upload_script_s3_pipeline
from pipelines.crawler_pipeline import run_crawler_pipeline
from pipelines.glue_job_pipeline import run_glue_pipeline
# DEFINE DEFAULT ARGUMENTS
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 5),
    "email": ['yojangiri@gmail.com'],
    "email_on_failure": False,  
    "email_on_retry": False,     
    "retries": 0,
}

# DEFINE DAG
dag = DAG(
    "t_data_dag",
    default_args=default_args,
    schedule_interval=None,  # Set the interval as needed
    catchup=False,
)

# Define Tasks
upload_data_file_to_s3 = PythonOperator(
    task_id='s3_data_upload',
    python_callable=upload_data_s3_pipeline,
    dag=dag
)

run_crawler = PythonOperator(
    task_id='crawler_run',
    python_callable=run_crawler_pipeline,
    dag=dag
)

upload_script_file_to_s3 = PythonOperator(
    task_id='s3_script_upload',
    python_callable=upload_script_s3_pipeline,
    dag=dag
)

run_glue_job= PythonOperator(
    task_id='run_glue_job',
    python_callable= run_glue_pipeline,
    dag=dag
    
)


# Set Task Dependencies
upload_data_file_to_s3 >> upload_script_file_to_s3  >> run_crawler

run_crawler >> run_glue_job