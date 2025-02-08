

from etl.crawler_run import create_database_if_not_exists, create_crawler_if_not_exists, run_glue_crawler
from utils.constants import AWS_CONN_ID, GLUE_DATABASE, GLUE_CRAWLER, IAM_ROLE, S3_INPUT_TARGET_PATH



def run_crawler_pipeline():
    create_database_if_not_exists(AWS_CONN_ID, GLUE_DATABASE)
    create_crawler_if_not_exists(AWS_CONN_ID, GLUE_CRAWLER, GLUE_DATABASE, IAM_ROLE, S3_INPUT_TARGET_PATH)
    run_glue_crawler(AWS_CONN_ID, GLUE_CRAWLER)