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
        region_name=region_name  
    )

    return session.client("glue")

def create_database_if_not_exists(aws_conn_id, glue_database, description="Database for S3 data"):
   
    client = get_glue_client(aws_conn_id)

    databases = client.get_databases()
    existing_databases = [db["Name"] for db in databases["DatabaseList"]]

    if glue_database not in existing_databases:
        client.create_database(DatabaseInput={"Name": glue_database, "Description": description})
        print(f"Database {glue_database} created.")
    else:
        print(f"Database {glue_database} already exists.")


def create_crawler_if_not_exists(aws_conn_id, glue_crawler, glue_database, iam_role, s3_input_target_path, table_prefix="etl_"):
    """
    Create a Glue crawler if it does not exist.
    """
    client = get_glue_client(aws_conn_id)

    crawlers = client.list_crawlers()
    existing_crawlers = crawlers.get("CrawlerNames", [])

    if glue_crawler not in existing_crawlers:
        client.create_crawler(
            Name=glue_crawler,
            Role=iam_role,
            DatabaseName=glue_database,
            Targets={"S3Targets": [{"Path": s3_input_target_path}]},
            TablePrefix=table_prefix,
        )
        print(f"Crawler {glue_crawler} created.")
    else:
        print(f"Crawler {glue_crawler} already exists.")

def run_glue_crawler(aws_conn_id, glue_crawler):
    """
    Runs an AWS Glue Crawler and waits for its completion.
    """

    client = get_glue_client(aws_conn_id)

    # Start the crawler
    print(f"Starting crawler '{glue_crawler}'...")
    client.start_crawler(Name=glue_crawler)

    # Wait for completion
    while True:
        response = client.get_crawler(Name=glue_crawler)
        state = response["Crawler"]["State"]
        
        if state == "READY":
            print(f"Crawler '{glue_crawler}' completed successfully!")
            break

        print(f"Crawler is still running... Current state: {state}")
        time.sleep(10)  # Check every 30 seconds 