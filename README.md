# Apache Airflow Project: NYC Taxi Data Automation

## Overview
This project automates a data pipeline using Apache Airflow, AWS Glue, and S3 to process NYC Yellow Taxi data. The pipeline follows these key steps:

## 1. EC2 Setup & Airflow Integration

- An EC2 instance is provisioned to host Apache Airflow, which is used to orchestrate the pipeline.

## 2. Data Ingestion to S3

- A new S3 bucket is created to store raw data files.

- The NYC Yellow Taxi dataset is uploaded to the S3 bucket.

## 3. Glue Crawler & Catalog Creation

- The pipeline ensures that an AWS Glue Crawler is created and catalogs the data from S3 into the AWS Glue Data Catalog if it doesn't already exist.

## 4. Data Transformation using AWS Glue Job

- The Glue Job script is stored in S3 and executed via Airflow.
- The script performs data cleaning, feature engineering, and outlier filtering (e.g., removing extreme fare values).
- New features such as trip duration, time-based features, trip categorization, and payment type mapping are added.
## 5. Processed Data Storage & Catalog Update

- The transformed dataset is written back to S3 in Parquet format and registered in the Glue Catalog for further analysis.

By leveraging Apache Airflow, the entire process is automated, ensuring efficient scheduling, monitoring, and execution of the data pipeline. 

## Prerequisites
Ensure you have the following installed:

- Python 3.8+
- Apache Airflow
- EC2 Instance
- AWS CLI 
- IAM Role with AWS Glue and S3 Access

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/Yojan-Giri/taxi_data_automation_airflow.git
cd taxi_data_automation_airflow
```
### 2. Run the Setup Script

```bash
bash setup.sh
```
### 3. Start Apache Airflow Services

```bash
airflow standalone
```
This command automatically initializes Airflow, starts the webserver, scheduler, and creates an admin user.

### 4. Access Airflow Web UI
Open your browser and go to:

```bash
http://localhost:8080
```

### 5. Default Login Credentials

After running airflow standalone, Airflow will generate a default admin user. You’ll see the login credentials in the terminal.

### 6. Running the DAGs

1. Place your DAG Python files inside the `airflow/dags` directory.

2. Restart the scheduler to detect new DAGs:

   ```bash
   airflow standalone
   ```
3. Open the Airflow UI and enable the DAGs.

### 7. Set Up AWS Connection in Airflow  

1. Open [Airflow Web UI](http://localhost:8080)  
2. Go to **Admin** > **Connections** > **+ Add Connection**  
3. Set:  
   - **Connection Id**: `aws_default`  
   - **Connection Type**: `Amazon Web Services`  
   - **Login**: *AWS Access Key ID*  
   - **Password**: *AWS Secret Access Key*  
   - **Extra**:  
     ```json
     { "region_name": "eu-north-1" }
     ```
   *(Replace `"eu-north-1"` with your AWS region)*  
4. Click **Save**  


### 8.  Troubleshooting

If Airflow doesn't start, check the logs:

```bash
airflow standalone --debug
```
