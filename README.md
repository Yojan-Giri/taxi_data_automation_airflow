# Apache Airflow Project

## Overview
This project automates data workflows using Apache Airflow. It is designed to orchestrate and schedule data pipelines efficiently.

## Prerequisites
Ensure you have the following installed:

- Python 3.8+
- Apache Airflow
- Virtual Environment (optional but recommended)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/Yojan-Giri/taxi_data_automation_airflow.git
cd taxi_data_automation_airflow

### 2. Run the Setup Script

```bash
bash setup.sh

### 3. Start Apache Airflow Services

```bash
airflow webserver -p 8080 -D
airflow scheduler -D


### 4. Access Airflow Web UI
Open your browser and go to:

```bash
http://localhost:8080


### 5. Default Login Credentials

- **Username:** admin
- **Password:** admin

### 6. Running the DAGs

1. Place your DAG Python files inside the `airflow/dags` directory.

2. Restart the scheduler to detect new DAGs:

   ```bash
   airflow scheduler -D
3. Open the Airflow UI and enable the DAGs.

### 7.  Troubleshooting

If Airflow doesn't start, check the logs:

```bash
airflow scheduler --debug
airflow webserver --debug
