#!/bin/bash

echo "Setting up Apache Airflow..."

# Check if Python is installed
if ! command -v python3 &>/dev/null; then
    echo "Python3 is not installed. Installing now..."
    sudo apt update && sudo apt install -y python3 python3-venv python3-pip
fi

# Create a virtual environment
if [ ! -d "airflow_env" ]; then
    python3 -m venv airflow_env
fi

# Activate virtual environment
source airflow_env/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Set Airflow home directory
export AIRFLOW_HOME=~/airflow
echo "export AIRFLOW_HOME=~/airflow" >> ~/.bashrc
source ~/.bashrc

# Initialize Airflow database
airflow db init

# Create an Airflow admin user
airflow users create --username admin --password admin --firstname Air --lastname Flow --role Admin --email admin@example.com

echo "Airflow setup complete. Start Airflow using:"
echo "   airflow webserver -p 8080 -D"
echo "   airflow scheduler -D"

