#!/bin/bash

echo "Setting up Apache Airflow..."

# Check if Python is installed
if ! command -v python3 &>/dev/null; then
    echo "Python3 is not installed. Installing now..."
    sudo apt update && sudo apt install -y python3 python3-venv python3-pip
fi

# Create a virtual environment if it doesn't exist
if [ ! -d "airflow_env" ]; then
    python3 -m venv airflow_env
    echo "Virtual environment created."
fi

# Activate virtual environment
source airflow_env/bin/activate
echo "Virtual environment activated."

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Set Airflow home directory
export AIRFLOW_HOME=~/airflow
if ! grep -q "export AIRFLOW_HOME=~/airflow" ~/.bashrc; then
    echo "export AIRFLOW_HOME=~/airflow" >> ~/.bashrc
    source ~/.bashrc
    echo "AIRFLOW_HOME set to ~/airflow."
fi

# Initialize Airflow database
airflow db init

# Start Airflow using standalone mode
echo "Starting Airflow..."
airflow standalone

echo "Airflow setup complete. You can access the UI at:"
echo "   http://localhost:8080"
echo "Use the credentials displayed in the terminal."
