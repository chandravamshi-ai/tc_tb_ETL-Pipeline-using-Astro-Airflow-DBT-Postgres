"""
Airflow DAG for extracting Uber dataset from GitHub and loading it into Postgres.

This DAG performs the following steps:
1. Ensures the raw schema exists in the Postgres database.
2. Downloads the Uber dataset CSV from GitHub.
3. Loads the dataset into the specified Postgres table.

DAG ID: ingest_from_github_dag
"""

# Import required libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from scripts.ingest_utils import download_and_load_csv
from scripts.config import RAW_SCHEMA, RAW_TABLE, CSV_URL, DB_DEFAULT_CONN_ID


# Define default_args 
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),  
    "retries": 2,  
}

# Define DAG
with DAG(
    dag_id="ingest_from_github_dag",
    default_args=default_args,  
    schedule_interval="@daily",  
    catchup=False,  
    max_active_runs=1,
    description="Ingest Uber dataset from GitHub and load into Postgres",
) as dag:
    
    # Task 1: Ensure the raw schema exists in Postgres
    create_raw_schema = PostgresOperator(
        task_id="create_raw_schema",
        postgres_conn_id="postgres_default",
        sql=f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};",
    )
    
    # Task 2: Download and load CSV into Postgres
    load_csv = PythonOperator(
        task_id="download_and_load_csv",
        python_callable=download_and_load_csv,
        op_kwargs={
            "csv_url": CSV_URL,
            "raw_schema": RAW_SCHEMA,
            "raw_table": RAW_TABLE,
            "conn_id": DB_DEFAULT_CONN_ID,
        },
    )

    # DAG execution flow
    create_raw_schema >> load_csv
