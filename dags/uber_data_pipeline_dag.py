"""
Airflow DAG for orchestrating Uber data pipeline.

This DAG executes:
1. The extraction DAG (ingests raw Uber data from GitHub into Postgres).
2. The transformation DAG (uses dbt to transform raw data into a structured format).

DAG ID: uber_data_pipeline_dag
"""

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

# Define default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
}

# Define the pipeline DAG
with DAG(
    dag_id="uber_data_pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily", 
    catchup=False,
    description="End-to-end pipeline: Extract Uber data from github and load to Postgres then transform with dbt and laod the transformed data to Postgres",
) as dag:

    # Task 1: Trigger Extraction DAG (GitHub -> Postgres Raw Schema)
    trigger_extract_dag = TriggerDagRunOperator(
        task_id="trigger_ingest_dag",
        trigger_dag_id="ingest_from_github_dag",  
        wait_for_completion=True,  
    )

    # Task 2: Trigger Transformation DAG (dbt Transformations)
    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="dbt_uber_transformations_dag",
        wait_for_completion=True,  # Ensures transformations finish
    )

    # Define DAG execution order
    trigger_extract_dag >> trigger_transform_dag
