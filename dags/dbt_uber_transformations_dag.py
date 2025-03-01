"""
Airflow DAG for transforming Uber dataset using dbt and Cosmos.

This DAG performs the following steps:
1. Installs dbt dependencies.
2. Runs dbt transformations on the Uber dataset.
3. Executes dbt tests to validate the transformed data.

DAG ID: dbt_uber_transformations_dag
"""

from datetime import datetime
from airflow.operators.bash import BashOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from scripts.config import DBT_PROJECT_DIR, DBT_EXECUTABLE, DBT_PROFILES_DIR,TRANSFORMED_SCHEMA ,DB_DEFAULT_CONN_ID

# DBT Profile Configuration
profile_config = ProfileConfig(
    profile_name="uber_dbt_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=DB_DEFAULT_CONN_ID,
        profile_args={"schema": TRANSFORMED_SCHEMA},
    ),
)

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 2, 
}

# Define the dbt DAG using Cosmos
dbt_uber_transformations_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
    schedule_interval="@daily",
    catchup=False,  
    dag_id="dbt_uber_transformations_dag",
    default_args=default_args,  
    description="Transform Uber dataset using dbt and create transformed schema in Postgres",
)


# Define dbt tasks
dbt_deps_task = BashOperator(
    task_id="install_dbt_dependencies",
    bash_command=f"{DBT_EXECUTABLE} deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dbt_uber_transformations_dag,
)

dbt_run_task = BashOperator(
    task_id="run_dbt_models",
    bash_command=f"{DBT_EXECUTABLE} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dbt_uber_transformations_dag,
)

dbt_test_task = BashOperator(
    task_id="test_dbt_models",
    bash_command=f"{DBT_EXECUTABLE} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dbt_uber_transformations_dag,
)

# Define task dependencies
dbt_deps_task >> dbt_run_task >> dbt_test_task
