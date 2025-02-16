"""
Configuration file for database connection and data source.
"""

# -----------------------------------------------------------------------------
# Extract-Transform-Load Configuration
# -----------------------------------------------------------------------------
CSV_URL = "https://raw.githubusercontent.com/chandravamshi-ai/Data-Engineering-and-Analysis/main/Uber%20Data%20Engineering%20%2C%20Analysis%20and%20Visualization/uber_data.csv"
RAW_SCHEMA = "raw"
RAW_TABLE = "uber_data"
# -----------------------------------------------------------------------------
# DBT Transformation Configuration
# -----------------------------------------------------------------------------
TRANSFORMED_SCHEMA = "transformed"
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/uber_dbt_project"
DBT_PROFILES_DIR = DBT_PROJECT_DIR  # Profiles directory is the same as project dir
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"