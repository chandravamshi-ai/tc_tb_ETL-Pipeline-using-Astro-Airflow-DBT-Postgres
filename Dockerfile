FROM quay.io/astronomer/astro-runtime:12.7.0


# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate

# Install dbt dependencies from packages.yml
#RUN /usr/local/airflow/dbt_venv/bin/dbt deps --project-dir /usr/local/airflow/dags/dbt/uber_dbt_project