# NYC Taxi  ETL-Pipeline-using-Astro-Airflow-DBT-Postgres

## Overview
This project demonstrates an end-to-end data pipeline for ingesting and transforming NYC Uber (Taxi) trip data. It uses Airflow to orchestrate tasks, PostgreSQL as the data warehouse, and dbt for data modeling, testing, and documentation.
- **Airflow** (via [Astronomer Cosmos](https://docs.astronomer.io/astro/cloud/cosmos)) to orchestrate data ingestion and transformation.
- **PostgreSQL** as the data warehouse (with separate schemas for raw and transformed data).
- **dbt** for data modeling, testing, and documentation.
  
![ETL Img](https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/etlflow.png)

## Data Sources
- **Official NYC TLC Trip Record Data**: [NYC TLC Website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
- **Data Dictionary**: [NYC TLC Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)  
- **CSV**: [uber_data.csv on GitHub](https://github.com/chandravamshi-ai/Data-Engineering-and-Analysis/blob/main/Uber%20Data%20Engineering%20%2C%20Analysis%20and%20Visualization/uber_data.csv)

## Architecture with results(screenshots)
1. **Raw Data Ingestion**  
   - A dedicated Airflow DAG downloads or retrieves the CSV file from GitHub (or local source) and loads it into the `raw.uber_data` table in PostgreSQL.
     
      ![Ingestion](https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/data_ingestion_dag_success.png)

 ![schemas](https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/schemas.png)

![raw table](https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/uber_data_table.png)

     
     
2. **Data Transformation**  
   - A separate Airflow DAG triggers dbt models that create dimension and fact tables in the `transformed` schema.  
   - The dimensional model includes tables such as `datetime_dim`, `pickup_location_dim`, `rate_code_dim`, `payment_type_dim`, etc.  
   - The **fact** table (e.g., `fact_table`) joins these dimensions based on keys.
     
     ![DBT Transformation](https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/dbt_transformation_dag_success.png)
  
     ![transfomed schema](https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/transfomred_schema.png)

     
3. **Testing & Documentation**  
   - dbt tests are used to validate data integrity (e.g., not null constraints, referential integrity).  
   - dbt documentation (generated by `dbt docs generate`) provides a browsable catalog of models and relationships.
     ![raw data doc](https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/raw.uber_data.png)
  
     ![facts data doc]( https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/fact_trips.png
)

    

## Data Modelling
  We use STAR Schema data modelling where whole data is divided into dimensions and fact table. 

  ![Data Modelling](https://github.com/chandravamshi-ai/tc_tb_ETL-Pipeline-using-Astro-Airflow-DBT-Postgres/blob/main/imgs/data_modelling.png)
  


 ## Project Structure
 
    ```
    ├── dags/
    │   ├── ingest_from_github_dag.py                # Airflow DAG for data ingestion
    │   ├── dbt_uber_transformations_dag.py          # Airflow DAG for dbt transformations
    │   ├── dbt/
    │   │   └── uber_dbt_project/
    │   │       ├── models/
    │   │       │   ├── staging/                     # Staging models (views)
    │   │       │   │    │──  stg_uber_data.sql
    │   │       │   └── marts/                       # Dimension & fact models
    │   │       │       ├── dim_datetime.sql
    │   │       │       ├── dim_dropoff_location.sql
    │   │       │       ├── dim_passenger_count.sql
    │   │       │       ├── dim_payment_type.sql
    │   │       │       ├── dim_pickup_location.sql
    │   │       │       ├── dim_rate_code.sql
    │   │       │       ├── dim_trip_distance.sql
    │   │       │       └── dim_fact_trips.sql   
    │   │       │       └── schema.yml                # Tests and Documentation          
    │   │       ├── tests/                    
    │   │       └── dbt_project.yml
    │   ├── scripts/
    │   │   ├── config.py                     # Configuration constants (e.g. CSV_URL, schema names)
    │   │   └── ingest_utils.py               # Custom Python functions for downloading/loading CSV
    └── ...
    ```



## Project Setup

### Prerequisites
- **Docker** (if using Astronomer CLI or a local Airflow Docker setup)
- **Astronomer CLI** or **Local Airflow** installed
- **PostgreSQL** will be created with Airflow
- **dbt** will be installed insdie docker airflwo container (compatible with your environment; e.g., `dbt-core` or `dbt-postgres`)

### Installation & Configuration

1. **Clone the Repository**
   ```bash
   git clone <YOUR_REPO_URL>.git
   cd <YOUR_REPO_FOLDER>
   ```
2. **Configure PostgreSQL Connection**
   - Update your Airflow connections (in the Airflow UI or via environment variables) to point to your local/remote PostgreSQL instance.  
   - Example connection:  
     - **Conn Id**: `postgres_default`  
     - **Conn Type**: `Postgres`  
     - **Host**: `localhost` (if local)  
     - **Schema**: `postgres`  
     - **Login**: `postgres`  
     - **Password**: `<YOUR_PASSWORD>`  #default is postgres
     - **Port**: `5432`

3. **Review/Update DAGs and dbt Project**
   - In the `dags/` folder, you will find:
     - `ingest_data_dag.py` for loading CSV into the `raw.uber_data` table.
     - `transform_data_dag.py` for running dbt commands to build dimension/fact tables in the `transformed` schema.
   - In the `dbt/` folder, you will find:
     - `models/` containing `.sql` files for dimension and fact models.
     - `dbt_project.yml` with project-level configurations.

## Usage

1. **Start Airflow**
   - If not already running:
     ```bash
     astro dev start
     ```
   - Access Airflow UI at `http://localhost:8080` (or the host/port you configured).

2. **Trigger the Ingestion DAG**
   - In Airflow, enable and trigger the DAG (e.g., `ingest_data_dag`).  
   - This DAG will download the CSV file (from GitHub or local path) and load it into `raw.uber_data`.

3. **Trigger the Transformation DAG**
   - Once the raw table is populated, enable and trigger the `transform_data_dag`.  
   - This will execute dbt commands (`dbt run`) to create and populate dimension/fact tables in the `transformed` schema.

4. **Testing**
   - Tests are automatically run as it is configured in the `transform_data_dag` (e.g., `dbt test`).  
   - Alternatively, run tests locally:
     ```bash
     cd dbt
     dbt test
     ```

5. **Documentation**
   - Generate dbt docs:
     ```bash
     dbt docs generate
     dbt docs serve
     ```
   - Open the docs in a browser at the provided local URL to explore the data lineage and table schema.

## References
- **NYC TLC Official Data**:  
  [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
- **Data Dictionary**:  
  [https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)  
- **Source CSV**:  
  [https://github.com/chandravamshi-ai/Data-Engineering-and-Analysis/blob/main/Uber%20Data%20Engineering%20%2C%20Analysis%20and%20Visualization/uber_data.csv](https://github.com/chandravamshi-ai/Data-Engineering-and-Analysis/blob/main/Uber%20Data%20Engineering%20%2C%20Analysis%20and%20Visualization/uber_data.csv)

