import pandas as pd
import logging
from io import StringIO
from scripts.db_utils import execute_sql, execute_batch_insert

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def download_and_load_csv(csv_url: str, raw_schema: str, raw_table: str, conn_id: str = "postgres_default"):
    """
    Downloads a CSV file from a URL and loads it into Postgres using COPY.
    Uses db_utils to handle all database operations.

    :param csv_url: URL to the raw CSV file (GitHub raw link)
    :param raw_schema: Schema name in Postgres
    :param raw_table: Table name to load data into
    :param conn_id: Airflow Postgres connection ID (default: postgres_default)
    """

    try:
        logger.info(f"Starting CSV download from {csv_url}...")
        df = pd.read_csv(csv_url)
        logger.info(f"Downloaded {len(df)} rows.")


        logger.info(f"Preparing to create table: {raw_schema}.{raw_table}")

        # SQL to drop and recreate the table
        drop_sql = f"DROP TABLE IF EXISTS {raw_schema}.{raw_table} CASCADE;"
        create_sql = f"""
                        CREATE TABLE {raw_schema}.{raw_table} (
                            id SERIAL PRIMARY KEY,
                            trip_id INTEGER, 
                            VendorID INTEGER,
                            tpep_pickup_datetime TIMESTAMP,
                            tpep_dropoff_datetime TIMESTAMP,
                            passenger_count INTEGER,
                            trip_distance DOUBLE PRECISION,
                            pickup_longitude DOUBLE PRECISION,
                            pickup_latitude DOUBLE PRECISION,
                            RatecodeID INTEGER,
                            store_and_fwd_flag TEXT,
                            dropoff_longitude DOUBLE PRECISION,
                            dropoff_latitude DOUBLE PRECISION,
                            payment_type INTEGER,
                            fare_amount DOUBLE PRECISION,
                            extra DOUBLE PRECISION,
                            mta_tax DOUBLE PRECISION,
                            tip_amount DOUBLE PRECISION,
                            tolls_amount DOUBLE PRECISION,
                            improvement_surcharge DOUBLE PRECISION,
                            total_amount DOUBLE PRECISION
                        );
        """

        # Use db_utils functions
        execute_sql(drop_sql, conn_id)  # Drop table
        execute_sql(create_sql, conn_id)  # Create table
        logger.info(f"Table {raw_schema}.{raw_table} created successfully.")

        logger.info(f"Loading data into {raw_schema}.{raw_table}...")
        execute_batch_insert(df, raw_schema, raw_table, 1000)
        logger.info(f"Data successfully loaded into {raw_schema}.{raw_table}.")

    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
        raise
