import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from scripts.config import BATCH_SIZE
from scripts.config import DB_DEFAULT_CONN_ID

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_postgres_connection(conn_id: str = DB_DEFAULT_CONN_ID):
    """
    Returns a Postgres connection and cursor using Airflow's PostgresHook.
    
    :param conn_id: Airflow Postgres connection ID
    :return: (connection, cursor) tuple
    """
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        logger.error(f"Error getting PostgreSQL connection: {e}")
        raise

def close_connection(conn, cursor):
    """
    Closes the cursor and connection safely.
    
    :param conn: Postgres connection object
    :param cursor: Postgres cursor object
    """
    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    except Exception as e:
        logger.error(f"Error closing connection: {e}")
        raise


def execute_batch_insert(df, schema_name, table_name):
    """
    Inserts a DataFrame into PostgreSQL using batch insert.

    :param df: Pandas DataFrame containing the data
    :param table_name: Target PostgreSQL table
    :param schema_name: Schema name for the table
    :param batch_size: Number of rows per batch insert
    """
    if df.empty:
        raise ValueError("❌ The DataFrame is empty. Upload aborted.")

    try:
        # Get PostgreSQL connection using PostgresHook
        conn, cursor = get_postgres_connection()

        # Replace NaN values with None in place to avoid memory overhead
        df.where(pd.notna(df), None, inplace=True)

        # Prepare the columns and values template
        df['trip_id'] = df.index
        
        # Prepare the list of columns
        columns = ",".join(df.columns)
        
        # Prepare the insert query
        insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES %s"
        
        
        # Process data in batches using generator
        for batch in batch_generator(df):
            execute_values(cursor, insert_query, batch, page_size=BATCH_SIZE)
            logger.info(f"Inserted a batch of {len(batch)} rows")

        # Commit **once** after all batches have been processed (atomic transaction)
        conn.commit()
        logger.info(f"✅ Successfully uploaded entire DataFrame to {schema_name}.{table_name}")

    except Exception as e:
        logger.error(f"❌ Error uploading DataFrame to {schema_name}.{table_name}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        close_connection(conn, cursor)        


def batch_generator(df):
    """
    Generator that yields batches of rows from the DataFrame using itertuples.
    """
    batch = []
    for row in df.itertuples(index=False, name=None):
        batch.append(row)
        if len(batch) == BATCH_SIZE:
            yield batch
            batch = []
    if batch:  # Yield remaining rows if any
        yield batch
          
        
def execute_sql(sql: str, conn_id: str = DB_DEFAULT_CONN_ID):
    """
    Executes a given SQL statement with error handling.
    
    :param sql: The SQL query to execute
    :param conn_id: Airflow Postgres connection ID
    """
    #conn, cursor = None, None
    try:
        conn, cursor = get_postgres_connection(conn_id)
        cursor.execute(sql)
        conn.commit()
        logger.info(f"Executed SQL: {sql}")
    except Exception as e:
        logger.error(f"Error executing SQL: {e}")
        if conn:
            conn.rollback()
        raise 
    finally:
        close_connection(conn, cursor)