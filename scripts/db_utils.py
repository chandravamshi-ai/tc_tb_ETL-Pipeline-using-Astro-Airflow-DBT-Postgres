import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_postgres_connection(conn_id: str = "postgres_default"):
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
        logger.warning(f"Error closing connection: {e}")

def execute_sql(sql: str, conn_id: str = "postgres_default"):
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

def execute_batch_insert(df, schema_name, table_name, batch_size=1000):
    """
    Inserts a DataFrame into PostgreSQL using batch insert.

    :param df: Pandas DataFrame containing the data
    :param table_name: Target PostgreSQL table
    :param schema_name: Schema name for the table
    :param batch_size: Number of rows per batch insert
    """
    if df.empty:
        logger.warning("⚠️ The DataFrame is empty. Skipping upload.")
        return

    try:
        # Get PostgreSQL connection using PostgresHook
        conn, cursor = get_postgres_connection()

        # Prepare the columns and values template
        df['trip_id'] = df.index
        columns = ",".join(df.columns)
        values_template = ",".join(["%s"] * len(df.columns))  # Ensure each column gets its own placeholder

        # Construct the insert query dynamically with correct placeholders
        insert_query = f"""
                INSERT INTO {schema_name}.{table_name} (
                    {columns}) 
                VALUES ({values_template});
            """
            
        logger.info(f"Insert query: {insert_query}")
        

        # Convert NaN to None for all columns in the DataFrame
        df = df.where(pd.notna(df), None)

        # Convert DataFrame to list of tuples for insertion
        data = df.to_records(index=False).tolist()

        # Insert in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            # Correct usage of execute_values:
            cursor.executemany(insert_query, batch)
            conn.commit()
            logger.info(f"Inserted {len(batch)} rows into {schema_name}.{table_name}")
        logger.info(f"✅ Successfully uploaded DataFrame to {schema_name}.{table_name}")

    except Exception as e:
        logger.error(f"❌ Error uploading DataFrame to {schema_name}.{table_name}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        close_connection(conn, cursor)