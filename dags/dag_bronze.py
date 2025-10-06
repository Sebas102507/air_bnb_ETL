import logging
from datetime import datetime
import pandas as pd
import gcsfs
from sqlalchemy import create_engine
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Get the bucket name from Airflow Variables
try:
    BUCKET_NAME = Variable.get("gcs_bucket")
except KeyError:
    logging.error("Failed to get Airflow Variable 'gcs_bucket'.")
    BUCKET_NAME = "default-bucket-name"

def extract_and_load_gcs_data(folder_name: str, table_name: str):
    """
    Reads all CSV files from a GCS folder, combines them, and loads
    the result into a specified PostgreSQL table.
    """
    # Part 1: Read data from GCS
    logging.info("--- Reading from GCS folder: %s ---", folder_name)
    gcs_folder_path = f"gs://{BUCKET_NAME}/data/{folder_name}"

    fs = gcsfs.GCSFileSystem()
    csv_files = fs.glob(f"{gcs_folder_path}/*.csv") 

    if not csv_files:
        logging.warning("No CSV files found in %s. Skipping task.", gcs_folder_path)
        return

    full_gcs_paths = [f"gs://{file}" for file in csv_files]
    combined_df = pd.concat(
        [pd.read_csv(path, low_memory=False) for path in full_gcs_paths],
        ignore_index=True
    )
    logging.info(
        "Successfully read %d files. Combined DataFrame has shape: %s",
        len(csv_files), combined_df.shape
    )

    # Part 2: Load DataFrame into PostgreSQL
    logging.info("--- Loading data into PostgreSQL table: %s ---", table_name)
    
    hook = PostgresHook(postgres_conn_id="postgres")
    engine = hook.get_sqlalchemy_engine()

    combined_df.to_sql(
        name=table_name.split('.')[-1], # Table name without schema
        con=engine,
        schema=table_name.split('.')[0], # Schema name
        if_exists='replace',
        index=False,
        chunksize=1000
    )
    logging.info("Successfully loaded %d rows into %s.",
        len(combined_df), table_name
    )

@dag(
    dag_id='gcs_to_postgres_bronze_etl',
    start_date=datetime(2025, 10, 6),
    schedule=None,
    catchup=False,
    tags=['gcs', 'postgres', 'etl'],
)
def gcs_to_postgres_bronze_dag():
    """
    ### GCS to Postgres Bronze Layer ETL

    This DAG extracts data from three different sources in GCS and loads each
    into its own table in the 'bronze' schema in a PostgreSQL database.
    """
    load_census_task = PythonOperator(
        task_id='load_census_lga_to_postgres',
        python_callable=extract_and_load_gcs_data,
        op_kwargs={
            'folder_name': 'Census LGA',
            'table_name': 'bronze.census_lga'
        }
    )

    load_listings_task = PythonOperator(
        task_id='load_listings_to_postgres',
        python_callable=extract_and_load_gcs_data,
        op_kwargs={
            'folder_name': 'listings',
            'table_name': 'bronze.listings'
        }
    )

    load_nsw_lga_suburbs_task = PythonOperator(
        task_id='load_nsw_lga_suburbs_to_postgres',
        python_callable=extract_and_load_gcs_data,
        op_kwargs={
            'folder_name': 'NSW_LGA',
            'table_name': 'bronze.nsw_lga_suburbs'
        }
    )

# Instantiate the DAG
gcs_to_postgres_bronze_dag()