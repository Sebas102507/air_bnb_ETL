import logging
import os
import glob
from datetime import datetime
import pandas as pd
import gcsfs
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"

#########################################################
#
#   Custom Logics for Operator
#
#########################################################
def extract_and_load_data_func(folder_name: str, table_name: str, **kwargs):
    """
    Reads CSV files from a folder, combines them, and loads the result
    into a specified PostgreSQL table. It prioritizes the local GCS mount
    and falls back to a direct GCS bucket read if necessary.
    """
    file_paths = []
    local_folder_path = os.path.join(AIRFLOW_DATA, folder_name)

    # Try reading from the local mounted directory first
    if os.path.isdir(local_folder_path):
        logging.info("--- Reading from local GCS mount: %s ---", local_folder_path)
        file_paths = glob.glob(os.path.join(local_folder_path, "*.csv"))
        # Added log for files found locally
        if file_paths:
            logging.info("Found local files to process: %s", file_paths)

    # If local path fails or is empty, fall back to the GCS bucket
    if not file_paths:
        logging.warning(
            "Local path '%s' not found or empty. Falling back to GCS bucket.",
            local_folder_path
        )
        try:
            BUCKET_NAME = Variable.get("gcs_bucket")
            gcs_folder_path = f"gs://{BUCKET_NAME}/data/{folder_name}"
            logging.info("--- Reading from GCS bucket: %s ---", gcs_folder_path)
            fs = gcsfs.GCSFileSystem()
            gcs_files = fs.glob(f"{gcs_folder_path}/*.csv")
            file_paths = [f"gs://{file}" for file in gcs_files]
        except KeyError:
            logging.error("Fallback failed: Airflow Variable 'gcs_bucket' not found.")
            return

    # Abort if no files were found by either method
    if not file_paths:
        logging.warning("No CSV files found in local path or GCS. Skipping task.")
        return

    # Generate dataframe by reading the CSV files
    logging.info("Reading %d CSV files...", len(file_paths))
    df = pd.concat(
        [pd.read_csv(path, low_memory=False) for path in file_paths],
        ignore_index=True
    )
    logging.info("Successfully read files. Combined DataFrame has shape: %s", df.shape)

    # Setup Postgres connection and load data
    logging.info("--- Loading data into PostgreSQL table: %s ---", table_name)
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql(
        name=table_name.split('.')[-1],
        con=engine,
        schema=table_name.split('.')[0],
        if_exists='replace',
        index=False,
        chunksize=1000
    )
    logging.info("Successfully loaded %d rows into %s.", len(df), table_name)

#########################################################
#
#   DAG Definition
#
#########################################################
@dag(
    dag_id='process_data_from_gcs_folders_to_db_bronze',
    start_date=datetime(2025, 10, 6),
    schedule=None,
    catchup=False,
    tags=['gcs', 'postgres', 'etl'],
)
def gcs_to_postgres_bronze_dag():
    """
    ### GCS to Postgres Bronze Layer ETL

    This DAG extracts data from GCS and loads it into the 'bronze' schema.
    """
    load_census_task = PythonOperator(
        task_id='load_census_lga_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'folder_name': 'Census LGA',
            'table_name': 'bronze.census_lga'
        }
    )

    load_listings_task = PythonOperator(
        task_id='load_listings_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'folder_name': 'listings',
            'table_name': 'bronze.listings'
        }
    )

    load_nsw_lga_suburbs_task = PythonOperator(
        task_id='load_nsw_lga_suburbs_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'folder_name': 'NSW_LGA',
            'table_name': 'bronze.nsw_lga_suburbs'
        }
    )

# Instantiate the DAG
gcs_to_postgres_bronze_dag()