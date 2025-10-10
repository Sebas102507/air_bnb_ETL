import logging
import os
import glob
from datetime import datetime
import pandas as pd
from airflow.decorators import dag
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
    Reads all CSV files from a local folder, combines them, and loads
    the result into a specified PostgreSQL table.
    """
    local_folder_path = os.path.join(AIRFLOW_DATA, folder_name)

    # Read from the local mounted directory
    logging.info("--- Reading from local GCS mount: %s ---", local_folder_path)
    file_paths = glob.glob(os.path.join(local_folder_path, "*.csv"))

    # Abort if no files were found
    if not file_paths:
        logging.warning("No CSV files found in %s. Skipping task.", local_folder_path)
        return

    logging.info("Found local files to process: %s", file_paths)

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
            'table_name': 'bronze.census_lga_bronze'
        }
    )

    load_listings_task = PythonOperator(
        task_id='load_listings_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'folder_name': 'listings',
            'table_name': 'bronze.listings_bronze'
        }
    )

    load_nsw_lga_suburbs_task = PythonOperator(
        task_id='load_nsw_lga_suburbs_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'folder_name': 'NSW_LGA',
            'table_name': 'bronze.nsw_lga_suburbs_bronze'
        }
    )

# Instantiate the DAG
gcs_to_postgres_bronze_dag()