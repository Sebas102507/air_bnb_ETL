import logging
import os
import glob
from datetime import datetime, timedelta
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
def extract_and_load_data_func(table_name: str, **kwargs):
    """
    Reads CSV data from a local file or folder, cleans it, and loads it
    into a specified PostgreSQL table.
    """
    file_to_load = kwargs.get('file_name')
    folder_to_load = kwargs.get('folder_name')
    df = None

    # --- Part 1: Read Data ---
    if file_to_load:
        # Logic for a single file
        file_path = os.path.join(AIRFLOW_DATA, file_to_load)
        logging.info("--- Reading from local file: %s ---", file_path)
        if not os.path.exists(file_path):
            logging.error("File not found: %s. Skipping task.", file_path)
            return
        df = pd.read_csv(file_path, low_memory=False)

    elif folder_to_load:
        # Logic for a whole folder
        local_folder_path = os.path.join(AIRFLOW_DATA, folder_to_load)
        logging.info("--- Reading from local folder: %s ---", local_folder_path)
        file_paths = glob.glob(os.path.join(local_folder_path, "*.csv"))
        if not file_paths:
            logging.warning("No CSV files found in %s. Skipping task.", local_folder_path)
            return
        logging.info("Found local files to process: %s", file_paths)
        df = pd.concat(
            [pd.read_csv(path, low_memory=False) for path in file_paths],
            ignore_index=True
        )
    else:
        logging.error("No file_name or folder_name provided. Skipping task.")
        return

    # --- Part 2: Clean Data ---
    logging.info("Original DataFrame shape: %s", df.shape)
    
    # Remove columns where all values are null
    df.dropna(axis=1, how='all', inplace=True)
    
    # Remove columns with 'Unnamed' in the name
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    logging.info("Cleaned DataFrame shape: %s", df.shape)

    # --- Part 3: Load Data to PostgreSQL ---
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

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
}

@dag(
    dag_id='process_data_from_gcs_folders_to_db_bronze',
    start_date=datetime(2025, 10, 6),
    schedule=None,
    catchup=False,
    tags=['gcs', 'postgres', 'etl'],
    default_args=default_args  # Apply the default arguments to the DAG
)
def gcs_to_postgres_bronze_dag():
    """
    ### GCS to Postgres Bronze Layer ETL

    This DAG extracts data from local GCS files and folders, cleans it,
    and loads it into the 'bronze' schema in PostgreSQL.
    """
    load_census_g01_task = PythonOperator(
        task_id='load_census_g01_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'file_name': 'Census LGA/2016Census_G01_NSW_LGA.csv',
            'table_name': 'bronze.census_g01_nsw_lga_bronze'
        }
    )

    load_census_g02_task = PythonOperator(
        task_id='load_census_g02_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'file_name': 'Census LGA/2016Census_G02_NSW_LGA.csv',
            'table_name': 'bronze.census_g02_nsw_lga_bronze'
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

    load_nsw_lga_code_task = PythonOperator(
        task_id='load_nsw_lga_code_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'file_name': 'NSW_LGA/NSW_LGA_CODE.csv',
            'table_name': 'bronze.nsw_lga_code_bronze'
        }
    )

    load_nsw_lga_suburb_task = PythonOperator(
        task_id='load_nsw_lga_suburb_to_postgres',
        python_callable=extract_and_load_data_func,
        op_kwargs={
            'file_name': 'NSW_LGA/NSW_LGA_SUBURB.csv',
            'table_name': 'bronze.nsw_lga_suburb_bronze'
        }
    )

# Instantiate the DAG
gcs_to_postgres_bronze_dag()

