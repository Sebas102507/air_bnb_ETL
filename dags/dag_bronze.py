import os
import logging
from datetime import datetime
import pandas as pd
import gcsfs
from airflow.decorators import dag, task
from airflow.models.variable import Variable # <-- 1. Import the Variable model

# 2. Get the bucket name from Airflow Variables
try:
    BUCKET_NAME = Variable.get("gcs_bucket")
except KeyError:
    logging.error("Failed to get Airflow Variable 'gcs_bucket'.")
    BUCKET_NAME = "default-bucket-name" # Set a default or raise an error

@dag(
    dag_id='process_data_from_gcs_folders_to_db_bronze',
    start_date=datetime(2025, 10, 6),
    schedule=None,
    catchup=False,
    tags=['data_processing', 'gcs', 'pandas'],
)
def process_data_from_gcs_folders_dag():
    """
    ### ETL DAG for Processing CSV Files from GCS

    This DAG connects to GCS, dynamically creates a task for each subfolder in the 'data'
    directory, reads all CSVs within it, combines them, and logs the result.
    """

    @task
    def process_gcs_folder(folder_name: str) -> str:
        """
        Reads all CSV files from a specified GCS folder, combines them,
        and logs the shape of the final DataFrame.
        """
        gcs_folder_path = f"gs://{BUCKET_NAME}/data/{folder_name}"
        logging.info("--- Processing GCS folder: %s ---", gcs_folder_path)

        fs = gcsfs.GCSFileSystem()
        csv_files = fs.glob(f"{gcs_folder_path}/*.csv") 

        if not csv_files:
            message = f"No CSV files found in {gcs_folder_path}. Task succeeded."
            logging.info(message)
            return message

        logging.info("Found %d CSV files to process in GCS.", len(csv_files))
        
        full_gcs_paths = [f"gs://{file}" for file in csv_files]
        list_of_dfs = [pd.read_csv(path) for path in full_gcs_paths]
        combined_df = pd.concat(list_of_dfs, ignore_index=True)

        logging.info("Successfully combined %d files from GCS.", len(csv_files))
        logging.info("Final DataFrame shape: %s", combined_df.shape)
        logging.info("First 5 rows of combined data:\n%s", combined_df.head())
        
        return f"Processed {combined_df.shape[0]} rows from {len(csv_files)} files in {folder_name}."

    process_census_task = process_gcs_folder.override(task_id='process_census_lga_data')(folder_name='Census LGA')
    process_listings_task = process_gcs_folder.override(task_id='process_listings_data')(folder_name='listings')
    process_nsw_lga_task = process_gcs_folder.override(task_id='process_nsw_lga_data')(folder_name='NSW_LGA')

# Instantiate the DAG
process_data_from_gcs_folders_dag()