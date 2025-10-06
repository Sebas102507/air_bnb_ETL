import os
import glob
import logging
from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
logging.getLogger().setLevel(logging.INFO)

# Define the base path relative to the DAG file
DAGS_FOLDER = os.path.dirname(__file__)
DATA_FOLDER = os.path.join(DAGS_FOLDER, 'data')

@dag(
    dag_id='process_data_from_folders_logging',
    start_date=datetime(2025, 10, 6),
    schedule=None,
    catchup=False,
    tags=['data_processing', 'pandas'],
    doc_md="""
    ### Process Data from Folders DAG

    This DAG demonstrates reading CSV files from multiple subdirectories using best practices.
    It creates one dynamic task for each folder found inside the `data/` directory.
    - **Task 1:** Processes CSVs in `Census LGA/`
    - **Task 2:** Processes CSVs in `listings/`
    - **Task 3:** Processes CSVs in `NSW_LGA/`
    """
)
def process_data_from_folders_dag():
    """
    ### ETL DAG for Processing CSV Files

    This DAG dynamically creates a task for each subfolder, reads all CSVs within it,
    combines them, and logs the shape of the resulting DataFrame.
    """

    @task
    def process_folder(folder_name: str) -> str:
        """
        Reads all CSV files from a specified folder, combines them,
        and logs the shape of the final DataFrame.
        """
        folder_path = os.path.join(DATA_FOLDER, folder_name)
        logging.info("--- Processing folder: %s ---", folder_path)

        # Find all CSV files in the directory
        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

        if not csv_files:
            message = f"No CSV files found in {folder_path}. Task succeeded."
            logging.info(message)
            return message

        logging.info("Found %d CSV files to process.", len(csv_files))

        # Read all CSVs into a list of DataFrames
        list_of_dfs = [pd.read_csv(file) for file in csv_files]

        # Combine all DataFrames into a single one
        combined_df = pd.concat(list_of_dfs, ignore_index=True)

        # Log the results
        logging.info("Successfully combined %d files.", len(csv_files))
        logging.info("Final DataFrame shape: %s", combined_df.shape)
        logging.info("First 5 rows of combined data:\n%s", combined_df.head())
        
        return f"Processed {combined_df.shape[0]} rows from {len(csv_files)} files in {folder_name}."

    # Create a task for each folder from your screenshot
    process_census_task = process_folder(folder_name='Census LGA')
    process_listings_task = process_folder(folder_name='listings')
    process_nsw_lga_task = process_folder(folder_name='NSW_LGA')


# Instantiate the DAG
process_data_from_folders_dag()