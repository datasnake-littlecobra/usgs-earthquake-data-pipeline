# save_to_delta.py
import os
from datetime import datetime
import polars as pl
from deltalake import DeltaTable, write_deltalake
# import boto3

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)


# Function to save data to Delta Lake format
def save_to_delta_table(data: pl.DataFrame, path: str, mode):
    try:
        # logging.info(f"inside save to delta table")
        # Ensure the path exists, or create it (you could use pathlib for this)
        os.makedirs(path, exist_ok=True)

        # Create a file path within the directory
        file_path = os.path.join(path, "")
        # logging.info(f"Starting to write into Delta Parquet: {file_path}")
        # logging.info(datetime.now())
        # Check if the table exists and handle mode appropriately
        if os.path.exists(file_path):
            if mode == "overwrite":
                logging.info(f"Overwriting the existing Delta Lake table.")
                # data.write_delta(file_path,mode="overwrite")
                # data.write_delta(target=path, delta_write_options={"year", "month"}, mode="append")
                write_deltalake(
                    table_or_uri=path,
                    data=data.to_arrow(),  # Convert Polars DataFrame to Arrow Table
                    mode="overwrite",  # Write mode: "append", "overwrite", etc.
                    partition_by=["year", "month"],  # Specify partition keys
                )
                logging.info(
                    f"Data successfully written to {file_path} in {mode} mode."
                )
            elif mode == "append":
                logging.info(f"Appending to the existing Delta Lake table.")
                # data.write_delta(file_path,mode="append")
                write_deltalake(
                    table_or_uri=path,
                    data=data.to_arrow(),  # Convert Polars DataFrame to Arrow Table
                    mode="append",  # Write mode: "append", "overwrite", etc.
                    partition_by=["year", "month"],  # Specify partition keys
                )
                return
            else:
                raise ValueError("Invalid mode: Choose either 'overwrite' or 'append'.")
        else:
            logging.info(f"Creating a new Delta Lake table.")
    except Exception as e:
        logging.error(f"Errored out in save_to_delta_table function: {e}")
        raise e