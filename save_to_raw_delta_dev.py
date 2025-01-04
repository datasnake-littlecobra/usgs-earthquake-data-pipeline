# save_to_delta.py
import os
from datetime import datetime
import polars as pl
from deltalake import DeltaTable, write_deltalake

# import boto3
import logging

access_key = ""
secret_key = ""
# print(f"Access Key: {access_key}")
# print(f"Secret Key: {secret_key}")

hostname = "sjc1.vultrobjects.com"


# Define the path to the Delta Lake table
test_bucket_name = "usgs-delta-lake-test-bucket"
usgs_bucket_name_dev = "usgs-delta-lake-bucket-dev"
delta_s3_key_raw_dev = "usgs-delta-lake-dev-raw"
delta_s3_key_silver_dev = "usgs-delta-lake-dev-silver"
delta_s3_key_test_write = "usgs-delta-lake-test-write"
raw_bucket_uri_dev = f"s3://{usgs_bucket_name_dev}/{delta_s3_key_raw_dev}"
silver_bucket_uri_dev = f"s3://{usgs_bucket_name_dev}/{delta_s3_key_silver_dev}/"
table_path_dev = f"s3://{test_bucket_name}/{delta_s3_key_test_write}/"
directory_path_dev = f"usgs-test-directory"

storage_options = {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)


# Function to save data to Delta Lake format
def save_to_delta_table_local(data: pl.DataFrame, path: str, mode):
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
            else:
                raise ValueError("Invalid mode: Choose either 'overwrite' or 'append'.")
        else:
            logging.info(f"Creating a new Delta Lake table.")
    except Exception as e:
        logging.error(f"Errored out in save_to_delta_table function: {e}")
        raise e


# Upload Delta Table to S3
def upload_raw_delta_to_s3_dev(dataframe: pl.DataFrame, delta_path: str, bucket: str, key: str):
    try:
        write_deltalake(
            table_or_uri=raw_bucket_uri_dev,
            storage_options=storage_options,
            data=dataframe.to_arrow(),  # Convert Polars DataFrame to Arrow Table
            mode="overwrite",  # Write mode: "append", "overwrite", etc.
            partition_by=["year", "month"],  # Specify partition keys
        )
    except Exception as e:
        logging.error(f"Error uploading Delta table to S3: {e}")
