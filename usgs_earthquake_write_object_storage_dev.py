import polars as pl
from deltalake import DeltaTable, write_deltalake
import logging


access_key = ""
secret_key = ""

# print(f"Access Key: {access_key}")
# print(f"Secret Key: {secret_key}")

hostname = "sjc1.vultrobjects.com"

# Sample Data
data = [
    {
        "region": "North America",
        "timestamp": "2023-01-01 10:00:00",
        "year": 2023,
        "month": 1,
    },
    {"region": "Asia", "timestamp": "2023-01-02 11:00:00", "year": 2022, "month": 1},
    {"region": "Europe", "timestamp": "2023-02-01 12:00:00", "year": 2023, "month": 2},
    {
        "region": "North America",
        "timestamp": "2024-01-01 10:00:00",
        "year": 2024,
        "month": 1,
    },
]

# Convert to Polars DataFrame
df = pl.DataFrame(data)

# Define the path to the Delta Lake table
test_bucket_name = "usgs-delta-lake-test-bucket"
bucket_name = "usgs-delta-lake-bucket"
delta_s3_key_raw = "usgs-delta-lake-raw"
delta_s3_key_silver = "usgs-delta-lake-silver"
delta_s3_key_test_write = "usgs-delta-lake-test-write"
raw_bucket_uri = f"s3://{bucket_name}/{delta_s3_key_raw}"
silver_bucket_uri = f"s3://{bucket_name}/{delta_s3_key_silver}/"
table_path = f"s3://{test_bucket_name}/{delta_s3_key_test_write}/"
directory_path = f"usgs-test-directory"

storage_options = {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
}
# Delta table options
delta_table_options = {
    "ignoreDeletes": "true",  # Ignore delete operations
    "ignoreChanges": "true",  # Ignore incremental changes
    "readChangeFeed": "false",
}

# PyArrow options (optional, applied when `use_pyarrow=True`)
pyarrow_options = {
    "use_threads": True,  # Enable multi-threading for performance
    "coerce_int96_timestamp_unit": "ms",  # Adjust timestamp precision
}

# Write DataFrame to Delta Lake with partitioning
write_deltalake(
    table_or_uri=table_path,
    storage_options=storage_options,
    data=df.to_arrow(),  # Convert Polars DataFrame to Arrow Table
    mode="overwrite",  # Write mode: "append", "overwrite", etc.
    partition_by=["year", "month"],  # Specify partition keys
)
# df.write_delta(
#     table_path,
#     mode="overwrite",  # Options: 'append', 'overwrite', 'error', 'ignore'
#     partition_by=["year", "month"],  # Columns to partition by
#     storage_options=storage_options,  # Storage options for cloud services
#     delta_write_options={
#         "schema_mode": "add",  # Options: 'add', 'overwrite', 'fail'
#         # Additional Delta write options can be specified here
#     },
#     pyarrow_options={
#         "use_threads": True,  # Enable multi-threading for performance
#         "coerce_int96_timestamp_unit": "ms",  # Adjust timestamp precision
#     },
#     # delta_merge_options={
#     # Options required for merging data into an existing Delta table
#     # }
# )

print("Data written successfully to Delta Lake with partitioning.")
