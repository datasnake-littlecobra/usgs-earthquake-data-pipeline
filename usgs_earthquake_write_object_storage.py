import polars as pl
import hvac
import logging

# client = hvac.Client(url="http://127.0.0.1:8200", token="")
client = hvac.Client(url="http://127.0.0.1:8200")
# print("client.is_authenticated() : ")
# print(client.is_authenticated())

datasnake_test_role_id = os.environ["DATASNAKE_TEST_ROLE_ID"]
datasnake_test_secret_id = os.environ["DATASNAKE_TEST_SECRET_ID"]

response = client.auth.approle.login(
    role_id=datasnake_test_role_id, secret_id=datasnake_test_secret_id
)
client.token = response["auth"]["client_token"]
# print(client.token)
# print(response)


# Access the secret
secret_path_v1 = "secret/s3keys"
# For KV version 2, use 'data/' in the path
secret_path_v2 = "secret/data/s3keys"
# using this path
secret_path = "s3keys"

# read secret using secret_path_v1 with NO default key path set to "secret" by VAULT
# print(client.read(secret_path_v1))
# vault_response = client.read(secret_path_v1)

# using v1 kv with default path SET TO = "secret" by VAULT
secret_response = client.secrets.kv.v1.read_secret(
    path=secret_path, mount_point="secret"
)

# Print the secret values
# access_key = vault_response["data"]["data"]["aws_access_key_id"]
# secret_key = vault_response["data"]["data"]["aws_secret_access_key"]

access_key = secret_response["data"]["aws_access_key_id"]
secret_key = secret_response["data"]["aws_secret_access_key"]

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
    {"region": "Asia", "timestamp": "2023-01-02 11:00:00", "year": 2023, "month": 1},
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
bucket_name = "usgs-delta-lake-bucket"
delta_s3_key_raw = "usgs-delta-lake-raw"
delta_s3_key_silver = "usgs-delta-lake-silver"
delta_s3_key_test_write = "usgs-delta-lake-test-write"
raw_bucket_uri = f"s3://{bucket_name}/{delta_s3_key_raw}"
silver_bucket_uri = f"s3://{bucket_name}/{delta_s3_key_silver}/"
table_path = f"s3://{bucket_name}/{delta_s3_key_test_write}/"

storage_options = {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
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
df.write_delta(
    table_path,
    mode="overwrite",  # Options: 'append', 'overwrite', 'error', 'ignore'
    partition_by=["year", "month"],  # Columns to partition by
    storage_options=storage_options,  # Storage options for cloud services
    delta_write_options={
        "schema_mode": "add",  # Options: 'add', 'overwrite', 'fail'
        # Additional Delta write options can be specified here
    },
    pyarrow_options={
        "use_threads": True,  # Enable multi-threading for performance
        "coerce_int96_timestamp_unit": "ms",  # Adjust timestamp precision
    },
    # delta_merge_options={
    # Options required for merging data into an existing Delta table
    # }
)

print("Data written successfully to Delta Lake with partitioning.")
