import os
import boto3
import hvac
import polars as pl
import duckdb
import logging
from deltalake import DeltaTable

project_name = "usgs-delta-lake-bucket"
bucket_name = "usgs-delta-lake-bucket"
delta_s3_key_raw = "usgs-delta-lake-raw"
delta_s3_key_silver = "usgs-delta-lake-silver"

# con = duckdb.connect()

# client = hvac.Client(url="http://127.0.0.1:8200", token="")
# works
client = hvac.Client(url="http://127.0.0.1:8200")
# print("client.is_authenticated() : ")
# print(client.is_authenticated())


# datasnake_test_role_id = os.environ["DATASNAKE_TEST_ROLE_ID"]
# datasnake_test_secret_id = os.environ["DATASNAKE_TEST_SECRET_ID"]

# response = client.auth.approle.login(
#     role_id=datasnake_test_role_id, secret_id=datasnake_test_secret_id
# )
# client.token = response["auth"]["client_token"]

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
# works
secret_response = client.secrets.kv.v1.read_secret(
    path=secret_path, mount_point="secret"
)

# Print the secret values
# access_key = vault_response["data"]["data"]["aws_access_key_id"]
# secret_key = vault_response["data"]["data"]["aws_secret_access_key"]

#
access_key = secret_response["data"]["aws_access_key_id"]
secret_key = secret_response["data"]["aws_secret_access_key"]

# print(f"Access Key: {access_key}")
# print(f"Secret Key: {secret_key}")

hostname = "sjc1.vultrobjects.com"

session = boto3.session.Session()
s3_client = session.client(
    "s3",
    **{
        "region_name": hostname.split(".")[0],
        "endpoint_url": "https://" + hostname,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
    },
)

def access_delta_lake():
    # List raw Delta Lake files
    print("calling s3 client")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=delta_s3_key_silver)
    for obj in response.get('Contents', []):
        print(obj['Key'])

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

raw_bucket_uri= f"s3://{bucket_name}/{delta_s3_key_raw}"
silver_bucket_uri=f"s3://{bucket_name}/{delta_s3_key_silver}/"

def read_delta_lake_using_polars():
    try:
        # Load Delta Table locally
        logging.info(f"bucket uri: {raw_bucket_uri}")
        print("bucket uri: {raw_bucket_uri}")
        df = pl.read_delta(
            raw_bucket_uri, storage_options=storage_options,
            pyarrow_options=pyarrow_options
        )

        print(df.head())
    except Exception as e:
        raise e


def convert_save_to_silver_delta_lake():
    try:
        logging.info(f"using duckdb to connect to object storage")

        # Connect to DuckDB
        # con = duckdb.connect()

        # Load the S3 extension
        duckdb.sql("INSTALL httpfs;")
        duckdb.sql("LOAD httpfs;")

        # Configure S3 parameters for Vultr
        duckdb.sql("""
        SET s3_endpoint = '${hostname}';
        SET s3_access_key_id = '${access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_url_style = 'path';      -- Use path-style access (common for custom endpoints)
        """)
        # SET s3_region = 'YOUR_REGION';  -- Optional, depending on Vultr setup

        duckdb.sql("""
            SELECT * FROM delta_scan('s3://usgs-delta-lake-bucket/usgs-delta-lake-raw/')
        """).show()

        # duckdb.sql(
        #     """
        #     INSTALL delta;
        #     LOAD delta;
        #     """
        # )
        # duckdb.sql(
        #     """
        #     SELECT count(*)
        #     FROM delta_scan('s3://usgs-delta-lake-bucket/usgs-delta-lake-raw/')
        #     where year=2010
        #     """
        # ).show()
    except Exception as e:
        raise e

access_delta_lake()