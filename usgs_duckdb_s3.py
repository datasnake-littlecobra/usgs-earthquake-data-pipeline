import duckdb
import os
from deltalake import write_deltalake
import os
import boto3
import hvac
import polars as pl
# import duckdb
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

DELTA_TABLE_PATH = "s3://usgs-delta-lake-bucket/ducklamb"
DAILY_TABLE_PATH = "s3://usgs-delta-lake-bucket/usgs-delta-lake-raw"
KEY_ID = access_key
SECRET = secret_key
bucket="usgs-delta-lake-bucket"

def handler():
    # bucket = event["Records"][0]["s3"]["bucket"]["name"]
    # key = event["Records"][0]["s3"]["object"]["key"]
    print("here1")
    conn = duckdb.connect()
    print("here2")
    conn.query(
        """
               INSTALL httpfs;
               LOAD httpfs;
                CREATE SECRET secretaws (
                TYPE S3,
                KEY_ID {access_key},
                SECRET {secret_key}
            );
               """
    )
    print("here3")
    conn.query(
        f"""
                        SELECT count(*)
                        FROM delta_scan('{DAILY_TABLE_PATH}')
                        where year=2010
                    """
    )
    print("here4")
    
handler()