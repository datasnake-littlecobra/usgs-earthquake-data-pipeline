import boto3
import hvac
import duckdb
import logging

project_name = "usgs-delta-lake-bucket"
bucket_name = "usgs-delta-lake-bucket"
delta_s3_key_raw = f"{project_name}/usgs-delta-lake-raw"
delta_s3_key_silver = f"{project_name}/usgs-delta-lake-silver"

con = duckdb.connect()

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

def convert_save_to_silver_delta_lake():
    logging.info(f"using duckdb to connect to object storage")
    # con = duckdb.connect()
    duckdb.sql(
        """
        INSTALL delta;
        LOAD delta;
        """
    )
    duckdb.sql(
        """
        SELECT count(*)
        FROM delta_scan('s3://usgs-delta-lake-bucket/usgs-delta-lake-silver')
        where year=2010
        """
    ).show()
    
    