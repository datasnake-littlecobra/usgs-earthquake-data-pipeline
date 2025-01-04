import os
from datetime import datetime
import polars as pl
import duckdb
import hvac
from deltalake import DeltaTable, write_deltalake
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

# Configuration
output_dir = "usgs-delta-lake-directory"
usgs_bucket_name_prod = "usgs-delta-lake-bucket-prod"
delta_dir_raw = os.path.join(output_dir, "usgs-delta-lake-raw")
delta_dir_silver = os.path.join(output_dir, "usgs-delta-lake-silver")

RAW_TABLE_PATH = delta_dir_raw
FACT_TSUNAMI_YEARLY_PATH = os.path.join(delta_dir_silver, "fact_tsunami_yearly")
FACT_TSUNAMI_MONTHLY_PATH = os.path.join(delta_dir_silver, "fact_tsunami_monthly")

# print(f"Access Key: {access_key}")
# print(f"Secret Key: {secret_key}")

hostname = "sjc1.vultrobjects.com"

usgs_delta_s3_bucket_silver_prod = "usgs-delta-lake-bucket-silver-prod"
usgs_fact_tsunami_yearly = "fact_tsunami_yearly"
usgs_fact_tsunami_monthly = "fact_tsunami_monthly"
usgs_fact_tsunami_yearly_s3_uri = (
    f"s3://{usgs_delta_s3_bucket_silver_prod}/{usgs_fact_tsunami_yearly}"
)
usgs_fact_tsunami_monthly_s3_uri = (
    f"s3://{usgs_delta_s3_bucket_silver_prod}/{usgs_fact_tsunami_monthly}"
)

storage_options = {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# Load raw Delta Lake data into DuckDB
con = duckdb.connect()
# con.sql(f"INSTALL 'delta'; LOAD 'delta';")


def convert_save_to_silver_delta_lake_local():

    duckdb.sql(
        """
        INSTALL delta;
        LOAD delta;
        """
    )

    t1 = datetime.now()
    duckdb.sql(
        """
        SELECT COUNT(*)
        FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
        where year=2014
        """
    ).show()

    duckdb.sql(
        """
        SELECT COUNT(*), year AS tsunami_yearly_count
        FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
        where tsunami = 1
        group by year
        """
    ).show()

    duckdb.sql(
        """
        SELECT year, COUNT(*) AS tsunami_yearly_count
        FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
        WHERE tsunami = 1
        GROUP BY year
        ORDER BY year
        """
    ).show()

    write_deltalake(
        table_or_uri=FACT_TSUNAMI_YEARLY_PATH,
        data=duckdb.sql(
            """
            SELECT year, COUNT(*) AS tsunami_yearly_count
            FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
            WHERE tsunami = 1
            GROUP BY year
            ORDER BY year
            """
        )
        .pl()
        .to_arrow(),  # Convert Polars DataFrame to Arrow Table
        mode="overwrite",  # Write mode: "append", "overwrite", etc.
        partition_by=["year"],
    )

    duckdb.sql(
        """
        SELECT *
        FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-silver/fact_tsunami_yearly')
        order by year
        """
    ).show()

    t2 = datetime.now()
    total = t2 - t1
    logging.info(f"it took {total} to run this query.")

    ### below process would us up memory to store dataframe
    # raw_data = con.sql(
    #     f"""
    #     SELECT *
    #     FROM '{RAW_TABLE_PATH}'
    # """
    # ).df()

    # # Yearly Tsunami Aggregation
    # fact_tsunami_yearly = con.sql(
    #     """
    #     SELECT year, COUNT(*) AS tsunami_count
    #     FROM usgs-delta-lake-raw
    #     WHERE tsunami = 1
    #     GROUP BY year
    #     ORDER BY year
    # """
    # ).df()
    # # fact_tsunami_yearly.write_parquet(FACT_TSUNAMI_YEARLY_PATH)
    # # Convert DuckDB result to Polars DataFrame
    # fact_tsunami_yearly_pl = pl.from_pandas(fact_tsunami_yearly)

    # # Write to Delta Lake
    # write_deltalake(FACT_TSUNAMI_YEARLY_PATH, fact_tsunami_yearly_pl, mode="overwrite")

    # # # Monthly Tsunami Aggregation

    duckdb.sql(
        """
        SELECT year, month, COUNT(*) as tsunami_monthly_count
        FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
        WHERE tsunami = 1
        group by year, month
        order by year, month
        """
    ).show()

    write_deltalake(
        table_or_uri=FACT_TSUNAMI_MONTHLY_PATH,
        data=duckdb.sql(
            """
        SELECT year, month, COUNT(*) as tsunami_monthly_count
        FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
        WHERE tsunami = 1
        group by year, month
        order by year, month
        """
        )
        .pl()
        .to_arrow(),  # Convert Polars DataFrame to Arrow Table
        mode="overwrite",  # Write mode: "append", "overwrite", etc.
        partition_by=["year", "month"],
    )

    duckdb.sql(
        """
        SELECT *
        FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-silver/fact_tsunami_monthly')
        order by year, month
        """
    ).show()

    # logging.info(
    #     f"Fact tables created at {FACT_TSUNAMI_YEARLY_PATH} and {FACT_TSUNAMI_MONTHLY_PATH}"
    # )

    return True


def convert_save_to_silver_delta_lake_s3():
    duckdb.sql(
        """
        INSTALL delta;
        LOAD delta;
        """
    )
    write_deltalake(
        table_or_uri=usgs_fact_tsunami_yearly_s3_uri,
        storage_options=storage_options,
        data=duckdb.sql(
            """
            SELECT year, COUNT(*) AS tsunami_yearly_count
            FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
            WHERE tsunami = 1
            GROUP BY year
            ORDER BY year
            """
        )
        .pl()
        .to_arrow(),  # Convert Polars DataFrame to Arrow Table
        mode="overwrite",  # Write mode: "append", "overwrite", etc.
        partition_by=["year"],
    )
    # duckdb.sql(
    #     """
    #     SELECT year, month, COUNT(*) as tsunami_monthly_count
    #     FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
    #     WHERE tsunami = 1
    #     group by year, month
    #     order by year, month
    #     """
    # ).show()
    
    write_deltalake(
        table_or_uri=usgs_fact_tsunami_monthly_s3_uri,
        storage_options=storage_options,
        data=duckdb.sql(
            """
            SELECT year, month, COUNT(*) as tsunami_monthly_count
            FROM delta_scan('usgs-delta-lake-directory/usgs-delta-lake-raw')
            WHERE tsunami = 1
            group by year, month
            order by year, month
            """
        )
        .pl()
        .to_arrow(),  # Convert Polars DataFrame to Arrow Table
        mode="overwrite",  # Write mode: "append", "overwrite", etc.
        partition_by=["year", "month"],
    )


# convert_save_to_silver_delta_lake_local()
convert_save_to_silver_delta_lake_s3()
