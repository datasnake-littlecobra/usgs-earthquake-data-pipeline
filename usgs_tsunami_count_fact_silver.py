import os
from datetime import datetime
import polars as pl
import duckdb
from deltalake import DeltaTable, write_deltalake
import logging

# Configuration
output_dir = "usgs-delta-lake-directory"
delta_dir_raw = os.path.join(output_dir, "usgs-delta-lake-raw")
delta_dir_silver = os.path.join(output_dir, "usgs-delta-lake-silver")

RAW_TABLE_PATH = delta_dir_raw
FACT_TSUNAMI_YEARLY_PATH = os.path.join(delta_dir_silver, "fact_tsunami_yearly")
FACT_TSUNAMI_MONTHLY_PATH = os.path.join(delta_dir_silver, "fact_tsunami_monthly")

# Load raw Delta Lake data into DuckDB
con = duckdb.connect()
# con.sql(f"INSTALL 'delta'; LOAD 'delta';")

def convert_save_to_silver_delta_lake():

    duckdb.sql(
        """
        INSTALL delta;
        LOAD delta;
        """
    )

    t1 = datetime.now()
    duckdb.sql(
        """
        SELECT COUNT(*), year AS tsunami_yearly_count
        FROM delta_scan('output_directory/usgs-delta-data-raw')
        group by year
        """
    ).show()

    duckdb.sql(
        """
        SELECT year, COUNT(*) AS tsunami_yearly_count
        FROM delta_scan('output_directory/usgs-delta-data-raw')
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
            FROM delta_scan('output_directory/usgs-delta-data-raw')
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
        FROM delta_scan('output_directory/usgs-delta-data-silver/fact_tsunami_yearly')
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
    #     FROM usgs-delta-data-raw
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
        FROM delta_scan('output_directory/usgs-delta-data-raw')
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
        FROM delta_scan('output_directory/usgs-delta-data-raw')
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
        FROM delta_scan('output_directory/usgs-delta-data-silver/fact_tsunami_monthly')
        order by year, month
        """
    ).show()
    
    return True


    # logging.info(
    #     f"Fact tables created at {FACT_TSUNAMI_YEARLY_PATH} and {FACT_TSUNAMI_MONTHLY_PATH}"
    # )
