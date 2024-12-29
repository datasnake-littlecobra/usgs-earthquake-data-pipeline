import argparse
import requests
import polars as pl
import geojson
import json

# import datetime
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import os
import logging
from save_to_delta import save_to_delta_table
from save_to_delta import upload_delta_to_s3
# from save_to_cassandra import save_to_cassandra_main
from usgs_tsunami_count_fact_silver import convert_save_to_silver_delta_lake

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)

# logging.basicConfig(
#     filename="basic.log",
#     encoding="utf-8",
#     level=logging.INFO,
#     filemode="w",
#     format="%(process)d-%(levelname)s-%(message)s",
# )

# logging.info("This will write in basic.log")
# logging.waring("This will also write in basic.log")
# logging.debug("This will not write in basic.log")

# delta_lake_output_dir = "usgs-delta-lake-directory"
# delta_dir_raw = os.path.join(delta_lake_output_dir, "usgs-delta-lake-raw")
# delta_table_path = "delta-lake/usgs-delta-data"

project_name = "usgs-delta-lake-bucket"
bucket_name = "usgs-delta-lake-bucket"
delta_s3_key_raw = f"usgs-delta-lake-raw"
delta_s3_key_silver = f"usgs-delta-lake-silver"

usgs_earthquake_events_schema = {
    "id": pl.Utf8,  # Unique earthquake ID, assumed to always exist
    "month": pl.Int32,  # Extracted month, integer type
    "year": pl.Int32,  # Extracted year, integer type
    "magnitude": pl.Float64,  # Magnitude can be null
    "latitude": (pl.Float64),  # Latitude of the event
    "longitude": (pl.Float64),  # Longitude of the event
    "depth": (pl.Float64),  # Depth of the event, can be null
    "eventtime": pl.Datetime,  # Event timestamp, assumed to always exist
    "updated": (pl.Datetime),  # Last updated timestamp, nullable
    "place": (pl.Utf8),  # Place description, can be null
    "url": (pl.Utf8),  # URL to the event details, nullable
    "detail": (pl.Utf8),  # Additional detail URL, nullable
    "felt": (pl.Int32),  # Number of reports, nullable
    "cdi": (pl.Float64),  # Community Internet Intensity, nullable
    "mmi": (pl.Float64),  # Modified Mercalli Intensity, nullable
    "alert": (pl.Utf8),  # Alert level, can be null
    "status": (pl.Utf8),  # Event status, can be null
    "tsunami": (pl.Int32),  # Tsunami flag, nullable
    "significance": (pl.Int32),  # Significance score, nullable
    "network": (pl.Utf8),  # Contributing network, nullable
    "code": (pl.Utf8),  # Network code, nullable
    "ids": (pl.Utf8),  # Event IDs, nullable
    "sources": (pl.Utf8),  # Data sources, nullable
    "types": (pl.Utf8),  # Event types, nullable
    "nst": (pl.Int32),  # Number of stations, nullable
    "dmin": (pl.Float64),  # Minimum distance to earthquake, nullable
    "rms": (pl.Float64),  # Root mean square residual, nullable
    "gap": (pl.Float64),  # Gap between stations, nullable
    "magnitude_type": (pl.Utf8),  # Magnitude type, nullable
    "type": (pl.Utf8),  # General type of event, nullable
    "title": (pl.Utf8),  # Event title, nullable
    "geometry": pl.Utf8,  # Geometry JSON as string, assumed to always exist
}


def call_usgs_date_time(API_URL: str, start_time: str, end_time: str) -> dict:
    """Fetch earthquake data from the USGS API."""
    try:
        params = {"format": "geojson", "starttime": start_time, "endtime": end_time}
        logging.info(
            f"url: {API_URL} with starttime: {start_time} and endtime: {end_time}"
        )
        response = requests.get(API_URL, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.info(f"Error fetching data from API: {e}")
        return {}


# @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def call_usgs_date_time_and_limit_offset(
    API_URL: str, start_time: str, end_time: str, limit: int, offset: int
) -> dict:
    """Fetch earthquake data from the USGS API."""
    try:
        params = {
            "format": "geojson",
            "starttime": start_time,
            "endtime": end_time,
            "limit": limit,
            "offset": offset,
        }
        logging.info(
            f"url: {API_URL} with starttime: {start_time} and endtime: {end_time} with offset: {offset}"
        )
        response = requests.get(API_URL, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.info(f"Error fetching data from USGS API: {e}")
        return {"error": "error", "message": str(e)}
    # except requests.exceptions.HTTPError as http_err:
    #     status_code = response.status_code
    #     logging.warning(
    #         f"HTTPError (Status Code {status_code}) for {start_time} to {end_time}: {http_err}"
    #     )
    #     return {"error": "http_error", "status_code": status_code}
    # except requests.exceptions.Timeout:
    #     logging.warning(f"Timeout error for {start_time} to {end_time}.")
    #     return {"error": "timeout"}
    # except Exception as e:
    #     logging.error(f"Unexpected error: {e}")
    #     return {"error": "unexpected", "message": str(e)}


def call_usgs_date_by_limit(API_URL: str, limit: int) -> dict:
    """Fetch earthquake data from the USGS API."""
    try:
        params = {"format": "geojson", "limit": limit}
        logging.info(f"url: {API_URL} with limit: {limit}")
        response = requests.get(API_URL, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.info(f"Error fetching data from API: {e}")
        return {}


# Function to convert timestamp to month_year
def extract_month(timestamp):
    # Convert timestamp (milliseconds) to datetime object
    dt = datetime.fromtimestamp(timestamp / 1000)
    # Extract year and month in YYYY-MM format
    return dt.strftime("%m")


# Function to convert timestamp to month_year
def extract_year(timestamp):
    # Convert timestamp (milliseconds) to datetime object
    dt = datetime.fromtimestamp(timestamp / 1000)
    # Extract year and month in YYYY-MM format
    return dt.strftime("%Y")


def parse_geojson_to_dataframe(data: dict) -> pl.DataFrame:
    """Parse GeoJSON data into a Polars DataFrame."""
    print("inside parse geojson")
    # print(data)
    features = data.get("features", [])
    if not features:
        print("No earthquake data found in the response.")
        return pl.DataFrame()

    # Extract relevant fields
    rows = []
    print('going to iterate on features now')
    for feature in features:
        # print("iterating features array")
        props = feature["properties"]
        geom = feature["geometry"]
        timestamp = props["time"]
        month = extract_month(timestamp)
        # print(month)
        year = extract_year(timestamp)
        # print(year)

        rows.append(
            {
                "id": feature["id"],
                "month": month,
                "year": year,
                "magnitude": props.get("mag"),
                "latitude": geom["coordinates"][1],
                "longitude": geom["coordinates"][0],
                "depth": (
                    geom["coordinates"][2] if len(geom["coordinates"]) > 2 else None
                ),
                "eventtime": datetime.fromtimestamp(props["time"] / 1000),
                "updated": (
                    datetime.fromtimestamp(props["updated"] / 1000)
                    if props.get("updated")
                    else None
                ),
                "place": props.get("place"),
                "url": props.get("url"),
                "detail": props.get("detail"),
                "felt": props.get("felt"),
                "cdi": props.get("cdi"),
                "mmi": props.get("mmi"),
                "alert": props.get("alert"),
                "status": props.get("status"),
                "tsunami": props.get("tsunami"),
                "significance": props.get("sig"),
                "network": props.get("net"),
                "code": props.get("code"),
                "ids": props.get("ids"),
                "sources": props.get("sources"),
                "types": props.get("types"),
                "nst": props.get("nst"),
                "dmin": props.get("dmin"),
                "rms": props.get("rms"),
                "gap": props.get("gap"),
                "magnitude_type": props.get("magType"),
                "type": props.get("type"),
                "title": props.get("title"),
                "geometry": geojson.dumps(
                    {"type": geom["type"], "coordinates": geom["coordinates"]}
                ),
            }
        )

    print('done iterating on features now')
    return pl.DataFrame(rows, schema=usgs_earthquake_events_schema)


def save_to_csv(dataframe: pl.DataFrame, delta_lake_output_dir: str):
    """Save the DataFrame to a timestamped CSV file."""
    if dataframe.is_empty():
        logging.info("No data to save.")
        return
    os.makedirs(delta_lake_output_dir, exist_ok=True)
    print('saving csv to: {delta_lake_output_dir}')
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(delta_lake_output_dir, f"earthquake_data_{timestamp}.csv")
    dataframe.write_csv(file_path)
    logging.info(f"CSV: Data saved to {file_path}")


def save_to_json(dataframe: pl.DataFrame, delta_lake_output_dir: str):
    """Save the DataFrame to a timestamped JSON file."""
    if dataframe.is_empty():
        logging.info("No data to save.")
        return

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(delta_lake_output_dir, f"earthquake_data_{timestamp}.json")
    dataframe.write_json(file_path)
    logging.info(f"JSON: Data saved to {file_path}")


def fetch_data_by_year_range(
    api_url: str,
    start_year: int,
    end_year: int,
    limit: int,
    delta_lake_output_dir: str,
    output_files_dir: str,
    cluster_ips: str,
    keyspace: str,
    table_name: str,
    batch_size: str,
    timeout: str,
) -> None:
    """Fetch earthquake data for a range of years, month by month."""
    try:
        logging.info("Starting year range fetch.")
        start_date = datetime(year=start_year, month=1, day=1)
        end_date = datetime(year=end_year, month=12, day=31)

        current_start_date = start_date

        while current_start_date < end_date:
            current_end_date = current_start_date + relativedelta(months=1)
            current_end_date = min(current_end_date, end_date)

            start_time_iso = current_start_date.strftime("%Y-%m-%d")
            end_time_iso = current_end_date.strftime("%Y-%m-%d")

            logging.info(f"Fetching data for range {start_time_iso} to {end_time_iso}")

            success = fetch_data_by_limit_range(
                api_url,
                start_time_iso,
                end_time_iso,
                limit,
                delta_lake_output_dir,
                output_files_dir,
                cluster_ips,
                keyspace,
                table_name,
                batch_size,
                timeout,
            )

            if not success:
                logging.warning(
                    f"Retrying range {start_time_iso} to {end_time_iso} with weekly increments."
                )
                weekly_start_date = current_start_date
                while weekly_start_date < current_end_date:
                    weekly_end_date = weekly_start_date + relativedelta(weeks=1)
                    weekly_end_date = min(weekly_end_date, current_end_date)

                    start_time_iso = weekly_start_date.strftime("%Y-%m-%d")
                    end_time_iso = weekly_end_date.strftime("%Y-%m-%d")

                    logging.info(
                        f"Retrying weekly range {start_time_iso} to {end_time_iso}"
                    )
                    success = fetch_data_by_limit_range(
                        api_url,
                        start_time_iso,
                        end_time_iso,
                        limit,
                        delta_lake_output_dir,
                        cluster_ips,
                        keyspace,
                        table_name,
                        batch_size,
                        timeout,
                    )

                    if success:
                        break
                    weekly_start_date = weekly_end_date

            current_start_date = current_end_date

    except Exception as e:
        logging.error(f"Unexpected error in year range fetch: {e}")


def fetch_data_by_limit_range(
    api_url: str,
    start_time_iso: str,
    end_time_iso: str,
    limit: int,
    delta_lake_output_dir: str,
    output_files_dir: str,
    cluster_ips: str,
    keyspace: str,
    table_name: str,
    batch_size: str,
    timeout: str,
) -> bool:
    """Fetch earthquake data in chunks, with error handling."""
    try:
        offset = 1
        while True:
            logging.info(f"Fetching data with limit {limit} and offset {offset}")
            try:
                data = call_usgs_date_time_and_limit_offset(
                    api_url, start_time_iso, end_time_iso, limit, offset
                )
                # print(data.get("features"))
                features = data.get("features", [])
                if not features:
                    logging.info(
                        f"No more data for range {start_time_iso} to {end_time_iso} at offset {offset}."
                    )
                    break
                
                print('found features...', output_files_dir)
                dataframe = parse_geojson_to_dataframe(data)
                
                # save_to_csv(dataframe, output_files_dir)
                
                delta_dir_raw = os.path.join(delta_lake_output_dir, "usgs-delta-lake-raw")
                save_to_delta_table(dataframe, delta_dir_raw, mode="append")

                logging.info("Uploading the raw delta lake to Object Storage...", delta_s3_key_raw)
                upload_delta_to_s3(delta_dir_raw, bucket_name, delta_s3_key_raw)
                
                logging.info("Uploading the silver delta lake to Object Storage...", delta_s3_key_silver)
                delta_dir_silver = os.path.join(delta_lake_output_dir, "usgs-delta-lake-silver")
                upload_delta_to_s3(delta_dir_silver, bucket_name, delta_s3_key_silver)
                
                # save_to_cassandra_main(
                #     cluster_ips, keyspace, table_name, dataframe, batch_size, timeout
                # )
                
                return True
                offset += limit
                if len(features) < limit:
                    break

            except requests.exceptions.HTTPError as http_err:
                status_code = http_err.response.status_code
                logging.warning(f"HTTPError {status_code}: Retrying.")
                if 500 <= status_code < 600 or 400 <= status_code < 500:
                    return False
                else:
                    raise

            except Exception as e:
                logging.error(f"Unexpected error in limit range fetch: {e}")
                return False

        return True

    except Exception as e:
        logging.error(f"Critical error in limit range fetch: {e}")
        return False


def ETLIngestion() -> bool:
    logging.info("Starting main...")
    # Set up CLI arguments
    parser = argparse.ArgumentParser(
        description="Ingest earthquake data from USGS API and save to CSV."
    )
    parser.add_argument(
        "--starttime",
        required=False,
        default="2014-01-01",
        help="Start time for the query (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--endtime",
        required=False,
        default="2014-01-02",
        help="End time for the query (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--output_dir", default="usgs-delta-lake-directory", help="Directory to save the CSV files."
    )
    parser.add_argument(
        "--output_files", default="usgs-output-files", help="Directory to save the CSV files."
    )
    parser.add_argument(
        "--cassandra", action="store_true", help="Enable Cassandra ingestion"
    )
    parser.add_argument(
        "--cluster_ips",
        type=str,
        default="127.0.0.1",
        help="Cassandra cluster IPs (comma-separated)",
    )
    parser.add_argument(
        "--keyspace",
        type=str,
        default="usgs_earthquake_events_keyspace",
        help="Cassandra keyspace",
    )
    parser.add_argument(
        "--table_name",
        type=str,
        default="usgs_earthquake_events",
        help="Cassandra table name for individual fields",
    )
    parser.add_argument(
        "--table_name_geojson",
        type=str,
        default="earthquake_geojson",
        help="Cassandra table name for GeoJSON data",
    )
    parser.add_argument(
        "--batch_size", type=int, default=100, help="Batch size for Cassandra inserts"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=2,
        help="Timeout between batch inserts (in seconds)",
    )
    args = parser.parse_args()

    # print("args.cluster_ips")
    # print(args.cluster_ips)
    # print("args.keyspace")
    # print(args.keyspace)

    # Prepare API URL and directory
    API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    os.makedirs(args.output_dir, exist_ok=True)

    # Fetch, parse, and save data
    logging.info("Calling Fetch Earthquake api fetch_data_by_limit_range")
    # data = fetch_earthquake_data(API_URL, args.starttime, args.endtime)
    # print(data)

    # years_to_fetch = 10
    # data = extract_data_for_past_years(API_URL, years=years_to_fetch)
    # print(f"Total events fetched: {len(data)}")

    # data = fetch_data_by_year_range(API_URL, start_year=2010, end_year=2010, delta_lake_output_dir= args.output_dir, cluster_ips=args.cluster_ips, keyspace=args.keyspace, table_name=args.table_name, batch_size=args.batch_size, timeout=args.timeout)
    data = fetch_data_by_year_range(
        API_URL,
        start_year=2010,
        end_year=2011,
        limit=10000,
        delta_lake_output_dir=args.output_dir,
        output_files_dir=args.output_files,
        cluster_ips=args.cluster_ips,
        keyspace=args.keyspace,
        table_name=args.table_name,
        batch_size=args.batch_size,
        timeout=args.timeout,
    )

    return data


def ETLSilverLayer():
    logging.info(f"inside ETL Silver Layer function now!")
    silver_success = convert_save_to_silver_delta_lake()
    logging.info(f"back from etl silver layer")


if __name__ == "__main__":
    ETLIngestion()
    print("---- came back from etlingestions ---- ",ETLIngestion())
    if ETLIngestion:
        ETLSilverLayer()

    logging.info(f"Done with Data Ingestion/Silver data processing and Main function!")
