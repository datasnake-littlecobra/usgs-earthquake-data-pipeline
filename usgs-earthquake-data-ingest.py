import argparse
import requests
import polars as pl
import geojson
import json
import datetime
import os
import logging
from save_to_delta import save_to_delta_table
from save_to_delta import upload_delta_to_s3
from save_to_cassandra import save_to_cassandra_main

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

output_dir = "output_directory"
delta_dir = os.path.join(output_dir, "usgs-delta-data")
# delta_table_path = "delta-lake/usgs-delta-data"

project_name = "usgs"
bucket_name = "usgs-bucket"
delta_s3_key = f"{project_name}/usgs_delta_lake"

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


def fetch_earthquake_data(api_url: str, start_time: str, end_time: str) -> dict:
    """Fetch earthquake data from the USGS API."""
    try:
        params = {"format": "geojson", "starttime": start_time, "endtime": end_time}
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return {}


# Function to convert timestamp to month_year
def extract_month(timestamp):
    # Convert timestamp (milliseconds) to datetime object
    dt = datetime.datetime.fromtimestamp(timestamp / 1000)
    # Extract year and month in YYYY-MM format
    return dt.strftime("%m")


# Function to convert timestamp to month_year
def extract_year(timestamp):
    # Convert timestamp (milliseconds) to datetime object
    dt = datetime.datetime.fromtimestamp(timestamp / 1000)
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
                "eventtime": datetime.datetime.fromtimestamp(props["time"] / 1000),
                "updated": (
                    datetime.datetime.fromtimestamp(props["updated"] / 1000)
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

    return pl.DataFrame(rows, schema=usgs_earthquake_events_schema)


def save_to_csv(dataframe: pl.DataFrame, output_dir: str):
    """Save the DataFrame to a timestamped CSV file."""
    if dataframe.is_empty():
        print("No data to save.")
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(output_dir, f"earthquake_data_{timestamp}.csv")
    dataframe.write_csv(file_path)
    print(f"Data saved to {file_path}")


def save_to_json(dataframe: pl.DataFrame, output_dir: str):
    """Save the DataFrame to a timestamped JSON file."""
    if dataframe.is_empty():
        print("No data to save.")
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(output_dir, f"earthquake_data_{timestamp}.json")
    dataframe.write_json(file_path)
    print(f"Data saved to {file_path}")


def main():
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
        "--output_dir", default="output-files", help="Directory to save the CSV files."
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
    api_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    os.makedirs(args.output_dir, exist_ok=True)

    # Fetch, parse, and save data
    logging.info("Calling Fetch Earthquake api...")
    data = fetch_earthquake_data(api_url, args.starttime, args.endtime)
    print(data)
    logging.info("Parsing geojson dataframe back from api call...")
    dataframe = parse_geojson_to_dataframe(data)
    print("--- dataframe.count() ---")
    print(dataframe.count())
    logging.info("Saving the dataframe to CSV...")
    save_to_csv(dataframe, args.output_dir)
    logging.info("Saving the dataframe to JSON...")
    save_to_json(dataframe, args.output_dir)
    logging.info("Saving the dataframe to local delta lake...")
    save_to_delta_table(dataframe, delta_dir, mode="append")
    logging.info("Uploading the delta lake to Object Storage...")
    # need research on appending vs overwrite
    # z order and other ways to make it efficient
    # upload_delta_to_s3(delta_dir, bucket_name, delta_s3_key)
    logging.info("Finished with Files...")
    logging.info("Going to call Cassandra Connect with:")
    logging.info(args.cluster_ips)
    logging.info(args.keyspace)
    save_to_cassandra_main(args.cluster_ips, args.keyspace, args.table_name, dataframe, args.batch_size, args.timeout)


if __name__ == "__main__":
    main()
