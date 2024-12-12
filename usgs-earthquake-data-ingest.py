import argparse
import requests
import polars as pl
import geojson
import datetime
import os
import logging
from save_to_delta import save_to_delta_table
from save_to_delta import upload_delta_to_s3

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


def parse_geojson_to_dataframe(data: dict) -> pl.DataFrame:
    """Parse GeoJSON data into a Polars DataFrame."""
    features = data.get("features", [])
    if not features:
        print("No earthquake data found in the response.")
        return pl.DataFrame()

    # Extract relevant fields
    rows = []
    for feature in features:
        props = feature["properties"]
        geom = feature["geometry"]
        rows.append(
            {
                "id": feature["id"],
                "magnitude": props.get("mag"),
                "place": props.get("place"),
                "time": datetime.datetime.fromtimestamp(props["time"] / 1000),
                "tsunami": props.get("tsunami"),
                "significance": props.get("sig"),
                "type": props.get("type"),
                "longitude": geom["coordinates"][0],
                "latitude": geom["coordinates"][1],
                "depth": geom["coordinates"][2],
            }
        )

    return pl.DataFrame(rows)


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
    args = parser.parse_args()

    # Prepare API URL and directory
    api_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    os.makedirs(args.output_dir, exist_ok=True)

    # Fetch, parse, and save data
    logging.info("Calling Fetch Earthquake api...")
    data = fetch_earthquake_data(api_url, args.starttime, args.endtime)
    logging.info("Parsing geojson dataframe back from api call...")
    dataframe = parse_geojson_to_dataframe(data)
    logging.info("Saving the dataframe to CSV...")
    save_to_csv(dataframe, args.output_dir)
    logging.info("Saving the dataframe to JSON...")
    save_to_json(dataframe, args.output_dir)
    logging.info("Saving the dataframe to local delta lake...")
    save_to_delta_table(dataframe, delta_dir, mode="append")
    logging.info("Uploading the delta lake to Object Storage...")
    upload_delta_to_s3(delta_dir, bucket_name, delta_s3_key)
    logging.info("Finished...")

    # write_to_cassandra()

if __name__ == "__main__":
    main()
