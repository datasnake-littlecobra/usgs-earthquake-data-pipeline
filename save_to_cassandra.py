import geojson
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging


def save_to_cassandra_main(
    cluster_ips, keyspace, table_name, dataframe, batch_size, timeout
):
    logging.info("Inside Cassandra Connect call:")
    logging.info(cluster_ips.split(","))
    logging.info(keyspace)
    session = connect_cassandra(cluster_ips.split(","), keyspace)
    batch_insert_cassandra(session, table_name, dataframe, batch_size, timeout)


def convert_to_geojson(row):
    """Convert a row to GeoJSON format."""
    return geojson.Feature(
        geometry=geojson.Point((row["longitude"], row["latitude"], row["depth"])),
        properties={
            "id": row["id"],
            "magnitude": row["magnitude"],
            "place": row["place"],
            "time": row["time"].isoformat(),
            "tsunami": row["tsunami"],
            "significance": row["significance"],
            "type": row["type"],
        },
    )


def connect_cassandra(cluster_ips, keyspace):
    logging.info(f"Connecting to Cassandra cluster: {cluster_ips}")
    try:
        """Connect to Cassandra."""
        USERNAME = "cassandra"
        PASSWORD = "cassandra"
        auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
        cluster = Cluster(
            cluster_ips, auth_provider=auth_provider
        )  # Replace with container's IP if needed
        session = cluster.connect()
        session.set_keyspace("usgs_earthquake_events_keyspace")
        logging.info("Connected to cassandra...")
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        raise


def batch_insert_cassandra(session, table_name, dataframe, batch_size=100, timeout=2):
    """Insert data into Cassandra in batches."""
    insert_query = f"""
    INSERT INTO {table_name} (
        id, month, year, magnitude, latitude, longitude, depth, eventtime, updated, place, url, detail,
        felt, cdi, mmi, alert, status, tsunami, significance, network, code, ids, sources,
        types, nst, dmin, rms, gap, magnitude_type, type, title, geometry
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(insert_query)
    batch = session.new_batch_statement()

    for idx, row in enumerate(dataframe.iterrows(named=True)):
        batch.add(
            prepared,
            (
                row["id"],
                row["month"],
                row["year"],
                row["magnitude"],
                row["latitude"],
                row["longitude"],
                row["depth"],
                row["eventtime"],
                row["updated"],
                row["place"],
                row["url"],
                row["detail"],
                row["felt"],
                row["cdi"],
                row["mmi"],
                row["alert"],
                row["status"],
                row["tsunami"],
                row["significance"],
                row["network"],
                row["code"],
                row["ids"],
                row["sources"],
                row["types"],
                row["nst"],
                row["dmin"],
                row["rms"],
                row["gap"],
                row["magnitude_type"],
                row["type"],
                row["title"],
                row["geometry"],
            ),
        )

        if (idx + 1) % batch_size == 0:
            session.execute(batch)
            print(f"Inserted {idx + 1} rows...")
            batch.clear()
            time.sleep(timeout)

    if batch:
        session.execute(batch)
        print("Inserted remaining rows.")

    # def batch_insert_geojson(session, table_name, dataframe, batch_size=100, timeout=2):
    """Insert GeoJSON data into Cassandra in batches."""
    insert_query = f"INSERT INTO {table_name} (id, geojson) VALUES (?, ?)"
    prepared = session.prepare(insert_query)
    batch = session.new_batch_statement()

    for idx, row in enumerate(dataframe.iter_rows(named=True)):
        geojson_data = row["geojson"]
        batch.add(prepared, (row["id"], geojson_data))

        if (idx + 1) % batch_size == 0:
            session.execute(batch)
            print(f"Inserted {idx + 1} GeoJSON rows...")
            batch.clear()
            time.sleep(timeout)

    if batch:
        session.execute(batch)
        print("Inserted remaining GeoJSON rows.")
