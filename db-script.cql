CREATE KEYSPACE IF NOT EXISTS usgs_earthquake_events_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE usgs_earthquake_events_keyspace;
DROP TABLE usgs_earthquake_events;
CREATE TABLE IF NOT EXISTS usgs_earthquake_events (
    id TEXT,
    month INT,
    year INT,
    magnitude FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    depth FLOAT,
    eventtime TIMESTAMP,
    updated TIMESTAMP,
    place TEXT,
    url TEXT,
    detail TEXT,
    felt INT,
    cdi FLOAT,
    mmi FLOAT,
    alert TEXT,
    status TEXT,
    tsunami INT,
    significance INT,
    network TEXT,
    code TEXT,
    ids TEXT,
    sources TEXT,
    types TEXT,
    nst INT,
    dmin FLOAT,
    rms FLOAT,
    gap FLOAT,
    magnitude_type TEXT,
    type TEXT,
    title TEXT,
    geometry TEXT,
    PRIMARY KEY ((month, year), eventtime)
) WITH CLUSTERING ORDER BY (eventtime DESC);

-- INSERT INTO usgs_earthquake_events (
--     id, month, year, magnitude, latitude, longitude, depth, eventtime, updated, place, url, detail,
--     felt, cdi, mmi, alert, status, tsunami, significance, network, code, ids, sources,
--     types, nst, dmin, rms, gap, magnitude_type, type, title, geometry
-- ) VALUES (
--     'abcd1234',12, 2024, 4.5, 36.12, -117.12, 5.2, '2024-12-14T08:30:00Z', '2024-12-14T09:00:00Z',
--     'Someplace, CA', 'http://example.com', 'details...', 10, 2.3, 3.1, 'green', 'reviewed',
--     0, 120, 'us', 'abcd1234', ',abcd1234,xyz1234', 'us,xyz', 'earthquake', 45, 0.001,
--     0.32, 76, 'mb', 'earthquake', 'An example title', '{"type":"Point","coordinates":[-117.12,36.12]}'
-- ) IF NOT EXISTS;

-- maybe create a materialized view later on