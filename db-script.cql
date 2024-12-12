CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_keyspace;
DROP TABLE stocks;
CREATE TABLE IF NOT EXISTS stocks (
    stock_id UUID,
    symbol TEXT,
    price FLOAT,
    timestamp TIMESTAMP,
    PRIMARY KEY ( symbol, timestamp )
) WITH CLUSTERING ORDER BY (timestamp DESC);
INSERT INTO stocks (stock_id, symbol, price, timestamp) 
VALUES (uuid(), 'DATASNAKE', 1550.50, toTimestamp(now()))
IF NOT EXISTS;

-- maybe create a materialized view later on