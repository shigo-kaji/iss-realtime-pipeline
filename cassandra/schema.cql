CREATE KEYSPACE IF NOT EXISTS iss_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE iss_data;

CREATE TABLE IF NOT EXISTS iss_position (
    satellite      text,
    record_time    timestamp,
    latitude       double,
    longitude      double,
    altitude_km    double,
    velocity_kmh   double,
    visibility     text,
    country_code   text,
    location       text,
    processed_at   timestamp,
    PRIMARY KEY (satellite, record_time)
) WITH CLUSTERING ORDER BY (record_time DESC);

CREATE TABLE IF NOT EXISTS iss_telemetry (
    satellite      text,
    record_time    timestamp,
    footprint_km   double,
    daynum         double,
    solar_lat      double,
    solar_lon      double,
    units          text,
    processed_at   timestamp,
    PRIMARY KEY (satellite, record_time)
) WITH CLUSTERING ORDER BY (record_time DESC);
