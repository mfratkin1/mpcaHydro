-- analytics_tables.sql
-- Create tables in the analytics schema for processed/transformed data

CREATE TABLE IF NOT EXISTS analytics.equis (
    datetime TIMESTAMP,
    value DOUBLE,
    station_id TEXT,
    station_origin TEXT,
    constituent TEXT,
    unit TEXT
);

CREATE TABLE IF NOT EXISTS analytics.wiski (
    datetime TIMESTAMP,
    value DOUBLE,
    station_id TEXT,
    station_origin TEXT,
    constituent TEXT,
    unit TEXT
);
