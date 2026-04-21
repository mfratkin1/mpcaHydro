-- outlets_schema.sql
-- Schema for managing associations between model reaches and observation stations via outlets
-- Compatible with DuckDB and SQLite

CREATE SCHEMA IF NOT EXISTS outlets;

-- Table 1: outlet_groups
-- Represents a logical grouping that ties stations and reaches together
CREATE TABLE IF NOT EXISTS outlets.outlet_groups (
    outlet_id INTEGER PRIMARY KEY,
    repository_name TEXT NOT NULL,
    outlet_name TEXT,
    notes TEXT
);

-- Table 2: outlet_stations
-- One-to-many: outlet -> stations
CREATE TABLE IF NOT EXISTS outlets.outlet_stations (
    outlet_id INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    true_opnid INTEGER NOT NULL,
    comments TEXT,
    CONSTRAINT uq_station_origin UNIQUE (station_id, station_origin),
    FOREIGN KEY (outlet_id) REFERENCES outlets.outlet_groups(outlet_id)
);

-- Table 3: outlet_reaches
-- One-to-many: outlet -> reaches
-- A reach can appear in multiple outlets, enabling many-to-many overall
CREATE TABLE IF NOT EXISTS outlets.outlet_reaches (
    outlet_id INTEGER NOT NULL,
    reach_id INTEGER NOT NULL,
    repository_name TEXT NOT NULL,
    FOREIGN KEY (outlet_id) REFERENCES outlets.outlet_groups(outlet_id)
);

-- Note: the station_reach_pairs view is defined in views_outlets.sql and
-- created separately by create_outlets_tables() after populating the tables.

