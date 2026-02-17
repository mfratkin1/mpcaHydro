-- calibration_schema.sql
-- Calibration configuration schema for SQLite
-- Separate from outlets.py DuckDB database

-- Table: calibration_locations
-- Represents calibration locations (groupings of stations)
CREATE TABLE IF NOT EXISTS calibration_locations (
    location_id INTEGER PRIMARY KEY,
    location_name TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    notes TEXT
);

-- Table: calibration_location_reaches
-- Reach mappings for calibration locations
-- Note: negative reach_ids indicate reaches to subtract from totals
CREATE TABLE IF NOT EXISTS calibration_location_reaches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL,
    reach_id INTEGER NOT NULL,
    is_upstream INTEGER DEFAULT 0,
    FOREIGN KEY (location_id) REFERENCES calibration_locations(location_id)
);

-- Table: calibration_flow_stations
-- Supplemental flow stations for locations (used for load calculations)
CREATE TABLE IF NOT EXISTS calibration_flow_stations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL,
    flow_station_id TEXT NOT NULL,
    FOREIGN KEY (location_id) REFERENCES calibration_locations(location_id)
);

-- Table: calibration_stations
-- Station metadata (separate from outlets.py station/reach mappings)
CREATE TABLE IF NOT EXISTS calibration_stations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    true_reach_id INTEGER,
    comments TEXT,
    FOREIGN KEY (location_id) REFERENCES calibration_locations(location_id)
);

-- Table: calibration_observations
-- Observation/constituent configurations for stations
CREATE TABLE IF NOT EXISTS calibration_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    constituent TEXT NOT NULL,
    start_year INTEGER,
    end_year INTEGER,
    avg_samples_per_year REAL,
    median_samples_per_year REAL,
    years_with_data INTEGER,
    total_samples INTEGER
);

-- Table: calibration_metrics
-- Metrics to calculate for observations
CREATE TABLE IF NOT EXISTS calibration_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    target REAL,
    weight REAL DEFAULT 1.0,
    enabled INTEGER DEFAULT 1,
    FOREIGN KEY (observation_id) REFERENCES calibration_observations(id)
);

-- Table: calibration_derived_constituents
-- Derived constituents (e.g., load from flow and concentration)
CREATE TABLE IF NOT EXISTS calibration_derived_constituents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id INTEGER NOT NULL,
    source_constituent TEXT NOT NULL,
    FOREIGN KEY (observation_id) REFERENCES calibration_observations(id)
);

-- Table: calibration_watershed_constraints
-- Watershed loading rate constraints
CREATE TABLE IF NOT EXISTS calibration_watershed_constraints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL,
    constituent TEXT NOT NULL,
    target_rate REAL,
    min_rate REAL,
    max_rate REAL,
    FOREIGN KEY (location_id) REFERENCES calibration_locations(location_id)
);

-- Table: calibration_landcover_constraints
-- Landcover-specific loading rate constraints
CREATE TABLE IF NOT EXISTS calibration_landcover_constraints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watershed_constraint_id INTEGER NOT NULL,
    landcover_type TEXT NOT NULL,
    target_rate REAL,
    min_rate REAL,
    max_rate REAL,
    FOREIGN KEY (watershed_constraint_id) 
        REFERENCES calibration_watershed_constraints(id)
);
