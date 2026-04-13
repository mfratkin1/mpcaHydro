# mpcaHydro

Modules for downloading hydrology data from MPCA servers and databases.

Please note I am creating and maintaining this package to learn and practice programming.

---

## Overview

**mpcaHydro** is a Python package that downloads, stores, and retrieves water-quality and streamflow data from multiple Minnesota Pollution Control Agency (MPCA) data sources. It brings together information from several independent systems into a single local database so you can query and analyze data from one place instead of connecting to each system separately.

### Data Sources

The package pulls from four data sources:

| Source | What It Contains | How It Connects |
|--------|-----------------|-----------------|
| **WISKI** | Continuous streamflow (discharge), water temperature, dissolved oxygen, and other sensor-based time-series data collected by MPCA monitoring stations | Public web API (KiWIS) — no credentials required |
| **EQuIS** | Lab-analyzed water-quality sample results (nutrients, solids, chlorophyll, etc.) from MPCA's environmental database | Oracle database — requires MPCA username and password |
| **SWD (Surface Water Data)** | Similar water-quality sample results available through MPCA's public Surface Water Data portal | Public web API — no credentials required |
| **CSG (Cooperative Stream Gauging)** | Discharge, water temperature, and dissolved oxygen data from the Minnesota DNR's stream gauging network | Public web API — no credentials required |

### Supported Constituents

The package works with these water-quality and hydrological measurements:

| Abbreviation | Full Name |
|-------------|-----------|
| **Q** | Discharge (streamflow) |
| **QB** | Baseflow (the portion of streamflow from groundwater) |
| **TSS** | Total Suspended Solids |
| **TP** | Total Phosphorus |
| **OP** | Orthophosphate |
| **TKN** | Total Kjeldahl Nitrogen |
| **N** | Nitrate + Nitrite Nitrogen |
| **WT** | Water Temperature |
| **DO** | Dissolved Oxygen |
| **WL** | Water Level |
| **CHLA** | Chlorophyll-a |

---

## Warehouse Structure

The warehouse is a local DuckDB database file (named `observations.duckdb`) that organizes all downloaded data into a structured layout with five separate areas called **schemas**. Each schema serves a different purpose in the data pipeline:

```
observations.duckdb
├── staging       — Raw data as it was first downloaded (temporary holding area)
├── analytics     — Cleaned and standardized data ready for analysis
├── reports       — Pre-built summary statistics and aggregated views
├── outlets       — Definitions linking monitoring stations to model reaches
└── mappings      — Reference lookup tables (parameter codes, station cross-references, quality codes)
```

### How Data Flows Through the Warehouse

The warehouse download-and-process pipeline currently supports **WISKI** and **EQuIS** data. SWD and CSG modules can download and transform data independently but are not yet integrated into the warehouse pipeline.

1. **Download** — Raw data is fetched from a source (WISKI or EQuIS).
2. **Staging** — The raw data is loaded directly into the `staging` schema, preserving all original columns exactly as they came from the source system.
3. **Transform** — The raw data goes through cleaning steps: unit conversions (e.g., micrograms to milligrams, Celsius to Fahrenheit), timezone normalization to Central Standard Time (UTC-6), mapping of technical parameter codes to readable constituent names, removal of invalid quality codes, and averaging of duplicate measurements within the same hour.
4. **Analytics** — The transformed data is loaded into the `analytics` schema in a standardized format with consistent columns: `datetime`, `value`, `station_id`, `station_origin`, `constituent`, and `unit`.
5. **Reports** — Summary views are automatically built on top of the analytics data.

### Warehouse Schemas in Detail

#### Staging Schema

The staging schema preserves the raw data exactly as downloaded from each source system. This gives you access to every original field for troubleshooting or custom analysis. It contains:

- **`staging.equis`** — Raw EQuIS data with all original Oracle columns (latitude, longitude, sample methods, lab qualifiers, detection limits, etc.).
- **`staging.wiski`** — Raw WISKI data with all original KiWIS fields (timestamps, quality codes, quality code names, parameter types, station metadata, etc.).

#### Analytics Schema

The analytics schema holds the final, cleaned data that you query for analysis. All data here follows a standard format. It contains:

- **`analytics.equis`** — Processed EQuIS data with standardized columns: `datetime`, `value`, `station_id`, `station_origin`, `constituent`, and `unit`.
- **`analytics.wiski`** — Processed WISKI data with the same standardized columns.
- **`analytics.observations`** — A combined view that merges both EQuIS and WISKI data into a single table. When you query this view, you get all observations from both sources together, with each row tagged by its `station_origin` so you can tell where it came from.
- **`analytics.outlet_observations`** — A view that links observations to model outlets by joining observation data with the outlet station mappings. Results are grouped by outlet, constituent, and datetime, with values averaged when multiple stations feed the same outlet.
- **`analytics.outlet_observations_with_flow`** — Extends outlet observations by joining in corresponding flow (discharge) and baseflow data for each outlet and timestamp. This makes it easy to analyze water-quality data alongside the flow conditions at the time of measurement.

#### Reports Schema

The reports schema provides pre-computed summaries so you do not have to write your own aggregation queries:

- **`reports.constituent_summary`** — Groups all observations by station and constituent. For each station–constituent combination, it shows: sample count, average value, minimum and maximum values, and the first and last year of available data.
- **`reports.outlet_constituent_summary`** — Same type of summary but organized by outlet instead of by individual station. Shows sample count, average, min, max, and date range for each outlet–constituent combination.
- **`reports.wiski_qc_count`** — Summarizes the quality codes present in the raw WISKI data for each station and parameter, along with descriptions of what each quality code means. Useful for understanding data quality before analysis.

#### Outlets Schema

The outlets schema manages the relationship between monitoring stations and hydrological model reaches. In HSPF modeling, an "outlet" is a point where you compare model predictions against observed data. A single outlet can be associated with multiple monitoring stations (e.g., a WISKI gauge and an EQuIS sampling site at the same location) and multiple model reaches. It contains:

- **`outlets.outlet_groups`** — Defines each outlet with an ID, name, and the model repository it belongs to.
- **`outlets.outlet_stations`** — Links stations to outlets (which stations feed data to which outlet).
- **`outlets.outlet_reaches`** — Links model reaches to outlets (which model segments correspond to which outlet).
- **`outlets.station_reach_pairs`** — A view that derives the many-to-many relationship between stations and reaches through their shared outlets.

#### Mappings Schema

The mappings schema stores reference lookup tables used during data processing:

- **`mappings.wiski_parametertype`** — Maps WISKI internal parameter type IDs (e.g., `11500`) to readable constituent names (e.g., `Q` for discharge).
- **`mappings.equis_casrn`** — Maps EQuIS chemical CAS registry numbers (e.g., `7723-14-0`) to constituent names (e.g., `TP` for Total Phosphorus).
- **`mappings.station_xref`** — Cross-reference table linking WISKI station numbers to EQuIS station IDs, along with Watershed IDs (WID).
- **`mappings.wiski_quality_codes`** — Reference table with descriptions of WISKI quality codes and whether each code is currently active.

---

## DataManagerWrapper

The `DataManagerWrapper` class is the main interface for users of this package. It manages the database connection and provides methods for downloading data, processing it, and querying results. You create one instance by pointing it at the path where your database lives.

### Creating a DataManagerWrapper

```python
from mpcaHydro.warehouse_functions import DataManagerWrapper

# Create a new wrapper (initializes the database if it doesn't exist)
dm = DataManagerWrapper('/path/to/my/observations.duckdb')

# Or reset the database and start fresh
dm = DataManagerWrapper('/path/to/my/observations.duckdb', reset=True)
```

### Methods Reference

#### Downloading Data

| Method | What It Does |
|--------|-------------|
| **`download_wiski_data(station_ids, ...)`** | Downloads time-series data for a list of WISKI station IDs from the KiWIS web service. The raw data is stored in `staging.wiski`, then automatically transformed (unit conversions, quality filtering, baseflow calculation) and loaded into `analytics.wiski`. You can specify a year range, choose whether to filter by quality codes, and select the baseflow separation method. If `replace=True`, existing data for those stations is removed first. |
| **`download_equis_data(station_ids, oracle_username, oracle_password, ...)`** | Downloads lab sample data for a list of EQuIS station IDs from the MPCA Oracle database. Requires Oracle credentials. The raw data is stored in `staging.equis`, then transformed (non-detect handling, timezone normalization, unit conversion, constituent mapping, hourly averaging) and loaded into `analytics.equis`. If `replace=True`, existing data for those stations is removed first. |

#### Processing Data

| Method | What It Does |
|--------|-------------|
| **`process_wiski_data(...)`** | Re-processes the raw WISKI data already in the staging schema. Reads from `staging.wiski`, applies the full transformation pipeline (quality filtering, unit conversion, column normalization, hourly averaging, baseflow calculation), and writes the result to `analytics.wiski`. Useful if you want to reprocess data with different quality code filters or a different baseflow method without re-downloading. |
| **`process_equis_data()`** | Re-processes the raw EQuIS data already in the staging schema. Reads from `staging.equis`, applies the full transformation pipeline (constituent mapping, timezone normalization, unit conversion, non-detect handling, hourly averaging), and writes the result to `analytics.equis`. |
| **`process_all_data(...)`** | Convenience method that runs both `process_wiski_data` and `process_equis_data` in sequence. |
| **`update_views()`** | Refreshes all database views (analytics, reports, and outlets). Call this after making manual changes to ensure views reflect the current data. |

#### Retrieving Observation Data

| Method | What It Does |
|--------|-------------|
| **`get_observation_data(station_ids, constituent, agg_period=None)`** | Retrieves time-series data from the combined `analytics.observations` view for the specified stations and constituent (e.g., `'TP'`, `'Q'`). Optionally aggregates to a time period — `'D'` for daily, `'H'` for hourly, `'W'` for weekly, `'ME'` for monthly, etc. The aggregation method is chosen automatically: averages for flow and concentration data, sums for load data. Returns a pandas DataFrame with a datetime index. |
| **`get_outlet_data(outlet_id, constituent, agg_period='D')`** | Retrieves observation data for a specific outlet, including matching flow and baseflow values at each timestamp. The result includes columns for the constituent value, observed flow, and observed baseflow. This is the primary method for getting data formatted for model calibration comparison. |
| **`get_station_data(station_id, station_origin)`** | Retrieves all processed observation data for a single station from `analytics.observations`. Returns every constituent and time step available for that station. |
| **`get_raw_data(station_id, station_origin)`** | Retrieves the original, un-transformed data from the staging schema for a specific station. Useful for inspecting the raw data before processing or debugging transformation issues. |
| **`get_station_ids(station_origin=None)`** | Returns a list of all station IDs that have data in the analytics schema. Optionally filter by origin (`'wiski'` or `'equis'`) to see only stations from one source. |
| **`get_outlets(model_name)`** | Returns a table of all outlet definitions (station-to-reach mappings) for a specific model repository. |

#### Summary Reports

| Method | What It Does |
|--------|-------------|
| **`get_constituent_summary()`** | Returns a table showing, for each station and constituent: how many observations exist, and the first and last year of data. Queries the observations directly from the analytics schema. |
| **`station_summary(constituent=None)`** | Returns the pre-computed constituent summary from `reports.constituent_summary`, which includes sample count, average value, min/max values, and date range for each station–constituent combination. Optionally filter to a single constituent. |
| **`outlet_summary()`** | Returns the pre-computed outlet summary from `reports.outlet_constituent_summary`, showing sample count, average, min, max, and date range for each outlet–constituent combination. |
| **`wiski_qc_counts()`** | Returns a breakdown of WISKI quality codes by station and parameter from `reports.wiski_qc_count`. Shows how many observations fall under each quality code, with human-readable descriptions. Useful for assessing data quality before analysis. |
| **`station_reach_pairs()`** | Returns the full mapping of stations to model reaches from `reports.station_reach_pairs`, showing which stations are connected to which model reach segments. |

#### Data Export

| Method | What It Does |
|--------|-------------|
| **`export_station_to_csv(station_id, station_origin, output_path)`** | Exports all processed analytics data for a station to a CSV file. |
| **`export_raw_to_csv(station_id, station_origin, output_path)`** | Exports raw staging data for a station to a CSV file. |

#### Schema Templates

| Method | What It Does |
|--------|-------------|
| **`get_equis_template()`** | Returns an empty DataFrame with the exact column names and types of the `staging.equis` table. Useful if you need to manually prepare data for loading into the warehouse. |
| **`get_wiski_template()`** | Returns an empty DataFrame with the exact column names and types of the `staging.wiski` table. |

---

## Station Cross-Reference (xref)

The MPCA uses different station ID systems in WISKI and EQuIS. The `xref` module provides standalone functions to translate between them using a built-in cross-reference table (`WISKI_EQUIS_XREF.csv`). Each row in the cross-reference table links a WISKI station number to its corresponding EQuIS station ID(s), along with a Watershed ID (WID).

| Function | What It Does |
|----------|-------------|
| **`get_wiski_stations()`** | Returns a list of all WISKI station numbers in the cross-reference table. |
| **`get_equis_stations()`** | Returns a list of all EQuIS station IDs in the cross-reference table. |
| **`wiski_equis_alias(wiski_station_id)`** | Given a WISKI station number, returns the single primary EQuIS station ID it maps to. Raises an error if there are multiple matches. |
| **`wiski_equis_associations(wiski_station_id)`** | Given a WISKI station number, returns all EQuIS station IDs associated with it (may be more than one). |
| **`equis_wiski_alias(equis_station_id)`** | Given an EQuIS station ID, returns the single WISKI station number it maps to. Raises an error if multiple matches exist. |
| **`equis_wiski_associations(equis_station_id)`** | Given an EQuIS station ID, returns all WISKI station numbers associated with it. |

---

## How Data Is Processed

Each data source goes through its own transformation pipeline before being stored in the analytics schema. Here is a summary of what happens during processing:

### WISKI Data Processing
1. **Convert units** — Standardizes temperature from Celsius to Fahrenheit, mass from kilograms to pounds, and renames cubic-feet-per-second to "cfs".
2. **Map parameters** — Converts internal WISKI parameter type IDs (e.g., `11500`) and station parameter numbers (e.g., `262`) to readable constituent names (e.g., `Q` for discharge).
3. **Normalize columns** — Renames raw column names (e.g., `Value`, `Timestamp`, `Quality Code`) to standard names (`value`, `datetime`, `quality_code`).
4. **Filter quality codes** — Removes observations with invalid or unreliable quality flags, keeping only codes that represent valid measurements.
5. **Average by hour** — Groups measurements within the same hour and takes the average to produce a consistent hourly time-series.
6. **Calculate baseflow** — For stations with discharge data, estimates the baseflow component using the Boughton method (separating groundwater contribution from surface runoff).

### EQuIS Data Processing
1. **Map constituents** — Converts chemical CAS registry numbers (e.g., `7723-14-0`) to readable names (e.g., `TP` for Total Phosphorus).
2. **Normalize timezones** — Converts all timestamps to a consistent Central Standard Time offset (UTC-6), accounting for daylight saving time.
3. **Normalize columns** — Selects and renames relevant columns to the standard format.
4. **Convert units** — Standardizes micrograms/L to milligrams/L, milligrams/gram to milligrams/L, and Celsius to Fahrenheit.
5. **Handle non-detects** — Replaces lab results below the detection limit with zero.
6. **Average by hour** — Groups samples taken within the same hour at the same station and averages them.

### SWD Data Processing
1. **Filter parameters** — Keeps only observations for supported constituents.
2. **Parse dates** — Combines separate date and time columns into a single datetime.
3. **Convert units** — Same unit conversions as EQuIS (micrograms to milligrams, Celsius to Fahrenheit, kilograms to pounds).
4. **Map constituents** — Converts parameter names to standard abbreviations.
5. **Average by hour** — Groups and averages within the same hour.
