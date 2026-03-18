# mpcaHydro

Modules for downloading hydrology data from MPCA servers and databases.

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
| **WPLMN (Watershed Pollutant Load Monitoring Network)** | Modeled and measured daily pollutant loads and concentrations from MPCA's long-term monitoring network | Public web API (KiWIS) — no credentials required |

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

The warehouse is a local DuckDB database file (named `observations.duckdb`) that organizes all downloaded data into a structured layout with four separate areas called **schemas**. Each schema serves a different purpose in the data pipeline:

```
observations.duckdb
├── staging       — Raw data as it was first downloaded (temporary holding area)
├── analytics     — Cleaned and standardized data ready for analysis
├── reports       — Pre-built summary statistics and aggregated views
└── hspf          — Reserved for HSPF hydrological modeling data
```

### How Data Flows Through the Warehouse

1. **Download** — Raw data is fetched from a source (WISKI, EQuIS, SWD, or WPLMN).
2. **Transform** — The raw data goes through cleaning steps: unit conversions (e.g., micrograms to milligrams, Celsius to Fahrenheit), timezone normalization to Central Standard Time (UTC-6), mapping of technical parameter codes to readable constituent names, removal of invalid quality codes, and averaging of duplicate measurements within the same hour.
3. **Staging** — The transformed data is loaded into the `staging` schema as an intermediate step.
4. **Analytics** — Data is moved from staging to the `analytics` schema, where it is stored in its final, query-ready form.
5. **Reports** — Summary views are automatically built on top of the analytics data.

### Warehouse Schemas in Detail

#### Staging Schema

The staging schema is a temporary landing zone. When data is first loaded into the warehouse — whether from a CSV file, a Parquet file, or directly from a download — it goes here first. This allows the data to be inspected or re-processed before being promoted to analytics. Key tables include `staging.equis_processed` (EQuIS data after initial transformation) and `staging.wiski` (WISKI data after initial transformation).

#### Analytics Schema

The analytics schema holds the final, cleaned data that you query for analysis. It contains:

- **`analytics.equis`** — All processed EQuIS sample data with standardized columns: datetime, value, station_id, station_origin, constituent, and unit.
- **`analytics.wiski`** — All processed WISKI time-series data with the same standardized columns.
- **`analytics.observations`** — A combined view that merges both EQuIS and WISKI data into a single table. When you query this view, you get all observations from both sources together, with each row tagged by its `station_origin` so you can tell where it came from.

#### Reports Schema

The reports schema provides pre-computed summaries so you do not have to write your own aggregation queries:

- **`reports.constituent_summary`** — A summary view that groups all observations by station and constituent. For each station–constituent combination, it shows: the total number of samples (`sample_count`), the average measured value (`average_value`), the minimum and maximum measured values (`min_value`, `max_value`), and the first and last year of available data (`start_date`, `end_date`).

#### HSPF Schema

Reserved for data formatted for HSPF (Hydrological Simulation Program – Fortran) modeling workflows. This schema is created automatically but is populated by separate modeling processes.

### Warehouse Functions

These are the building-block functions that manage the database. Most users will not call these directly — they are used internally by the `dataManager` class — but understanding them helps explain how the warehouse works.

| Function | What It Does |
|----------|-------------|
| **`init_db`** | Creates the database file and sets up all four schemas. On first run, it also attempts to create the combined observations view and constituent summary report (these will be available once data is loaded). If you pass `reset=True`, it deletes the existing database and starts fresh. |
| **`connect`** | Opens a connection to the database file. Creates the folder for the database if it does not already exist. Can open in read-only mode for safe querying. |
| **`create_schemas`** | Creates the four schemas (staging, analytics, reports, hspf) if they do not already exist. |
| **`create_combined_observations_view`** | Builds the `analytics.observations` view that unions EQuIS and WISKI data together. |
| **`create_constituent_summary_report`** | Builds the `reports.constituent_summary` view with aggregated statistics for each station and constituent. |
| **`load_df_to_staging`** | Takes a pandas DataFrame and saves it as a table in the staging schema. |
| **`load_df_to_analytics`** | Takes a pandas DataFrame and saves it as a table in the analytics schema. |
| **`load_csv_to_staging`** | Reads a CSV file and loads it directly into a staging table. |
| **`load_parquet_to_staging`** | Reads a Parquet file and loads it directly into a staging table. |
| **`migrate_staging_to_analytics`** | Copies a table from the staging schema to the analytics schema, promoting it to query-ready status. |
| **`load_df_to_table`** | General-purpose function that saves a DataFrame to any table in the database. |
| **`write_table_to_parquet`** | Exports a database table to a Parquet file for sharing or archiving. |
| **`write_table_to_csv`** | Exports a database table to a CSV file for sharing or archiving. |
| **`dataframe_to_parquet`** | Converts a pandas DataFrame directly to a Parquet file without going through the database. |

---

## dataManager Class

The `dataManager` class is the main interface for users of this package. It coordinates downloading data from remote sources, storing it in the local warehouse, and retrieving it for analysis. You create one instance by pointing it at a folder where your data will be stored.

### Creating a dataManager

```python
from mpcaHydro.data_manager import dataManager

# Without EQuIS access (can still use WISKI, SWD, and WPLMN)
dm = dataManager('/path/to/my/data/folder')

# With EQuIS access (enables Oracle database downloads)
dm = dataManager('/path/to/my/data/folder',
                 oracle_user='your_username',
                 oracle_password='your_password')
```

When you create a `dataManager`, it sets up:
- A **folder path** where CSV data files and the DuckDB database will live.
- A **data cache** (in-memory dictionary) so that data you have already loaded does not need to be re-read from disk each time you request it.
- An **optional Oracle connection** for accessing EQuIS data (only needed if you have MPCA database credentials).

### Methods Reference

#### Setting Up the Warehouse

| Method | What It Does |
|--------|-------------|
| **`_build_warehouse()`** | Initializes the local DuckDB database by creating all schemas and views. Call this once before loading data for the first time. Internally calls `warehouse.init_db`. |
| **`_reconstruct_database()`** | Rebuilds the database from scratch by reading all CSV files in the data folder and loading them into a single `observations` table. Useful if the database file gets corrupted or if you want to reimport all your downloaded CSV data. |

#### Downloading Data

| Method | What It Does |
|--------|-------------|
| **`download_station_data(station_id, station_origin, ...)`** | Downloads data for a single station from the specified source. You specify the station ID and which system to fetch from (`'wiski'`, `'equis'`, `'swd'`, or `'wplmn'`). The method downloads the raw data, runs it through the appropriate cleaning/transformation pipeline, and saves the result as a CSV file in your data folder. If the file already exists, it skips the download unless you set `overwrite=True`. For WISKI sources, it also calculates baseflow from the discharge data. |
| **`download_stations_by_wid(wid_no, station_origin, ...)`** | Downloads data for all stations within a given Watershed ID (WID). It looks up which stations belong to that watershed using the cross-reference table, then calls `download_station_data` for each one. |
| **`connect_to_oracle()`** | Establishes a connection to the MPCA Oracle database for EQuIS data access. Requires that Oracle credentials were provided when the `dataManager` was created. |
| **`credentials_exist()`** | Returns `True` if Oracle username and password have been provided, `False` otherwise. Useful for checking whether EQuIS downloads are available before attempting them. |

#### Loading and Retrieving Data

| Method | What It Does |
|--------|-------------|
| **`load(station_id)`** | Retrieves all observation data for a station. First checks the in-memory cache — if you have already loaded this station during the current session, it returns the cached copy instantly. If not, it queries the `analytics.observations` table in the warehouse database. This is the primary method for getting station data. |
| **`get_data(station_id, constituent, agg_period='D')`** | Retrieves a time-series for a specific station and constituent (e.g., discharge, phosphorus). The data is aggregated to the specified time period — `'D'` for daily (default), `'H'` for hourly, `'W'` for weekly, `'ME'` for monthly, etc. The aggregation method is chosen automatically based on the unit: flow data (cfs) and concentration data (mg/l) use averages, while load data (lb) uses sums. Returns a pandas DataFrame with a datetime index and a `value` column. |
| **`get_wplmn_data(station_id, constituent, unit='mg/l', agg_period='YE', samples_only=True)`** | Retrieves data specifically from the WPLMN monitoring network. Similar to `get_data` but designed for WPLMN stations, which contain both modeled values and actual discrete samples. By default, it returns only the discrete sample measurements (`samples_only=True`) and aggregates them yearly (`'YE'`). Set `samples_only=False` to include modeled values as well. |

#### Querying Summary Information

| Method | What It Does |
|--------|-------------|
| **`constituent_summary(constituents=None)`** | Returns a summary table showing, for each station and constituent: how many observations exist (`sample_count`), and the first and last year of data (`start_date`, `end_date`). You can optionally pass a list of specific constituents to filter by (e.g., `['Q', 'TP']`), or leave it blank to see all constituents. This queries the `observations` table in the warehouse directly. |
| **`info(constituent)`** | Scans all CSV files in the data folder and returns a count of how many observations exist for each station and constituent combination. This is useful for a quick inventory of what data you have downloaded, without needing the warehouse database to be set up. |

#### Station Cross-Reference Lookups

The MPCA uses different station ID systems in WISKI and EQuIS. These methods let you translate between them using a built-in cross-reference table (`WISKI_EQUIS_XREF.csv`). Each row in the cross-reference table links a WISKI station number to its corresponding EQuIS station ID(s), along with a Watershed ID (WID).

| Method | What It Does |
|--------|-------------|
| **`get_wiski_stations()`** | Returns a list of all WISKI station numbers in the cross-reference table. |
| **`get_equis_stations()`** | Returns a list of all EQuIS station IDs in the cross-reference table. |
| **`wiski_equis_alias(wiski_station_id)`** | Given a WISKI station number, returns the single primary EQuIS station ID it maps to. Raises an error if there are multiple matches (which would indicate an ambiguous mapping). Returns an empty list if no match is found. |
| **`wiski_equis_associations(wiski_station_id)`** | Given a WISKI station number, returns all EQuIS station IDs associated with it. Unlike `wiski_equis_alias`, this returns the full list even if there are multiple associated EQuIS stations. |
| **`equis_wiski_alias(equis_station_id)`** | Given an EQuIS station ID, returns the single WISKI station number it maps to. Raises an error if multiple matches exist. Returns an empty list if no match is found. |
| **`equis_wiski_associations(equis_station_id)`** | Given an EQuIS station ID, returns all WISKI station numbers associated with it. Returns the full list of associated WISKI stations. |

---

## How Data Is Processed

Each data source goes through its own transformation pipeline before being stored. Here is a summary of what happens during processing:

### WISKI Data Processing
1. **Filter quality codes** — Removes observations with invalid or unreliable quality flags.
2. **Convert units** — Standardizes temperature from Celsius to Fahrenheit, mass from kilograms to pounds, and renames cubic-feet-per-second to "cfs".
3. **Map parameters** — Converts internal WISKI parameter type IDs (e.g., `11522`) to readable constituent names (e.g., `TP`).
4. **Average by hour** — Groups measurements within the same hour and takes the average to produce a consistent hourly time-series.
5. **Calculate baseflow** — For stations with discharge data, estimates the baseflow component using the Boughton method (separating groundwater contribution from surface runoff).

### EQuIS Data Processing
1. **Handle non-detects** — Replaces lab results below the detection limit with zero.
2. **Normalize timezones** — Converts all timestamps to a consistent Central Standard Time offset (UTC-6), accounting for daylight saving time.
3. **Convert units** — Standardizes micrograms/L to milligrams/L, milligrams/gram to milligrams/L, and Celsius to Fahrenheit.
4. **Map constituents** — Converts chemical CAS registry numbers (e.g., `7723-14-0`) to readable names (e.g., `TP` for Total Phosphorus).
5. **Average by hour** — Groups samples taken within the same hour at the same station and averages them.

### SWD Data Processing
1. **Filter parameters** — Keeps only observations for supported constituents.
2. **Parse dates** — Combines separate date and time columns into a single datetime.
3. **Convert units** — Same unit conversions as EQuIS (micrograms to milligrams, Celsius to Fahrenheit, kilograms to pounds).
4. **Map constituents** — Converts parameter names to standard abbreviations.
5. **Average by hour** — Groups and averages within the same hour.

### WPLMN Data Processing
1. **Remove invalid codes** — Filters out observations flagged as missing or invalid.
2. **Map constituents** — Converts internal parameter numbers to standard abbreviations.
3. **Standardize units** — Renames unit symbols to match the rest of the system (e.g., "ft³/s" becomes "cfs").
