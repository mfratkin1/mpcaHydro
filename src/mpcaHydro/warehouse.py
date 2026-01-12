import duckdb
import pandas as pd
from pathlib import Path
from mpcaHydro import outlets

def init_db(db_path: str,reset: bool = False):
    """
    Initialize the DuckDB database: create schemas and tables.
    """
    db_path = Path(db_path)
    if reset and db_path.exists():
        db_path.unlink()

    with connect(db_path.as_posix()) as con:
        # Create all schemas
        create_schemas(con)

        # Create tables
        create_outlets_tables(con)
        create_mapping_tables(con)
        create_analytics_tables(con)

        # Create views
        update_views(con)
        

def create_schemas(con: duckdb.DuckDBPyConnection):
    """
    Create staging, analytics, hspf, and reports schemas if they do not exist.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    con.execute("CREATE SCHEMA IF NOT EXISTS reports")
    con.execute("CREATE SCHEMA IF NOT EXISTS outlets")
    con.execute("CREATE SCHEMA IF NOT EXISTS mappings")

def create_analytics_tables(con: duckdb.DuckDBPyConnection):
    """
    Create necessary tables in the analytics schema.
    """
    con.execute("""
    CREATE TABLE IF NOT EXISTS analytics.equis (
        datetime TIMESTAMP,
        value DOUBLE,
        station_id TEXT,
        station_origin TEXT,
        constituent TEXT,
        unit TEXT
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS analytics.wiski (
        datetime TIMESTAMP,
        value DOUBLE,
        station_id TEXT,
        station_origin TEXT,
        constituent TEXT,
        unit TEXT
    );
    """)

def create_mapping_tables(con: duckdb.DuckDBPyConnection):
    """
    Create and populate tables in the mappings schema from Python dicts and CSVs.
    """
    # WISKI parametertype_id -> constituent
    wiski_parametertype_map = {
        '11522': 'TP', 
        '11531': 'TP', 
        '11532': 'TSS', 
        '11523': 'TSS',
        '11526': 'N', 
        '11519': 'N', 
        '11520': 'OP', 
        '11528': 'OP',
        '11530': 'TKN', 
        '11521': 'TKN', 
        '11500': 'Q', 
        '11504': 'WT',
        '11533': 'DO', 
        '11507': 'WL'
    }
    df_wiski_params = pd.DataFrame(wiski_parametertype_map.items(), columns=['parametertype_id', 'constituent'])
    con.execute("CREATE TABLE mappings.wiski_parametertype AS SELECT * FROM df_wiski_params")

    # EQuIS cas_rn -> constituent
    equis_casrn_map = {
        '479-61-8': 'CHLA', 
        'CHLA-CORR': 'CHLA', 
        'BOD': 'BOD', 
        'NO2NO3': 'N',
        '14797-55-8': 'NO3', 
        '14797-65-0': 'NO2', 
        '14265-44-2': 'OP',
        'N-KJEL': 'TKN', 
        'PHOSPHATE-P': 'TP', 
        '7723-14-0': 'TP',
        'SOLIDS-TSS': 'TSS', 
        'TEMP-W': 'WT', 
        '7664-41-7': 'NH3'
    }
    df_equis_cas = pd.DataFrame(equis_casrn_map.items(), columns=['cas_rn', 'constituent'])
    con.execute("CREATE TABLE mappings.equis_casrn AS SELECT * FROM df_equis_cas")

    # Load station cross-reference from CSV
    # Assumes this script is run from a location where this relative path is valid
    xref_csv_path = Path(__file__).parent / 'data/WISKI_EQUIS_XREF.csv'
    if xref_csv_path.exists():
        con.execute(f"CREATE TABLE mappings.station_xref AS SELECT * FROM read_csv_auto('{xref_csv_path.as_posix()}')")
    else:
        print(f"Warning: WISKI_EQUIS_XREF.csv not found at {xref_csv_path}")

    # Load wiski_quality_codes from CSV
    wiski_qc_csv_path = Path(__file__).parent / 'data/WISKI_QUALITY_CODES.csv'
    if wiski_qc_csv_path.exists():
        con.execute(f"CREATE TABLE mappings.wiski_quality_codes AS SELECT * FROM read_csv_auto('{wiski_qc_csv_path.as_posix()}')")
    else:
            print(f"Warning: WISKI_QUALITY_CODES.csv not found at {wiski_qc_csv_path}")

def create_outlets_tables(con: duckdb.DuckDBPyConnection):
    """
    Create tables in the outlets schema to define outlet-station-reach relationships.
    """
    con.execute("""-- schema.sql
            -- Simple 3-table design to manage associations between model reaches and observation stations via outlets.
            -- Compatible with DuckDB and SQLite.

            -- Table 1: outlets
            -- Represents a logical grouping that ties stations and reaches together.
            CREATE TABLE IF NOT EXISTS outlets.outlets (
            outlet_id TEXT PRIMARY KEY,
            repository_name TEXT NOT NULL,
            outlet_name TEXT,
            notes TEXT             -- optional: general notes about the outlet grouping
            );

            -- Table 2: outlet_stations
            -- One-to-many: outlet -> stations
            CREATE TABLE IF NOT EXISTS outlets.outlet_stations (
            outlet_id TEXT NOT NULL,
            station_id TEXT NOT NULL,
            station_origin TEXT NOT NULL,       -- e.g., 'wiski', 'equis'
            repository_name TEXT NOT NULL,  -- repository model the station is physically located in
            true_opnid TEXT NOT NULL,           -- The specific reach the station physically sits on (optional)
            comments TEXT,             -- Per-station comments, issues, etc.
            CONSTRAINT uq_station_origin UNIQUE (station_id, station_origin),
            FOREIGN KEY (outlet_id) REFERENCES outlets.outlets(outlet_id)
            );

            -- Table 3: outlet_reaches
            -- One-to-many: outlet -> reaches
            -- A reach can appear in multiple outlets, enabling many-to-many overall.
            CREATE TABLE IF NOT EXISTS outlets.outlet_reaches (
            outlet_id TEXT NOT NULL,
            reach_id TEXT NOT NULL,    -- model reach identifier (aka opind)
            repository_name TEXT NOT NULL,  -- optional: where the mapping comes from
            exclude INTEGER DEFAULT 0, -- flag to indicate if this reach should be excluded (1) or included (0)
            FOREIGN KEY (outlet_id) REFERENCES outlets.outlets(outlet_id)
            );

            -- Useful views:

            -- View: station_reach_pairs
            -- Derives the implicit many-to-many station <-> reach relationship via shared outlet_id
            CREATE VIEW IF NOT EXISTS outlets.station_reach_pairs AS
            SELECT
            s.outlet_id,
            s.station_id,
            s.station_origin,
            r.reach_id,
            r.exclude,
            r.repository_name,
            FROM outlets.outlet_stations s
            JOIN outlets.outlet_reaches r
            ON s.outlet_id = r.outlet_id;

          """)

def create_normalized_wiski_view(con: duckdb.DuckDBPyConnection):
    """
    Create a view in the database that contains normalized WISKI data.
    Units converted to standard units.
    columns renamed.
    constituents mapped.
    """
    con.execute("""
    -- Create a single view with all transformations
    CREATE OR REPLACE VIEW analytics.wiski_normalized AS
    SELECT 
        
        -- Convert °C to °F and keep other values unchanged
        CASE 
            WHEN LOWER(ts_unitsymbol) = '°c' THEN (value * 9.0 / 5.0) + 32
            WHEN ts_unitsymbol = 'kg' THEN value * 2.20462    -- Convert kg to lb
            ELSE value
        END AS value,

        -- Normalize units
        CASE 
            WHEN LOWER(ts_unitsymbol) = '°c' THEN 'degf'      -- Normalize °C to degF
            WHEN ts_unitsymbol = 'kg' THEN 'lb'              -- Normalize kg to lb
            WHEN ts_unitsymbol = 'ft³/s' THEN 'cfs'          -- Rename ft³/s to cfs
            ELSE ts_unitsymbol
        END AS unit,

        -- Normalize column names
        station_no AS station_id,                             -- Rename station_no to station_id
        Timestamp AS datetime,                                -- Rename Timestamp to datetime
        "Quality Code" AS quality_code,                      -- Rename Quality Code to quality_code
        "Quality Code Name" AS quality_code_name,            -- Rename Quality Code Name to quality_code_name
        parametertype_id,                                    -- Keeps parametertype_id as is
        constituent                                          -- Keeps constituent as is
    FROM staging.wiski_raw;""")


def create_filtered_wiski_view(con: duckdb.DuckDBPyConnection, data_codes: list):
    """
    Create a view in the database that filters WISKI data based on specified data codes.
    """
    query = f"""
    CREATE OR REPLACE VIEW analytics.wiski_filtered AS
    SELECT *
    FROM analytics.wiski_normalized
    WHERE quality_code IN ({placeholders});
    """

    placeholders = ', '.join(['?'] * len(data_codes))
    query = query.format(placeholders=placeholders)
    con.execute(query, data_codes)


def create_aggregated_wiski_view(con: duckdb.DuckDBPyConnection):
    """
    Create a view in the database that aggregates WISKI data by hour, station, and constituent.
    """
    con.execute("""
    CREATE OR REPLACE Table analytics.wiski_aggregated AS
    SELECT 
        station_id,
        constituent,
        time_bucket(INTERVAL '1 hour', datetime) AS hour_start,
        AVG(value) AS value,
        unit
    FROM analytics.wiski_normalized
    GROUP BY 
        station_id, 
        constituent, 
        hour_start,
        unit;
    """)

def create_staging_qc_count_view(con: duckdb.DuckDBPyConnection):
    """
    Create a view in staging schema that counts quality codes for each station and constituent.
    """
    con.execute("""
    CREATE OR REPLACE VIEW staging.wiski_qc_count AS (
        SELECT 
            w.station_no,
            w.parametertype_name,
            w."Quality Code",
            w."Quality Code Name",
            COUNT(w."Quality Code") AS count
        FROM staging.wiski w 
        GROUP BY
            w."Quality Code",w."Quality Code Name",w.parametertype_name, w.station_no
                );
        """)
    # ORDER BY
    #         w.station_no,w.parametertype_name, w."Quality Code"
    #         )
    # """)

def create_combined_observations_view(con: duckdb.DuckDBPyConnection):
    """
    Create a view in analytics schema that combines observations from equis and wiski processed tables.
    """
    con.execute("""
    CREATE OR REPLACE VIEW analytics.observations AS
    SELECT datetime,value,station_id,station_origin,constituent,unit
    FROM analytics.equis
    UNION ALL
    SELECT datetime,value,station_id,station_origin,constituent,unit
    FROM analytics.wiski;
    """)


def create_outlet_observations_view(con: duckdb.DuckDBPyConnection):
    """
    Create a view in analytics schema that links observations to model reaches via outlets.
    """
    con.execute("""
    CREATE VIEW analytics.outlet_observations AS 
    SELECT
        o.datetime,
        os.outlet_id,
        o.constituent,
        AVG(o.value) AS value,
        COUNT(o.value) AS count
    FROM
        analytics.observations AS o
    LEFT JOIN
        outlets.outlet_stations AS os ON o.station_id = os.station_id AND o.station_origin = os.station_origin
    GROUP BY
        os.outlet_id,
        o.constituent,
        o.datetime; -- Group by the truncated date
    """)
    # ORDER BY
    #     os.outlet_id,
    #     o.constituent,
    #     datetime);



def create_outlet_observations_with_flow_view(con: duckdb.DuckDBPyConnection):
    
    con.execute("""
    CREATE OR REPLACE VIEW analytics.outlet_observations_with_flow AS 
        WITH baseflow_data AS (
            SELECT
                outlet_id,
                datetime,
                "value" AS baseflow_value
            FROM
                analytics.outlet_observations
            WHERE
                (constituent = 'QB')),
        flow_data AS (
            SELECT
                outlet_id,
                datetime,
                "value" AS flow_value
            FROM
                analytics.outlet_observations
            WHERE
                (constituent = 'Q')),
        constituent_data AS (
            SELECT
                outlet_id,
                datetime,
                constituent,
                "value",
                count
            FROM
                analytics.outlet_observations
            WHERE
                (constituent NOT IN ('Q', 'QB')))
        SELECT
            constituent_data.outlet_id,
            constituent_data.constituent,
            constituent_data.datetime,
            constituent_data."value",
            flow_data.flow_value,
            baseflow_data.baseflow_value
        FROM
            constituent_data
        FULL JOIN flow_data ON
            (((constituent_data.outlet_id = flow_data.outlet_id)
                AND (constituent_data.datetime = flow_data.datetime)))
        LEFT JOIN baseflow_data ON
            (((constituent_data.outlet_id = baseflow_data.outlet_id)
                AND (constituent_data.datetime = baseflow_data.datetime)));""")
    # ORDER BY
    #     constituent_data.outlet_id,
    #     constituent_data.datetime;
    # 

def create_constituent_summary_report(con: duckdb.DuckDBPyConnection):
    """
    Create a constituent summary report in the reports schema that groups observations by constituent and station.
    """
    con.execute('''
            CREATE OR REPLACE VIEW reports.constituent_summary AS
            SELECT
            station_id,
            station_origin,
            constituent,
            COUNT(*) AS sample_count,
            AVG(value) AS average_value,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            year(MIN(datetime)) AS start_date,
            year(MAX(datetime)) AS end_date
            FROM
            analytics.observations
            GROUP BY
            constituent,station_id,station_origin;
            ''')
                
            # ORDER BY
            # constituent,sample_count;''')


def drop_station_id(con: duckdb.DuckDBPyConnection, station_id: str,station_origin: str):
    """
    Drop all data for a specific station from staging and analytics schemas.
    """
    con.execute(f"DELETE FROM staging.equis_raw WHERE station_id = '{station_id}' AND station_origin = '{station_origin}'")
    con.execute(f"DELETE FROM staging.wiski_raw WHERE station_id = '{station_id}' AND station_origin = '{station_origin}'")
    con.execute(f"DELETE FROM analytics.equis WHERE station_id = '{station_id}' AND station_origin = '{station_origin}'")
    con.execute(f"DELETE FROM analytics.wiski WHERE station_id = '{station_id}' AND station_origin = '{station_origin}'")
    update_views(con)

def update_views(con: duckdb.DuckDBPyConnection):
    """
    Update all views in the database.
    """
    create_combined_observations_view(con)
    create_constituent_summary_report(con)
    create_outlet_observations_view(con)
    create_outlet_observations_with_flow_view(con)

def connect(db_path: str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Returns a DuckDB connection to the given database path.
    Ensures the parent directory exists.
    """
    db_path = Path(db_path)
    parent = db_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(database=db_path.as_posix(), read_only=read_only)


def load_df_to_table(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str, replace: bool = True):
    """
    Persist a pandas DataFrame into a DuckDB table. This will overwrite the table
    by default (replace=True).
    """
    if replace:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
    # register pandas DF and create table
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_df")
    con.unregister("tmp_df")

def load_df_to_staging(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str, replace: bool = True):
    """
    Persist a pandas DataFrame into a staging table. This will overwrite the staging
    table by default (replace=True).
    """
    if replace:
        con.execute(f"DROP TABLE IF EXISTS staging.{table_name}")
    # register pandas DF and create table
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE staging.{table_name} AS SELECT * FROM tmp_df")
    con.unregister("tmp_df")

def add_df_to_staging(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str):
    """
    Append a pandas DataFrame into a staging table. This will create the staging
    table if it does not exist.
    """
    # register pandas DF and create table if not exists
    con.register("tmp_df", df)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS staging.{table_name} AS 
        SELECT * FROM tmp_df
    """)
    con.execute(f"""
        INSERT INTO staging.{table_name} 
        SELECT * FROM tmp_df
    """)
    con.unregister("tmp_df")

def load_csv_to_staging(con: duckdb.DuckDBPyConnection, csv_path: str, table_name: str, replace: bool = True, **read_csv_kwargs):
    """
    Persist a CSV file into a staging table. This will overwrite the staging
    table by default (replace=True).
    """
    if replace:
        con.execute(f"DROP TABLE IF EXISTS staging.{table_name}")
    con.execute(f"""
        CREATE TABLE staging.{table_name} AS 
        SELECT * FROM read_csv_auto('{csv_path}', {', '.join(f"{k}={repr(v)}" for k, v in read_csv_kwargs.items())})
    """)

def load_parquet_to_staging(con: duckdb.DuckDBPyConnection, parquet_path: str, table_name: str, replace: bool = True):
    """
    Persist a Parquet file into a staging table. This will overwrite the staging
    table by default (replace=True).
    """
    if replace:
        con.execute(f"DROP TABLE IF EXISTS staging.{table_name}")
    con.execute(f"""
        CREATE TABLE staging.{table_name} AS 
        SELECT * FROM read_parquet('{parquet_path}')
    """)


def write_table_to_parquet(con: duckdb.DuckDBPyConnection, table_name: str, path: str, compression="snappy"):
    """
    Persist a DuckDB table into a Parquet file.
    """
    con.execute(f"COPY (SELECT * FROM {table_name}) TO '{path}' (FORMAT PARQUET, COMPRESSION '{compression}')")


def write_table_to_csv(con: duckdb.DuckDBPyConnection, table_name: str, path: str, header: bool = True, sep: str = ',', **kwargs):
    """
    Persist a DuckDB table into a CSV file.
    """
    con.execute(f"COPY (SELECT * FROM {table_name}) TO '{path}' (FORMAT CSV, HEADER {str(header).upper()}, DELIMITER '{sep}' {', '.join(f', {k}={repr(v)}' for k, v in kwargs.items())})")




def load_df_to_analytics(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str):
    """
    Persist a pandas DataFrame into an analytics table.
    """
    con.execute(f"DROP TABLE IF EXISTS analytics.{table_name}")
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE analytics.{table_name} AS SELECT * FROM tmp_df")
    con.unregister("tmp_df")


def migrate_staging_to_analytics(con: duckdb.DuckDBPyConnection, staging_table: str, analytics_table: str):
    """
    Migrate data from a staging table to an analytics table.
    """
    con.execute(f"DROP TABLE IF EXISTS analytics.{analytics_table}")
    con.execute(f"""
        CREATE TABLE analytics.{analytics_table} AS 
        SELECT * FROM staging.{staging_table}
    """)


def load_to_analytics(con: duckdb.DuckDBPyConnection, table_name: str):
    con.execute(f"""
                CREATE OR REPLACE TABLE analytics.{table_name} AS
                SELECT
                station_id,
                constituent,
                datetime,
                value AS observed_value,
                time_bucket(INTERVAL '1 hour', datetime) AS hour_start,
                AVG(observed_value) AS value
                FROM
                    staging.equis_processed
                GROUP BY
                    hour_start,
                    constituent,
                    station_id
                ORDER BY
                    station_id,
                    constituent
                """)
    # register pandas DF and create table
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE analytics.{table_name} AS SELECT * FROM tmp_df")
    con.unregister("tmp_df")

def dataframe_to_parquet(con: duckdb.DuckDBPyConnection,  df: pd.DataFrame, path, compression="snappy"):
    # path should be a filename like 'data/raw/equis/equis-20251118.parquet'
    con = duckdb.connect()
    con.register("tmp_df", df)
    con.execute(f"COPY (SELECT * FROM tmp_df) TO '{path}' (FORMAT PARQUET, COMPRESSION '{compression}')")
    con.unregister("tmp_df")
    con.close()