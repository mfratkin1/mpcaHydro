from typing import List

import duckdb
import pandas as pd
from pathlib import Path
from mpcaHydro import outlets
from mpcaHydro import sql_loader

def init_db(db_path: str,reset: bool = False):
    """Initialize the DuckDB database: create schemas and tables."""
    db_path = Path(db_path)
    if reset and db_path.exists():
        db_path.unlink()

    with connect(db_path.as_posix()) as con:
        # Create all schemas
        create_schemas(con)

        # Create tables
        create_outlets_tables(con)
        create_mapping_tables(con)
        create_staging_tables(con)
        create_analytics_tables(con)
        

        # Create views
        update_views(con)



def create_schemas(con: duckdb.DuckDBPyConnection):
    """Create staging, analytics, hspf, and reports schemas if they do not exist."""
    con.execute(sql_loader.get_schemas_sql())

def create_staging_tables(con: duckdb.DuckDBPyConnection):
    """Create necessary tables in the staging schema."""
    con.execute(sql_loader.get_staging_tables_sql())


def create_analytics_tables(con: duckdb.DuckDBPyConnection):
    """Create necessary tables in the analytics schema."""
    con.execute(sql_loader.get_analytics_tables_sql())

def create_mapping_tables(con: duckdb.DuckDBPyConnection):
    """Create and populate tables in the mappings schema from Python dicts and CSVs."""
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
    con.execute("CREATE TABLE IF NOT EXISTS mappings.wiski_parametertype AS SELECT * FROM df_wiski_params")

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
    con.execute("CREATE TABLE IF NOT EXISTS mappings.equis_casrn AS SELECT * FROM df_equis_cas")

    # Load station cross-reference from CSV
    xref_csv_path = Path(__file__).parent / 'data/WISKI_EQUIS_XREF.csv'
    if xref_csv_path.exists():
        con.execute(f"CREATE TABLE IF NOT EXISTS mappings.station_xref AS SELECT * FROM read_csv_auto('{xref_csv_path.as_posix()}')")
    else:
        print(f"Warning: WISKI_EQUIS_XREF.csv not found at {xref_csv_path}")

    # Load wiski_quality_codes from CSV
    wiski_qc_csv_path = Path(__file__).parent / 'data/WISKI_QUALITY_CODES.csv'
    if wiski_qc_csv_path.exists():
        con.execute(f"CREATE TABLE IF NOT EXISTS mappings.wiski_quality_codes AS SELECT * FROM read_csv_auto('{wiski_qc_csv_path.as_posix()}')")
    else:
        print(f"Warning: WISKI_QUALITY_CODES.csv not found at {wiski_qc_csv_path}")

def create_outlets_tables(con: duckdb.DuckDBPyConnection):
    """Create tables in the outlets schema to define outlet-station-reach relationships."""
    con.execute(sql_loader.get_outlets_schema_sql())
    con.execute(sql_loader.get_views_outlets_sql())
    outlets.build_outlets(con)


def create_filtered_wiski_view(con: duckdb.DuckDBPyConnection, data_codes: list):
    """Create a view filtering WISKI data based on specified data codes."""
    placeholders = ', '.join(['?'] * len(data_codes))
    query = f"""
    CREATE OR REPLACE VIEW analytics.wiski_filtered AS
    SELECT *
    FROM analytics.wiski_normalized
    WHERE quality_code IN ({placeholders});
    """
    con.execute(query, data_codes)


def create_aggregated_wiski_view(con: duckdb.DuckDBPyConnection):
    """Create a view aggregating WISKI data by hour, station, and constituent."""
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


def update_views(con: duckdb.DuckDBPyConnection):
    """Update all views in the database by loading from SQL files."""
    con.execute(sql_loader.get_views_analytics_sql())
    con.execute(sql_loader.get_views_reports_sql())
    
def connect(db_path: str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Returns a DuckDB connection to the given database path.
    Ensures the parent directory exists.
    """
    db_path = Path(db_path)
    parent = db_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(database=db_path.as_posix(), read_only=read_only)


def drop_station_data(con: duckdb.DuckDBPyConnection, station_ids: List[str], station_origin: str):
    """
    Drop all data for a specific stations from staging and analytics schemas.
    """
    placeholders = ', '.join(['?'] * len(station_ids))
    con.execute(f"DELETE FROM staging.equis WHERE station_id IN ({placeholders}) AND station_origin = ?", station_ids + [station_origin])
    con.execute(f"DELETE FROM staging.wiski WHERE station_id IN ({placeholders}) AND station_origin = ?", station_ids + [station_origin])
    con.execute(f"DELETE FROM analytics.equis WHERE station_id IN ({placeholders}) AND station_origin = ?", station_ids + [station_origin])
    con.execute(f"DELETE FROM analytics.wiski WHERE station_id IN ({placeholders}) AND station_origin = ?", station_ids + [station_origin])
    update_views(con)

def get_column_names(con: duckdb.DuckDBPyConnection, table_schema: str, table_name: str) -> list:
    """
    Get the column names of a DuckDB table.
    """
    #table_schema, table_name = table_name.split('.')
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = ? AND table_schema = ?
    """
    result = con.execute(query,[table_name,table_schema]).fetchall()
    column_names = [row[0] for row in result]
    return column_names


def add_df_to_table(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_schema: str, table_name: str):
    """
    Append a pandas DataFrame into a DuckDB table. This will create the table
    if it does not exist.
    """
    # get existing columns
    existing_columns = get_column_names(con, table_schema, table_name)
    df = df[existing_columns]


    # register pandas DF and create table if not exists
    con.register("tmp_df", df)

    con.execute(f"""
        INSERT INTO {table_schema}.{table_name} 
        SELECT * FROM tmp_df
    """)
    con.unregister("tmp_df")

def load_df_to_table(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str):
    """
    Persist a pandas DataFrame into a DuckDB table. This will overwrite the table
    by default (replace=True).
    """
    # register pandas DF and create table
    con.register("tmp_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df")
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