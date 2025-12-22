import duckdb
import pandas as pd
from pathlib import Path

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

        # Create tables for observational data
        # Wrapped in try/except as they depend on tables that may not exist yet
        try:
            create_combined_observations_view(con)
            create_constituent_summary_report(con)
        except duckdb.CatalogException as e:
            print(f"Could not create observation views, likely because backing tables don't exist yet. This is safe to ignore on first run. Details: {e}")


def create_schemas(con: duckdb.DuckDBPyConnection):
    """
    Create staging, analytics, hspf, and reports schemas if they do not exist.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    con.execute("CREATE SCHEMA IF NOT EXISTS reports")
    con.execute("CREATE SCHEMA IF NOT EXISTS hspf")


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
            constituent,station_id,station_origin
            ORDER BY
            constituent,sample_count;''')


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