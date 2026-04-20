"""
warehouse
=========

DuckDB data warehouse for storing, transforming, and querying hydrological
observation data used in HSPF model calibration.

Overview
--------
The warehouse module manages a local DuckDB database that consolidates
data from multiple sources (WISKI, EQuIS) into a unified, query-ready
analytical store.  It handles database lifecycle operations — schema
creation, table creation, data loading, view management — so that
upstream ETL code (:mod:`wiski`, :mod:`equis`) and downstream query code
(:class:`~mpcaHydro.warehouse_functions.DataManagerWrapper`) do not need
to know the underlying SQL.

Database schema
---------------
The database is organised into **five schemas**, each serving a distinct
role in the data pipeline:

**staging**
    Raw data as received from external systems, with minimal
    transformation.  Tables mirror the source column layout.

    ``staging.equis``
        Raw EQuIS result rows (70+ columns) exactly as returned by the
        Oracle query in :func:`equis.download`.  Key columns include
        ``SYS_LOC_CODE``, ``CAS_RN``, ``RESULT_NUMERIC``,
        ``RESULT_UNIT``, ``SAMPLE_DATE_TIME``, and
        ``SAMPLE_DATE_TIMEZONE``.

    ``staging.wiski``
        Raw WISKI time-series rows including ``Timestamp``, ``Value``,
        ``Quality Code``, ``station_no``, ``parametertype_id``,
        ``stationparameter_no``, ``ts_unitsymbol``, and the
        ``wplmn_flag``.

**analytics**
    Cleaned, standardised, and aggregated data ready for analysis.
    All tables share a common schema:
    ``(datetime, value, station_id, station_origin, constituent, unit)``.

    ``analytics.equis``
        Transformed EQuIS data (hourly averages, standard units, mapped
        constituents) produced by :func:`equis.transform`.

    ``analytics.wiski``
        Transformed WISKI data (quality-filtered, hourly averages,
        standard units, baseflow) produced by :func:`wiski.transform`.

    ``analytics.observations`` *(view)*
        Union of ``analytics.equis`` and ``analytics.wiski``, providing
        a single virtual table of all observations regardless of source.

    ``analytics.outlet_observations`` *(view)*
        Observations aggregated by outlet, joining observations to
        ``outlets.outlet_stations`` so that data is grouped by outlet
        rather than by individual station.

    ``analytics.outlet_observations_with_flow`` *(view)*
        Extends ``outlet_observations`` by left-joining discharge (``Q``)
        and baseflow (``QB``) data alongside each constituent
        observation.  This is the primary view used for HSPF calibration
        comparisons.

**outlets**
    Defines the mapping between monitoring stations and model reaches.
    See :mod:`outlets` for full documentation.

    ``outlets.outlet_groups``
        One row per outlet: ``(outlet_id, repository_name, outlet_name,
        notes)``.

    ``outlets.outlet_stations``
        Station membership: ``(outlet_id, station_id, station_origin,
        repository_name, true_opnid, comments)``.  The
        ``(station_id, station_origin)`` pair is unique across all
        outlets.

    ``outlets.outlet_reaches``
        Reach membership: ``(outlet_id, reach_id, repository_name)``.
        A reach may appear in multiple outlets.

    ``outlets.station_reach_pairs`` *(view)*
        Convenience view that joins ``outlet_stations`` to
        ``outlet_reaches`` on ``outlet_id``, deriving the implicit
        many-to-many station ↔ reach relationship.

**mappings**
    Lookup tables used during ETL transformations.

    ``mappings.wiski_parametertype``
        Maps WISKI ``parametertype_id`` to constituent abbreviations.

    ``mappings.equis_casrn``
        Maps EQuIS CAS registry numbers to constituent abbreviations.

    ``mappings.station_xref``
        Cross-reference between WISKI and EQuIS station identifiers
        (loaded from ``data/WISKI_EQUIS_XREF.csv``).

    ``mappings.wiski_quality_codes``
        WISKI quality-code definitions (loaded from
        ``data/WISKI_QUALITY_CODES.csv``).

**reports**
    Pre-built summary views for quick reporting.

    ``reports.wiski_qc_count`` *(view)*
        Quality-code frequency counts per WISKI station and parameter,
        joined to quality-code descriptions.

    ``reports.constituent_summary`` *(view)*
        Per-station, per-constituent summary statistics (count, mean,
        min, max, date range) across all analytics observations.

    ``reports.outlet_constituent_summary`` *(view)*
        Same statistics aggregated by outlet instead of station.

Typical workflow
----------------
::

    from mpcaHydro import warehouse

    # 1. Create a fresh database
    warehouse.init_db('observations.duckdb', reset=True)

    # 2. Open a connection and load data
    with warehouse.connect('observations.duckdb') as con:
        warehouse.add_df_to_table(con, df_wiski, 'staging', 'wiski')
        warehouse.add_df_to_table(con, df_equis, 'staging', 'equis')
        warehouse.update_views(con)

SQL files
---------
All DDL statements live in the ``sql/`` folder and are loaded at runtime
by :mod:`sql_loader`:

* ``schemas.sql`` — ``CREATE SCHEMA`` for all five schemas.
* ``staging_tables.sql`` — ``CREATE TABLE`` for staging tables.
* ``analytics_tables.sql`` — ``CREATE TABLE`` for analytics tables.
* ``outlets_schema.sql`` — outlets schema, tables, and the
  ``station_reach_pairs`` view.
* ``views_analytics.sql`` — analytics views (observations,
  outlet_observations, outlet_observations_with_flow).
* ``views_reports.sql`` — report views.
* ``views_outlets.sql`` — outlets views.
"""

from typing import List

import duckdb
import pandas as pd
from pathlib import Path
from mpcaHydro import outlets
from mpcaHydro import sql_loader


def create_session(
    data_dir: str = "data",
    wiski_quality_codes: List[int] = None,
    min_year: int = 1996,
) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB session backed by parquet staging files.

    This is the primary entry point for the parquet-based workflow.  The
    returned connection is fully configured: schemas, mapping tables, outlet
    tables, and all analytics/report views are created.  Staging is backed
    by ``read_parquet`` views over ``data_dir/staging/{source}/*.parquet``
    when files are present, or by empty schema-compatible tables otherwise.

    Parameters
    ----------
    data_dir : str, default ``"data"``
        Root directory that contains the ``staging/`` sub-directories for
        each source.
    wiski_quality_codes : list of int, optional
        Quality codes to mark as *active* in ``mappings.wiski_quality_codes``.
        When ``None`` the defaults from ``WISKI_QUALITY_CODES.csv`` (the
        ``Active`` column) are used unchanged.
    min_year : int, default 1996
        Earliest year to retain.  Stored as the session variable
        ``min_year`` so SQL views can reference ``getvariable('min_year')``.

    Returns
    -------
    duckdb.DuckDBPyConnection
        Open in-memory connection.  The caller is responsible for closing it.
    """
    con = duckdb.connect()

    # Set session variable before any views are created so filters work.
    con.execute(f"SET VARIABLE min_year = {min_year}")

    create_schemas(con)
    create_outlets_tables(con)
    create_mapping_tables(con)

    # Override active quality codes when the caller specifies them explicitly.
    if wiski_quality_codes is not None:
        set_active_quality_codes(con, wiski_quality_codes)

    # Create empty staging tables first — guarantees the names exist even when
    # no parquet files have been downloaded yet.
    create_staging_tables(con)

    # Replace empty tables with parquet-backed views where files exist.
    refresh_staging_views(con, data_dir)

    # Build analytics + report views on top of whatever staging provides.
    update_views(con)

    return con


def refresh_staging_views(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """Rebind staging tables to parquet views where files are present.

    Should be called after downloading new data in a session so that the
    connection immediately reflects the new files without being restarted.
    If staging is already backed by a parquet view (or no files exist),
    this function is a no-op for that source.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection created by :func:`create_session`.
    data_dir : str
        Root data directory (same value used in :func:`create_session`).
    """
    data_dir = Path(data_dir)
    for source in ("wiski", "equis"):
        source_path = data_dir / "staging" / source
        if next(source_path.glob("*.parquet"), None) is None:
            continue  # no files yet — keep empty table

        # Check whether staging.{source} is currently a base table.
        row = con.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema = 'staging' AND table_name = ?",
            [source],
        ).fetchone()

        if row is not None and row[0] == "BASE TABLE":
            con.execute(f"DROP TABLE staging.{source}")
            con.execute(f"""
                CREATE VIEW staging.{source} AS
                SELECT * FROM read_parquet(
                    '{source_path.as_posix()}/*.parquet',
                    union_by_name = true
                )
            """)

def init_db(db_path: str, reset: bool = False):
    """Initialise the DuckDB warehouse database.

    Creates all schemas (staging, analytics, reports, outlets, mappings),
    tables, mapping data, outlet data, and views.  This is the primary
    entry point for standing up a new warehouse.

    Parameters
    ----------
    db_path : str
        Filesystem path for the DuckDB file.
    reset : bool, default False
        If ``True``, delete the existing file before creating a fresh
        database.
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
        create_staging_tables(con)
        create_analytics_tables(con)
        

        # Create views
        update_views(con)



def validate_schemas(con: duckdb.DuckDBPyConnection):
    """Validate that the database contains all expected schemas.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Raises
    ------
    ValueError
        If any of the expected schemas (staging, analytics, mappings,
        outlets, reports) are missing.
    """
    expected_schemas = {'staging', 'analytics', 'mappings', 'outlets', 'reports'}
    result = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    existing_schemas = {row[0] for row in result}
    missing_schemas = expected_schemas - existing_schemas
    if missing_schemas:
        raise ValueError(f"Missing schemas: {missing_schemas}")

def validate_tables(con: duckdb.DuckDBPyConnection, schema: str, expected_tables: set):
    """Validate that a schema contains the expected tables.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    schema : str
        Schema name to inspect.
    expected_tables : set of str
        Table names that must exist.

    Raises
    ------
    ValueError
        If any expected tables are missing.
    """
    result = con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [schema]).fetchall()
    existing_tables = {row[0] for row in result}
    missing_tables = expected_tables - existing_tables
    if missing_tables:
        raise ValueError(f"Missing tables in {schema} schema: {missing_tables}")

def create_schemas(con: duckdb.DuckDBPyConnection):
    """Create all warehouse schemas (staging, analytics, reports, outlets, mappings).

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    """
    con.execute(sql_loader.get_schemas_sql())

def create_staging_tables(con: duckdb.DuckDBPyConnection):
    """Create ``staging.equis`` and ``staging.wiski`` tables.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    """
    con.execute(sql_loader.get_staging_tables_sql())


def create_analytics_tables(con: duckdb.DuckDBPyConnection):
    """Create ``analytics.equis`` and ``analytics.wiski`` tables.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    """
    con.execute(sql_loader.get_analytics_tables_sql())

def create_mapping_tables(con: duckdb.DuckDBPyConnection):
    """Create and populate lookup tables in the ``mappings`` schema.

    Populates the following from Python dictionaries and CSV files:

    * ``mappings.wiski_parametertype`` — WISKI parameter-type ID →
      constituent mapping.
    * ``mappings.equis_casrn`` — CAS registry number → constituent
      mapping.
    * ``mappings.station_xref`` — WISKI/EQuIS station cross-reference
      (from ``data/WISKI_EQUIS_XREF.csv``).
    * ``mappings.wiski_quality_codes`` — quality-code descriptions
      (from ``data/WISKI_QUALITY_CODES.csv``).

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
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


def set_active_quality_codes(con, quality_codes: list):
    """Update which quality codes are active in the current session.
    
    Since analytics.wiski is a VIEW, the next query against it
    will automatically reflect the change. No reprocessing needed.
    """
    con.execute("UPDATE mappings.wiski_quality_codes SET active = 0")
    if quality_codes:
        placeholders = ', '.join(['?'] * len(quality_codes))
        con.execute(
            f"UPDATE mappings.wiski_quality_codes SET active = 1 WHERE quality_code IN ({placeholders})",
            quality_codes
        )

def attach_outlets_db(con: duckdb.DuckDBPyConnection, outlets_db_path: str):
    """Attach and copy tables/views from an external outlet DuckDB file.

    This is used to import a pre-built outlet database into the current
    warehouse connection.  All tables and views from the source database
    are copied into the current connection's default catalog.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection for the warehouse.
    outlets_db_path : str
        Path to the external DuckDB file containing outlet definitions.
    """
    create_schemas(con)

    con.execute(f"ATTACH DATABASE '{outlets_db_path}' AS outlets_db;")

    tables = con.execute("SHOW TABLES FROM outlets_db").fetchall()
    print(f"Tables in the source database: {tables}")

    for table in tables:
        table_name = table[0]  # Extract table name
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM outlets_db.{table_name}")  # Copy table contents

    # -- Step 2: Copy all views --
    # Retrieve the list of views in the source database
    views = con.execute("SHOW VIEWS FROM outlets_db").fetchall()
    print(f"Views in the source database: {views}")

    # Copy each view from source to destination
    for view in views:
        view_name = view[0]  # Extract view name

        # Get the CREATE VIEW statement for the view
        create_view_sql = con.execute(f"SHOW CREATE VIEW outlets_db.{view_name}").fetchone()[0]
        
        # Recreate the view in the destination database (remove the `outlets_db.` prefix if exists)
        create_view_sql = create_view_sql.replace(f"outlets_db.", "")
        con.execute(create_view_sql)

    # Detach the source database
    con.execute("DETACH 'outlets_db'")


def create_outlets_tables(con: duckdb.DuckDBPyConnection, model_name: str = None):
    """Create outlet tables, views, and populate them from :data:`outlets.MODL_DB`.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    model_name : str, optional
        Restrict outlet population to a single model.  If ``None``, all
        models are populated.
    """
    con.execute(sql_loader.get_outlets_schema_sql())
    con.execute(sql_loader.get_views_outlets_sql())
    outlets.build_outlets(con, model_name=model_name)


def update_views(con: duckdb.DuckDBPyConnection):
    """Refresh all analytics and reports views from their SQL definitions.

    Call this after loading or modifying data in the staging or analytics
    tables to ensure that dependent views reflect the latest data.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    """
    con.execute(sql_loader.get_transforms_wiski_sql())
    con.execute(sql_loader.get_transforms_equis_sql())
    con.execute(sql_loader.get_views_analytics_sql())
    con.execute(sql_loader.get_views_reports_sql())
    
def connect(db_path: str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, creating the parent directory if needed.

    Parameters
    ----------
    db_path : str
        Path to the DuckDB database file.
    read_only : bool, default False
        Open in read-only mode when ``True``.

    Returns
    -------
    duckdb.DuckDBPyConnection
    """
    db_path = Path(db_path)
    parent = db_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(database=db_path.as_posix(), read_only=read_only)


def drop_station_data(con, station_ids, station_origin):
    placeholders = ', '.join(['?'] * len(station_ids))

    # Staging tables use source-native column names
    if station_origin == 'wiski':
        con.execute(
            f"DELETE FROM staging.wiski WHERE station_no IN ({placeholders})",
            station_ids,
        )
    elif station_origin == 'equis':
        con.execute(
            f"DELETE FROM staging.equis WHERE SYS_LOC_CODE IN ({placeholders})",
            station_ids,
        )

    # Analytics tables use the unified schema
    con.execute(
        f"DELETE FROM analytics.equis WHERE station_id IN ({placeholders}) AND station_origin = ?",
        station_ids + [station_origin],
    )
    con.execute(
        f"DELETE FROM analytics.wiski WHERE station_id IN ({placeholders}) AND station_origin = ?",
        station_ids + [station_origin],
    )
    update_views(con)

def get_column_names(con: duckdb.DuckDBPyConnection, table_schema: str, table_name: str) -> list:
    """Return the column names of a table.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    table_schema : str
        Schema containing the table.
    table_name : str
        Name of the table.

    Returns
    -------
    list of str
        Column names in ordinal position order.
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
    """Append rows from a DataFrame into an existing DuckDB table.

    The DataFrame columns are reordered to match the target table's
    schema before insertion.  This is the standard method for
    incrementally loading data (e.g. new station downloads) into
    staging or analytics tables.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    df : pandas.DataFrame
        Data to insert.
    table_schema : str
        Target schema (e.g. ``'staging'``).
    table_name : str
        Target table (e.g. ``'wiski'``).
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
    """Replace a DuckDB table with the contents of a DataFrame.

    Creates or replaces the table using ``CREATE OR REPLACE TABLE``.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    df : pandas.DataFrame
        Data to persist.
    table_name : str
        Fully-qualified table name (e.g. ``'analytics.wiski'``).
    """
    # register pandas DF and create table
    con.register("tmp_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df")
    con.unregister("tmp_df")

def load_df_to_staging(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str, replace: bool = True):
    """Load a DataFrame into a ``staging`` table.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    df : pandas.DataFrame
        Data to persist.
    table_name : str
        Table name (without schema prefix).
    replace : bool, default True
        Drop and recreate the table if it already exists.
    """
    if replace:
        con.execute(f"DROP TABLE IF EXISTS staging.{table_name}")
    # register pandas DF and create table
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE staging.{table_name} AS SELECT * FROM tmp_df")
    con.unregister("tmp_df")

def load_csv_to_staging(con: duckdb.DuckDBPyConnection, csv_path: str, table_name: str, replace: bool = True, **read_csv_kwargs):
    """Load a CSV file directly into a ``staging`` table via DuckDB's ``read_csv_auto``.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    csv_path : str
        Path to the CSV file.
    table_name : str
        Table name (without schema prefix).
    replace : bool, default True
        Drop and recreate the table if it already exists.
    **read_csv_kwargs
        Additional keyword arguments forwarded to DuckDB's
        ``read_csv_auto`` function.
    """
    if replace:
        con.execute(f"DROP TABLE IF EXISTS staging.{table_name}")
    con.execute(f"""
        CREATE TABLE staging.{table_name} AS 
        SELECT * FROM read_csv_auto('{csv_path}', {', '.join(f"{k}={repr(v)}" for k, v in read_csv_kwargs.items())})
    """)
 
def load_parquet_to_staging(con: duckdb.DuckDBPyConnection, parquet_path: str, table_name: str, replace: bool = True):
    """Load a Parquet file directly into a ``staging`` table.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    parquet_path : str
        Path to the Parquet file.
    table_name : str
        Table name (without schema prefix).
    replace : bool, default True
        Drop and recreate the table if it already exists.
    """
    if replace:
        con.execute(f"DROP TABLE IF EXISTS staging.{table_name}")
    con.execute(f"""
        CREATE TABLE staging.{table_name} AS 
        SELECT * FROM read_parquet('{parquet_path}')
    """)


def write_table_to_parquet(con: duckdb.DuckDBPyConnection, table_name: str, path: str, compression="snappy"):
    """Export a DuckDB table to a Parquet file.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    table_name : str
        Fully-qualified table name (e.g. ``'analytics.wiski'``).
    path : str
        Output Parquet file path.
    compression : str, default ``'snappy'``
        Parquet compression codec.
    """
    con.execute(f"COPY (SELECT * FROM {table_name}) TO '{path}' (FORMAT PARQUET, COMPRESSION '{compression}')")


def write_table_to_csv(con: duckdb.DuckDBPyConnection, table_name: str, path: str, header: bool = True, sep: str = ',', **kwargs):
    """Export a DuckDB table to a CSV file.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    table_name : str
        Fully-qualified table name.
    path : str
        Output CSV file path.
    header : bool, default True
        Include a header row.
    sep : str, default ``','``
        Column delimiter.
    **kwargs
        Additional DuckDB ``COPY`` options.
    """
    con.execute(f"COPY (SELECT * FROM {table_name}) TO '{path}' (FORMAT CSV, HEADER {str(header).upper()}, DELIMITER '{sep}' {', '.join(f', {k}={repr(v)}' for k, v in kwargs.items())})")




def load_df_to_analytics(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str):
    """Replace an ``analytics`` table with the contents of a DataFrame.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    df : pandas.DataFrame
        Transformed data to persist.
    table_name : str
        Table name (without schema prefix).
    """
    con.execute(f"DROP TABLE IF EXISTS analytics.{table_name}")
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE analytics.{table_name} AS SELECT * FROM tmp_df")
    con.unregister("tmp_df")


def migrate_staging_to_analytics(con: duckdb.DuckDBPyConnection, staging_table: str, analytics_table: str):
    """Copy a staging table directly into the analytics schema.

    The target analytics table is dropped and recreated from the staging
    table's contents.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    staging_table : str
        Source table name (without schema prefix).
    analytics_table : str
        Destination table name (without schema prefix).
    """
    con.execute(f"DROP TABLE IF EXISTS analytics.{analytics_table}")
    con.execute(f"""
        CREATE TABLE analytics.{analytics_table} AS 
        SELECT * FROM staging.{staging_table}
    """)


def dataframe_to_parquet(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, path, compression="snappy"):
    """Write a DataFrame to a Parquet file using a temporary DuckDB connection.

    .. note::

       The *con* parameter is currently ignored — a new in-memory
       connection is created internally.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Unused (retained for API compatibility).
    df : pandas.DataFrame
        Data to write.
    path : str
        Output Parquet file path.
    compression : str, default ``'snappy'``
        Parquet compression codec.
    """
    # path should be a filename like 'data/raw/equis/equis-20251118.parquet'
    con = duckdb.connect()
    con.register("tmp_df", df)
    con.execute(f"COPY (SELECT * FROM tmp_df) TO '{path}' (FORMAT PARQUET, COMPRESSION '{compression}')")
    con.unregister("tmp_df")
    con.close()