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
        create_staging_tables(con)
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

def create_staging_tables(con: duckdb.DuckDBPyConnection):
    '''
    Create necessary tables in the staging schema. These were copied directly from database DDL. Would need to be updated if sources change.
    '''
    con.execute("""
    CREATE TABLE IF NOT EXISTS staging.equis(
                LATITUDE DOUBLE,
                LONGITUDE DOUBLE,
                WID_LIST VARCHAR,
                SAMPLE_METHOD VARCHAR,
                SAMPLE_REMARK VARCHAR,
                FACILITY_ID BIGINT,
                FACILITY_NAME VARCHAR,
                FACILITY_TYPE VARCHAR,
                SYS_LOC_CODE VARCHAR,
                LOC_NAME VARCHAR,
                LOC_TYPE VARCHAR,
                LOC_TYPE_2 VARCHAR,
                TASK_CODE VARCHAR,
                SAMPLE_ID BIGINT,
                SYS_SAMPLE_CODE VARCHAR,
                TEST_ID BIGINT,
                ANALYTE_TYPE VARCHAR,
                ANALYTE_TYPE_DESC VARCHAR,
                ANALYTIC_METHOD VARCHAR,
                PREFERRED_NAME VARCHAR,
                PARAMETER VARCHAR,
                CAS_RN VARCHAR,
                CHEMICAL_NAME VARCHAR,
                GTLT VARCHAR,
                RESULT_TEXT VARCHAR,
                RESULT_NUMERIC DOUBLE,
                RESULT_UNIT VARCHAR,
                STAT_TYPE INTEGER,
                VALUE_TYPE VARCHAR,
                DETECT_FLAG VARCHAR,
                DETECT_DESC VARCHAR,
                RESULT_REMARK VARCHAR,
                RESULT_TYPE_CODE VARCHAR,
                METHOD_DETECTION_LIMIT VARCHAR,
                REPORTING_DETECTION_LIMIT VARCHAR,
                QUANTITATION_LIMIT INTEGER,
                LAB_QUALIFIERS VARCHAR,
                INTERPRETED_QUALIFIERS VARCHAR,
                REPORTABLE_RESULT VARCHAR,
                APPROVAL_CODE VARCHAR,
                SENSITIVE_NOTPUBLIC VARCHAR,
                TEST_TYPE VARCHAR,
                DILUTION_FACTOR DOUBLE,
                FRACTION VARCHAR,
                BASIS VARCHAR,
                TEMP_BASIS VARCHAR,
                TEST_REMARK VARCHAR,
                ANALYSIS_DATE_TIME TIMESTAMP_NS,
                ANALYSIS_DATE VARCHAR,
                ANALYSIS_TIME VARCHAR,
                ANALYSIS_DATE_TIMEZONE VARCHAR,
                COMPANY_NAME VARCHAR,
                LAB_NAME_CODE VARCHAR,
                LAB_SAMPLE_ID VARCHAR,
                SAMPLE_TYPE_GROUP VARCHAR,
                SAMPLE_TYPE_CODE VARCHAR,
                SAMPLE_TYPE_DESC VARCHAR,
                MEDIUM_CODE VARCHAR,
                MATRIX_CODE VARCHAR,
                START_DEPTH DOUBLE,
                DEPTH_UNIT VARCHAR,
                SAMPLE_DATE_TIME TIMESTAMP_NS,
                SAMPLE_DATE VARCHAR,
                SAMPLE_TIME VARCHAR,
                SAMPLE_DATE_TIMEZONE VARCHAR,
                EBATCH DOUBLE);
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS staging.wiski(
                "Timestamp" VARCHAR,
                "Value" DOUBLE,
                "Quality Code" BIGINT,
                "Quality Code Name" VARCHAR,
                ts_unitsymbol VARCHAR,
                ts_name VARCHAR,
                ts_id VARCHAR,
                station_no VARCHAR,
                station_name VARCHAR,
                station_latitude VARCHAR,
                station_longitude VARCHAR,
                parametertype_id VARCHAR,
                parametertype_name VARCHAR,
                stationparameter_no VARCHAR,
                stationparameter_name VARCHAR,
                wplmn_flag BIGINT);
    """)


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
    # Assumes this script is run from a location where this relative path is valid
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
    """
    Create tables in the outlets schema to define outlet-station-reach relationships.Copies from outlets module.
    """
    query = outlets.OUTLETS_SCHEMA
    con.execute(query)
    outlets.build_outlets(con)

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
    FROM staging.wiski;""")


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
        CREATE OR REPLACE VIEW reports.wiski_qc_count AS (
        SELECT 
            w.station_no,
            w.parametertype_name,
            w."Quality Code",
            COUNT(w."Quality Code") AS count,
            wqc."Text",
            wqc.Description,
            
        FROM staging.wiski w 
        LEFT JOIN mappings.wiski_quality_codes wqc
            ON w."Quality Code" = wqc.quality_code
        WHERE wqc.Active = 1
        GROUP BY
            w."Quality Code",wqc."Text",wqc.Description,w.parametertype_name, w.station_no
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
    CREATE OR REPLACE VIEW analytics.outlet_observations AS 
    SELECT
        o.datetime,
        os.outlet_id,
        o.constituent,
        AVG(o.value) AS value,
        COUNT(o.value) AS count
    FROM
        analytics.observations AS o
    INNER JOIN
        outlets.outlet_stations AS os ON o.station_id = os.station_id AND o.station_origin = os.station_origin
    WHERE os.outlet_id IS NOT NULL
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
                WITH 
                -- Extract baseflow data (constituent = 'QB')
                baseflow_data AS (
                    SELECT
                        outlet_id,
                        datetime,
                        "value" AS baseflow_value
                    FROM
                        analytics.outlet_observations
                    WHERE
                        constituent = 'QB'
                ),

                -- Extract flow data (constituent = 'Q')
                flow_data AS (
                    SELECT
                        outlet_id,
                        datetime,
                        "value" AS flow_value
                    FROM
                        analytics.outlet_observations
                    WHERE
                        constituent = 'Q'
                ),

                -- Extract all other constituent data (not 'Q' or 'QB')
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
                        constituent NOT IN ('Q', 'QB')
                )

                -- Final join: Only include rows that have baseflow, flow, and constituent data
                SELECT
                    c.outlet_id,
                    c.constituent,
                    c.datetime,
                    c."value",
                    c.count,
                    f.flow_value,
                    b.baseflow_value
                FROM
                    constituent_data AS c
                LEFT JOIN
                    flow_data AS f
                    ON c.outlet_id = f.outlet_id 
                    AND c.datetime = f.datetime
                LEFT JOIN
                    baseflow_data AS b
                    ON c.outlet_id = b.outlet_id 
                    AND c.datetime = b.datetime;""")
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
 
def create_outlet_summary_report(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE VIEW reports.outlet_constituent_summary AS
    SELECT
        outlet_id,
        constituent,
        count_star() AS sample_count,
        avg("value") AS average_value,
        min("value") AS min_value,
        max("value") AS max_value,
        "year"(min(datetime)) AS start_date,
        "year"(max(datetime)) AS end_date
    FROM
        analytics.outlet_observations
    GROUP BY
        constituent,
        outlet_id
    """)

    

def update_views(con: duckdb.DuckDBPyConnection):
    """
    Update all views in the database.
    """
    create_staging_qc_count_view(con)
    create_combined_observations_view(con)
    create_constituent_summary_report(con)
    create_outlet_observations_view(con)
    create_outlet_observations_with_flow_view(con)
    create_outlet_summary_report(con)
    
def connect(db_path: str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Returns a DuckDB connection to the given database path.
    Ensures the parent directory exists.
    """
    db_path = Path(db_path)
    parent = db_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(database=db_path.as_posix(), read_only=read_only)


def drop_station_id(con: duckdb.DuckDBPyConnection, station_id: str,station_origin: str):
    """
    Drop all data for a specific station from staging and analytics schemas.
    """
    con.execute(f"DELETE FROM staging.equis WHERE station_id = '{station_id}' AND station_origin = '{station_origin}'")
    con.execute(f"DELETE FROM staging.wiski WHERE station_id = '{station_id}' AND station_origin = '{station_origin}'")
    con.execute(f"DELETE FROM analytics.equis WHERE station_id = '{station_id}' AND station_origin = '{station_origin}'")
    con.execute(f"DELETE FROM analytics.wiski WHERE station_id = '{station_id}' AND station_origin = '{station_origin}'")
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


def add_to_table(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_schema: str, table_name: str):
    """
    Append a pandas DataFrame into a DuckDB table. This will create the table
    if it does not exist.
    """


    # get existing columns
    existing_columns = get_column_names(con, table_schema, table_name)
    df = df[[existing_columns]]


    # register pandas DF and create table if not exists
    con.register("tmp_df", df)

    con.execute(f"""
        INSERT INTO {table_schema}.{table_name} 
        SELECT * FROM tmp_df
    """)
    con.unregister("tmp_df")

def add_station_data(con: duckdb.DuckDBPyConnection, station_id: str, station_origin: str, table_schema: str, table_name: str, df: pd.DataFrame, replace: bool = False):
    """
    Add station data to the staging and analytics schemas.
    """
    if replace:
        drop_station_id(con, station_id, station_origin)
    add_to_table(con, df, table_schema, table_name)


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