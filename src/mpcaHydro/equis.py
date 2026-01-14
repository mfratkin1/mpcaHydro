

from datetime import datetime, timezone, timedelta
import pandas as pd
from typing import Union
import oracledb
import duckdb

CONNECTION = None

CAS_RN_MAP = {'479-61-8':'CHLA',
            'CHLA-CORR':'CHLA',
            'BOD':'BOD',
            'NO2NO3':'N', #TODO change to 'NO2NO3'
            '14797-55-8': 'NO3',
            '14797-65-0':'NO2',
            '14265-44-2': 'OP',
            'N-KJEL' : 'TKN',
            'PHOSPHATE-P': 'TP',
            '7723-14-0' : 'TP',
            'SOLIDS-TSS': 'TSS',
            'TEMP-W' : 'WT',
            '7664-41-7' : 'NH3'}

def connect(user: str, password: str, host: str = "DELTAT", port: int = 1521, sid: str = "DELTAT"):
    '''Create and return an Oracle database connection.'''
    
    global CONNECTION
    CONNECTION = oracledb.connect(user=user, 
                                 password=password, 
                                 host=host, 
                                 port=port, 
                                 sid=sid) 
    return CONNECTION

def close_connection():
    '''Close the global Oracle database connection if it exists.'''
    global CONNECTION
    if CONNECTION:
        CONNECTION.close()
        CONNECTION = None



def test_connection():
    raise NotImplementedError("This function is a placeholder for testing Oracle DB connection.")
    try:
        # or for SID:
        # connection = oracledb.connect(user="your_username", 
        #                             password="your_password", 
        #                             host="your_host", 
        #                             port=1521, 
        #                             sid="your_sid")

        print("Successfully connected to Oracle Database")

        # Perform database operations here
        # ...
        if connection:
            connection.close()
            print("Connection closed")
    except oracledb.Error as e:
        print(f"Error connecting to Oracle Database: {e}")



def make_placeholders(items):
    '''Create SQL placeholders and bind values for a list of items'''
    # Create placeholders like :id0, :id1, :id2
    placeholders = ', '.join(f':id{i}' for i in range(len(items)))
    # Create dictionary of bind values
    binds = {f'id{i}': val for i, val in enumerate(items)}
    return placeholders, binds

def to_dataframe(odb_cursor):
    '''Convert Oracle cursor results to a pandas DataFrame'''
    column_names = [description[0] for description in odb_cursor.description]
    rows = odb_cursor.fetchall()
    df = pd.DataFrame(rows,columns = column_names)
    return df

#%% Query for station locations with HSPF related constituents
 
def download(station_ids):
    '''Download data for given station IDs from Oracle database.
    This grabs data from the Data access Layer (DAL) equis result view for
    river/stream locations and HSPF related constituents only.'''
    placeholders, binds = make_placeholders(station_ids)
    query = f"""
SELECT
    mpca_dal.eq_fac_station.latitude,
    mpca_dal.eq_fac_station.longitude,
    mpca_dal.eq_fac_station.wid_list,
    mpca_dal.eq_sample.sample_method,
    mpca_dal.eq_sample.sample_remark,
    mpca_dal.mv_eq_result.*
    FROM
    	mpca_dal.mv_eq_result
		LEFT JOIN mpca_dal.eq_fac_station 
		   ON mpca_dal.mv_eq_result.sys_loc_code = mpca_dal.eq_fac_station.sys_loc_code
		   AND mpca_dal.mv_eq_result.facility_id = mpca_dal.eq_fac_station.facility_id        
		LEFT JOIN mpca_dal.eq_sample ON mpca_dal.mv_eq_result.sample_id = mpca_dal.eq_sample.sample_id
    WHERE
        mpca_dal.mv_eq_result.cas_rn IN ('479-61-8',
                            'CHLA-CORR',
                            'BOD',
                            'NO2NO3',
                            '14797-55-8',
                            '14797-65-0',
                            '14265-44-2',
                            'N-KJEL',
                            'PHOSPHATE-P',
                            '7723-14-0',
                            'SOLIDS-TSS',
                            'TEMP-W',
                            '7664-41-7',
                            'FLOW')
        AND mpca_dal.eq_fac_station.loc_type = 'River/Stream'
        AND mpca_dal.mv_eq_result.approval_code = 'Final'
        AND mpca_dal.mv_eq_result.reportable_result = 'Y'
        AND mpca_dal.mv_eq_result.facility_id IN ( 1, 33836701 )
        AND mpca_dal.eq_sample.sample_method IN ('G-EVT', 'G', 'FIELDMSROBS', 'LKSURF1M', 'LKSURF2M', 'LKSURFOTH')
        AND mpca_dal.mv_eq_result.sys_loc_code IN ({placeholders})
    """
    with CONNECTION.cursor() as cursor:
        cursor.execute(query,binds)
        return to_dataframe(cursor)
    


def as_utc_offset(naive_dt: Union[datetime, str], tz_label: str, target_offset: timezone) -> datetime:
    """
    Interpret naive_dt (datetime or ISO string) using tz_label ('CST'|'CDT' or other).
    - If tz_label == 'CST' -> interpret as UTC-6
    - If tz_label == 'CDT' -> interpret as UTC-5
    - Else -> attempt to interpret using America/Chicago (ZoneInfo) which applies DST rules.

    WARNING : This function uses a replace mthod that assumes the input datetime is naive (no tzinfo).
    If the input datetime already has tzinfo, this will lead to incorrect results.

    Returns a timezone-aware datetime converted to a fixed UTC-6 timezone (tzinfo=UTC_MINUS_6).
    This preserves the instant and expresses it in UTC-6. 


    """
    if isinstance(naive_dt, str):
        naive = pd.to_datetime(naive_dt).to_pydatetime()
    elif isinstance(naive_dt, datetime):
        naive = naive_dt
    else:
        raise TypeError("naive_dt must be datetime or str")

    label = (tz_label or "").strip().upper()

    if label == "CST":
        src_tz = timezone(timedelta(hours=-6))
    elif label == "CDT":
        src_tz = timezone(timedelta(hours=-5))
    elif label == 'UTC':
        src_tz = timezone.utc
    else:
        raise ValueError(f"Unexpected timezone label: {tz_label}")
    # attach the source tz (interpret naive as local time in src_tz)
    aware_src = naive.replace(tzinfo=src_tz)

    # convert the instant to fixed UTC-6
    return aware_src.astimezone(target_offset).tz_localize(None)


def normalize_columns(df):
    '''Select relevant columns from Equis data.'''
    return df[['SYS_LOC_CODE',
               'constituent',
               'CAS_RN',
               'datetime',
               'RESULT_NUMERIC',
               'RESULT_UNIT',
               ]].rename(columns={
                   'SYS_LOC_CODE':'station_id',
                   'RESULT_NUMERIC':'value',
                   'RESULT_UNIT':'unit',
                   'CAS_RN':'cas_rn'
               })



def normalize_timezone(df):
    '''Normalize datetime to UTC in Equis data.'''
    target_offset = timezone(timedelta(hours=-6))
    def _conv(row):
        try:
            return as_utc_offset(row['SAMPLE_DATE_TIME'], row['SAMPLE_DATE_TIMEZONE'],target_offset)
        except Exception:
            return pd.NaT

    df.loc[:,'datetime'] = df.apply(_conv, axis=1)
    return df

def convert_units(df):
    '''Convert units in Equis data to standard units.'''
    # Convert ug/L to mg/L
    df['unit'] = df['unit'].str.lower()

    mask_ugL = df['unit'] == 'ug/l'
    df.loc[mask_ugL, 'value'] = df.loc[mask_ugL, 'value'] / 1000
    df.loc[mask_ugL, 'unit'] = 'mg/l'

    # Convert mg/g to mg/L (assuming density of 1 g/mL)
    mask_mgg = df['unit'] == 'mg/g'
    df.loc[mask_mgg, 'value'] = df.loc[mask_mgg, 'value'] * 1000
    df.loc[mask_mgg, 'unit'] = 'mg/l'

    # Convert deg C to degF
    mask_degC = df['unit'].isin(['deg c', 'degc'])
    df.loc[mask_degC, 'value'] = (df.loc[mask_degC, 'value'] * 9/5) + 32
    df.loc[mask_degC, 'unit'] = 'degf'

    return df

def map_constituents(df):
    '''Map CAS_RN to standard constituent names in Equis data.'''
    df['constituent'] = df['CAS_RN'].map(CAS_RN_MAP)
    return df


def average_results(df):
    ''' Average samples by hour, station, and constituent'''
    df['datetime'] = df['datetime'].dt.round('h')
    df['station_origin'] = 'equis'
    return df.groupby(['station_id', 'datetime', 'constituent', 'unit','station_origin']).agg(
        value=('value', 'mean')
    ).reset_index()

def replace_nondetects(df):
    '''Replace non-detect results with 0 in Equis data.'''
    df.loc[df['value'].isna(), 'value'] = 0
    return df

def normalize(df):
    '''Normalize Equis data: select relevant columns.'''
    df = map_constituents(df)
    df = normalize_timezone(df)
    df = normalize_columns(df)
    df = convert_units(df)
    return df

def transform(df):
    '''Transform Equis data: handle non-detects, convert units, map constituents.'''
    
    df = normalize(df)
    df = replace_nondetects(df)
    if not df.empty:
        df = average_results(df)
    return df



#%% Transformations using duckdb instead of pandas
# def transform_staging_to_hourly_cte(con: duckdb.DuckDBPyConnection,
#                                     source_table: str,
#                                     analytics_table: str):
#     """
#     Single-statement transformation using chained CTEs.
#     - Good when you want the whole logical pipeline in one place and avoid intermediate objects.
#     - Produces analytics.<analytics_table> as the final materialized table.
#     """

#     mapping_cases = " ".join([f"WHEN '{k}' THEN '{v}'" for k, v in CAS_RN_MAP.items()])
#     target_offset_hours = -6
#     # Example assumes source_table has: station_id, datetime, value (numeric), constituent, unit, station_origin
#     sql = f"""
#     CREATE OR REPLACE TABLE {analytics_table} AS
#     WITH
#         -- Step 1: normalize column names
#         normalized AS (
#             SELECT *,
#                 SYS_LOC_CODE AS station_id,
#                 SAMPLE_DATE_TIME AS datetime,
#                 SAMPLE_DATE_TIMEZONE AS datetime_timezone,
#                 RESULT_NUMERIC AS value,
#                 RESULT_UNIT AS unit
#             FROM {source_table}),

#         -- map constituents
#         constituents AS (
#         SELECT
#             *,
#             CASE CAS_RN
#                 {mapping_cases}
#                 ELSE NULL
#             END AS constituent
#         FROM normalized),

#         -- Step 2: convert units
#         conversions AS (
#         SELECT *,
#             CASE 
#                 WHEN LOWER(unit) = 'ug/l' THEN value / 1000
#                 WHEN LOWER(unit) = 'mg/g' THEN value * 1000
#                 WHEN LOWER(unit) IN ('deg c', 'degc') THEN (value * 9/5) + 32
#                 ELSE value
#             END AS value,
#             CASE 
#                 WHEN LOWER(unit) = 'ug/l' THEN 'mg/L'
#                 WHEN LOWER(unit) = 'mg/g' THEN 'mg/L'
#                 WHEN LOWER(unit) IN ('deg c', 'degc') THEN 'degF'
#                 ELSE unit
#             END AS unit
#         FROM constituents),

#         -- normalize timezone
#         timezones AS (
#             SELECT *,
#                 CASE
#                     WHEN datetime_timezone = 'CST' THEN 
#                         (datetime AT TIME ZONE INTERVAL '-6 hours') AT TIME ZONE INTERVAL '{target_offset_hours} hours'
#                     WHEN datetime_timezone = 'CDT' THEN 
#                         (datetime AT TIME ZONE INTERVAL '-5 hours') AT TIME ZONE INTERVAL '{target_offset_hours} hours'
#                     ELSE 
#                         datetime AT TIME ZONE INTERVAL '{target_offset_hours} hours'
#                 END AS datetime
#             FROM conversions),

        
#         hourly AS (
#                 SELECT 
#                     station_id,
#                     DATE_TRUNC('hour', datetime + INTERVAL '30 minute') AS datetime,
#                     constituent,
#                     unit,
#                     'equis' AS station_origin,
#                     AVG(value) AS value
#                 FROM timezone
#                 GROUP BY station_id, datetime, constituent, unit
#                 )

#     SELECT * FROM hourly
#     """
#     con.execute(sql)
#     return 0





# #%%

# def normalize_columns(con: duckdb.DuckDBPyConnection, table_name: str):
#     '''
#     Select relevant columns from Equis data using DuckDB.
#     '''
#     con.execute(f"""
#         CREATE TEMP VIEW v_normalized AS
#         SELECT *,
#             SYS_LOC_CODE AS station_id,
#             SAMPLE_DATE_TIME AS datetime,
#             SAMPLE_DATE_TIMEZONE AS datetime_timezone,
#             RESULT_NUMERIC AS value,
#             RESULT_UNIT AS unit
#         FROM {table_name} e
#     """)


# def map_constituents_duckdb(con: duckdb.DuckDBPyConnection, table_name: str):
#     '''
#     Map CAS_RN to standard constituent names in Equis data using DuckDB.
#     '''
    
#     mapping_cases = " ".join([f"WHEN '{k}' THEN '{v}'" for k, v in CAS_RN_MAP.items()])
#     con.execute(f"""
#         CREATE TEMP VIEW v_constituents AS
#         SELECT
#             *,
#             CASE CAS_RN
#                 {mapping_cases}
#                 ELSE NULL
#             END AS constituent
#         FROM v_normalized
#     """)

# def convert_units_duckdb(con: duckdb.DuckDBPyConnection, table_name: str):
#     '''
#     Convert units in Equis data to standard units using DuckDB.
#     '''

#     mapping_cases = " ".join([f"WHEN '{k}' THEN '{v}'" for k, v in CAS_RN_MAP.items()])
#     target_offset = timedelta(hours=-6)


#     con.execute(f"""
#         CREATE TEMP VIEW v_conversions AS
#         SELECT
#             *,


#             CASE 
#                 WHEN LOWER(unit) = 'ug/l' THEN value / 1000
#                 WHEN LOWER(unit) = 'mg/g' THEN value * 1000
#                 WHEN LOWER(unit) IN ('deg c', 'degc') THEN (value * 9/5) + 32
#                 ELSE value
#             END AS value,
#             CASE 
#                 WHEN LOWER(unit) = 'ug/l' THEN 'mg/L'
#                 WHEN LOWER(unit) = 'mg/g' THEN 'mg/L'
#                 WHEN LOWER(unit) IN ('deg c', 'degc') THEN 'degF'
#                 ELSE unit
#             END AS unit
#         FROM v_constituents""")


# def normalize_timezone(con: duckdb.DuckDBPyConnection, source_table: str, target_offset_hours: int = -6):

#     con.execute(f"""
#         CREATE TEMP VIEW v_timezone AS
#             SELECT *,
#                 CASE
#                     WHEN SAMPLE_DATE_TIMEZONE = 'CST' THEN 
#                         (SAMPLE_DATE_TIME AT TIME ZONE INTERVAL '-6 hours') AT TIME ZONE INTERVAL '{target_offset_hours} hours'
#                     WHEN SAMPLE_DATE_TIMEZONE = 'CDT' THEN 
#                         (SAMPLE_DATE_TIME AT TIME ZONE INTERVAL '-5 hours') AT TIME ZONE INTERVAL '{target_offset_hours} hours'
#                     ELSE 
#                         SAMPLE_DATE_TIME AT TIME ZONE INTERVAL '{target_offset_hours} hours'
#                 END AS datetime
#             FROM {source_table}""")


# def average_results(con: duckdb.DuckDBPyConnection, table_name: str):
#     '''
#     Average samples by hour, station, and constituent using DuckDB.
#     '''
#     con.execute(f"""
#         CREATE TABLE analytics.equis v_averaged AS
#         SELECT 
#             station_id,
#             DATE_TRUNC('hour', datetime) AS datetime,
#             constituent,
#             unit,
#             'equis' AS station_origin,
#             AVG(value) AS value
#         FROM v_timezone
#         GROUP BY station_id, DATE_TRUNC('hour', datetime), constituent, unit
#     """ )

def fetch_station_locations():
    '''Fetch station location data for stations with HSPF related constituents.'''
    query ="""SELECT DISTINCT
    m.SYS_LOC_CODE,
    stn.LONGITUDE,
    stn.LATITUDE,
    stn.LOC_MAJOR_BASIN,
    stn.NON_PUBLIC_LOCATION_FLAG
    FROM MPCA_DAL.MV_EQ_RESULT m
    LEFT JOIN MPCA_DAL.EQ_FAC_STATION_NP stn
    ON m.SYS_LOC_CODE = stn.SYS_LOC_CODE
    WHERE m.LOC_TYPE = 'River/Stream'
    AND m.CAS_RN IN ('479-61-8',
                        'CHLA-CORR',
                        'BOD',
                        'NO2NO3',
                        '14797-55-8',
                        '14797-65-0',
                        '14265-44-2',
                        'N-KJEL',
                        'PHOSPHATE-P',
                        '7723-14-0',
                        'SOLIDS-TSS',
                        'TEMP-W',
                        '7664-41-7')
        """
    with CONNECTION.cursor() as cursor:
        cursor.execute(query)
        df = to_dataframe(cursor)
        
        # dups = set(df.loc[df['SYS_LOC_CODE'].isin(df.loc[df['SYS_LOC_CODE'].duplicated()]['SYS_LOC_CODE']),'SYS_LOC_CODE'].to_list())
        # for dup in dups:
        #     #percent difference between lat/long values
        #     sub = df.loc[df['SYS_LOC_CODE'] == dup]
        #     lat_diff = abs(sub['LATITUDE'].max() - sub['LATITUDE'].min()) / ((sub['LATITUDE'].max() + sub['LATITUDE'].min()) / 2) * 100
        #     long_diff = abs(sub['LONGITUDE'].max() - sub['LONGITUDE'].min()) / ((sub['LONGITUDE'].max() + sub['LONGITUDE'].min()) / 2) * 100
        #     print(f'Duplicate station {dup} has {lat_diff:.6f}% latitude difference')
        #     print(f'Duplicate station {dup} has {long_diff:.6f}% longitude difference')

        
        # geometry = gpd.points_from_xy(df['LONGITUDE'], df['LATITUDE'])
        # gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        # filename = 'EQ_STATION_' + str(date.today()) + '.gpkg'
        # gdf.to_file(save_path.joinpath(filename), driver = 'GPKG')
        # gdf.rename(columns={'SYS_LOC_CODE':'station_id'}, inplace=True)
