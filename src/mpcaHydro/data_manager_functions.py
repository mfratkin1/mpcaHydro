# -*- coding: utf-8 -*-
"""Procedural functions for data management operations."""

from pathlib import Path
from typing import List, Optional, Union

import duckdb
import pandas as pd

AGG_DEFAULTS = {
    'cfs': 'mean',
    'mg/l': 'mean',
    'degf': 'mean',
    'lb': 'sum'
}

UNIT_DEFAULTS = {
    'Q': 'cfs',
    'QB': 'cfs',
    'TSS': 'mg/l',
    'TP': 'mg/l',
    'OP': 'mg/l',
    'TKN': 'mg/l',
    'N': 'mg/l',
    'WT': 'degf',
    'WL': 'ft'
}



def init_warehouse(db_path: Union[str, Path], reset: bool = False) -> Path:
    """Initialize the data warehouse database and return the db_path."""
    from mpcaHydro import warehouse
    db_path = Path(db_path)
    warehouse.init_db(db_path.as_posix(), reset)
    return db_path


def update_views(con: duckdb.DuckDBPyConnection) -> None:
    """Update all database views."""
    from mpcaHydro import warehouse
    warehouse.update_views(con)


def process_wiski_data(
    con: duckdb.DuckDBPyConnection,
    filter_qc_codes: bool = True,
    data_codes: Optional[List[int]] = None,
    baseflow_method: str = 'Boughton'
) -> None:
    """Process WISKI data from staging to analytics."""
    from mpcaHydro import wiski, warehouse
    df = con.execute("SELECT * FROM staging.wiski").df()
    df_transformed = wiski.transform(df, filter_qc_codes, data_codes, baseflow_method)
    warehouse.load_df_to_table(con, df_transformed, 'analytics.wiski')
    warehouse.update_views(con)


def process_equis_data(con: duckdb.DuckDBPyConnection) -> None:
    """Process EQuIS data from staging to analytics."""
    from mpcaHydro import equis, warehouse
    df = con.execute("SELECT * FROM staging.equis").df()
    df_transformed = equis.transform(df)
    warehouse.load_df_to_table(con, df_transformed, 'analytics.equis')
    warehouse.update_views(con)


def process_all_data(
    con: duckdb.DuckDBPyConnection,
    filter_qc_codes: bool = True,
    data_codes: Optional[List[int]] = None,
    baseflow_method: str = 'Boughton'
) -> None:
    """Process all data (WISKI and EQuIS) from staging to analytics."""
    process_wiski_data(con, filter_qc_codes, data_codes, baseflow_method)
    process_equis_data(con)


def drop_wiski_station_data(
    con: duckdb.DuckDBPyConnection,
    station_ids: List[str]
) -> None:
    """Drop WISKI data for specified station IDs from staging and analytics."""
    for station_id in station_ids:
        con.execute("DELETE FROM staging.wiski WHERE station_no = ?", [station_id])
        con.execute("DELETE FROM analytics.wiski WHERE station_id = ?", [station_id])


def drop_equis_station_data(
    con: duckdb.DuckDBPyConnection,
    station_ids: List[str]
) -> None:
    """Drop EQuIS data for specified station IDs from staging and analytics."""
    for station_id in station_ids:
        con.execute("DELETE FROM staging.equis WHERE SYS_LOC_CODE = ?", [station_id])
        con.execute("DELETE FROM analytics.equis WHERE station_id = ?", [station_id])


def download_wiski_data(
    con: duckdb.DuckDBPyConnection,
    station_ids: List[str],
    start_year: int = 1996,
    end_year: int = 2030,
    filter_qc_codes: bool = True,
    data_codes: Optional[List[int]] = None,
    baseflow_method: str = 'Boughton',
    replace: bool = False
) -> None:
    """Download WISKI data for given station IDs and load into the warehouse."""
    from mpcaHydro import wiski, warehouse
    
    if replace:
        drop_wiski_station_data(con, station_ids)
    
    df = wiski.download(station_ids, start_year=start_year, end_year=end_year)
    if not df.empty:
        warehouse.load_df_to_table(con, df, 'staging.wiski')
        warehouse.load_df_to_table(
            con,
            wiski.transform(df, filter_qc_codes, data_codes, baseflow_method),
            'analytics.wiski'
        )
        warehouse.update_views(con)
    else:
        print('No data necessary for HSPF calibration from wiski for:', station_ids)


def download_equis_data(
    con: duckdb.DuckDBPyConnection,
    station_ids: List[str],
    oracle_username: str,
    oracle_password: str,
    replace: bool = False
) -> None:
    """Download EQuIS data for given station IDs and load into the warehouse."""
    from mpcaHydro import equis, warehouse
    
    if replace:
        drop_equis_station_data(con, station_ids)
    
    equis.connect(user=oracle_username, password=oracle_password)
    print('Connected to Oracle database.')
    df = equis.download(station_ids)
    equis.close_connection()
    if not df.empty:
        warehouse.load_df_to_table(con, df, 'staging.equis')
        warehouse.load_df_to_table(con, equis.transform(df.copy()), 'analytics.equis')
        warehouse.update_views(con)
    else:
        print('No data necessary for HSPF calibration from equis for:', station_ids)
    

def get_outlets(con: duckdb.DuckDBPyConnection, model_name: str) -> pd.DataFrame:
    """Get outlet data for a given model."""
    query = '''
    SELECT *
    FROM outlets.station_reach_pairs
    WHERE repository_name = ?
    ORDER BY outlet_id'''
    return con.execute(query, [model_name]).fetch_df()


def get_station_ids(
    con: duckdb.DuckDBPyConnection,
    station_origin: Optional[str] = None
) -> List[str]:
    """Get list of station IDs, optionally filtered by origin."""
    if station_origin is None:
        query = '''
        SELECT DISTINCT station_id, station_origin
        FROM analytics.observations'''
        df = con.execute(query).fetch_df()
    else:
        query = '''
        SELECT DISTINCT station_id
        FROM analytics.observations
        WHERE station_origin = ?'''
        df = con.execute(query, [station_origin]).fetch_df()
    return df['station_id'].to_list()


def get_observation_data(
    con: duckdb.DuckDBPyConnection,
    station_ids: List[str],
    constituent: str,
    agg_period: Optional[str] = None
) -> pd.DataFrame:
    """Get observation data for given stations and constituent."""
    query = '''
    SELECT *
    FROM analytics.observations
    WHERE station_id IN ? AND constituent = ?'''
    df = con.execute(query, [station_ids, constituent]).fetch_df()

    unit = UNIT_DEFAULTS.get(constituent, 'mg/l')
    agg_func = AGG_DEFAULTS.get(unit, 'mean')

    df.set_index('datetime', inplace=True)
    df.attrs['unit'] = unit
    df.attrs['constituent'] = constituent

    if agg_period is not None:
        df = df[['value']].resample(agg_period).agg(agg_func)
        df.attrs['agg_period'] = agg_period

    df.rename(columns={'value': 'observed'}, inplace=True)
    return df.dropna(subset=['observed'])


def get_outlet_data(
    con: duckdb.DuckDBPyConnection,
    outlet_id: int,
    constituent: str,
    agg_period: str = 'D'
) -> pd.DataFrame:
    """Get outlet observation data with flow for a given outlet and constituent."""
    query = '''
    SELECT *
    FROM analytics.outlet_observations_with_flow
    WHERE outlet_id = ? AND constituent = ?'''
    df = con.execute(query, [outlet_id, constituent]).fetch_df()

    unit = UNIT_DEFAULTS.get(constituent, 'mg/l')
    agg_func = AGG_DEFAULTS.get(unit, 'mean')

    df.set_index('datetime', inplace=True)
    df.attrs['unit'] = unit
    df.attrs['constituent'] = constituent

    if agg_period is not None:
        df = df[['value', 'flow_value', 'baseflow_value']].resample(agg_period).agg(agg_func)
        df.attrs['agg_period'] = agg_period

    df.rename(columns={
        'value': 'observed',
        'flow_value': 'observed_flow',
        'baseflow_value': 'observed_baseflow'
    }, inplace=True)
    return df.dropna(subset=['observed'])


def get_station_data(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str
) -> pd.DataFrame:
    """Get all observation data for a specific station."""
    query = '''
    SELECT *
    FROM analytics.observations
    WHERE station_id = ? AND station_origin = ?'''
    return con.execute(query, [station_id, station_origin]).fetch_df()


def get_raw_data(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str
) -> pd.DataFrame:
    """Get raw staging data for a specific station."""
    if station_origin.lower() == 'equis':
        query = '''
        SELECT *
        FROM staging.equis_raw
        WHERE station_id = ?'''
    elif station_origin.lower() == 'wiski':
        query = '''
        SELECT *
        FROM staging.wiski_raw
        WHERE station_id = ?'''
    else:
        raise ValueError(f'Station origin {station_origin} not recognized.')
    return con.execute(query, [station_id]).fetch_df()


def get_constituent_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Get summary of constituents across all stations."""
    query = '''
    SELECT
      station_id,
      station_origin,
      constituent,
      COUNT(*) AS sample_count,
      year(MIN(datetime)) AS start_date,
      year(MAX(datetime)) AS end_date
    FROM
      analytics.observations
    GROUP BY
      constituent, station_id, station_origin
    ORDER BY
      sample_count'''
    return con.execute(query).fetch_df()


def export_station_to_csv(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str,
    output_path: Union[str, Path]
) -> None:
    """Export station data to a CSV file."""
    df = get_station_data(con, station_id, station_origin)
    df.to_csv(output_path, index=False)


def export_raw_to_csv(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str,
    output_path: Union[str, Path]
) -> None:
    """Export raw staging data to a CSV file."""
    df = get_raw_data(con, station_id, station_origin)
    df.to_csv(output_path, index=False)


def get_equis_template(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Get an empty DataFrame with EQuIS staging table schema."""
    query = '''SELECT * FROM staging.equis LIMIT 0'''
    return con.execute(query).fetch_df()


def get_wiski_template(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Get an empty DataFrame with WISKI staging table schema."""
    query = '''SELECT * FROM staging.wiski LIMIT 0'''
    return con.execute(query).fetch_df()


def outlet_summary(con: duckdb.DuckDBPyConnection):
    query = '''
    SELECT *,
    FROM 
        reports.outlet_constituent_summary
    ORDER BY
        outlet_id,
        constituent
    '''
    df = con.execute(query).fetch_df()
    return df
        

def wiski_qc_counts(con: duckdb.DuckDBPyConnection):
    query = '''
    SELECT *,
    FROM 
        reports.wiski_qc_count
    ORDER BY
        station_no,
        parametertype_name
    '''
    df = con.execute(query).fetch_df()
    return df

def station_summary(con: duckdb.DuckDBPyConnection,constituent: str = None):
    
    query = '''
    SELECT *,
    FROM 
        reports.constituent_summary
    ORDER BY
        station_id,
        station_origin,
        constituent
    '''
    df = con.execute(query).fetch_df()
    if constituent is not None:
        df = df[df['constituent'] == constituent]
    return df

def station_reach_pairs(con: duckdb.DuckDBPyConnection):
    query = '''
    SELECT *,
    FROM 
        reports.station_reach_pairs
    ORDER BY
        outlet_id,
        station_id
    '''
    df = con.execute(query).fetch_df()
    return df

class DataManagerWrapper:
    """Minimal wrapper class that calls procedural functions with context-managed connections."""
    
    def __init__(self, db_path: Union[str, Path], reset: bool = False):
        """Initialize wrapper with database path."""
        self.db_path = Path(db_path)
        if reset:
            self._init_warehouse(reset=True)
    
    def _init_warehouse(self, reset: bool = False) -> None:
        """Initialize the data warehouse database."""
        init_warehouse(self.db_path, reset)

    def _connect(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Create a database connection."""
        from mpcaHydro import warehouse
        return warehouse.connect(self.db_path.as_posix(), read_only=read_only)
    
    def update_views(self) -> None:
        """Update all database views."""
        with self._connect(read_only=False) as con:
            update_views(con)
    
    def wiski_qc_counts(self):
        with self._connect(read_only=True) as con:
            return wiski_qc_counts(con)
        
    def station_summary(self,constituent: str = None):
        with self._connect(read_only=True) as con:
            return station_summary(con,constituent)
        
    def station_reach_pairs(self):
        with self._connect(read_only=True) as con:
            return station_reach_pairs(con)
        
    def outlet_summary(self):
        with self._connect(read_only=True) as con:
            return outlet_summary(con)
    
    
    def process_wiski_data(
        self,
        filter_qc_codes: bool = True,
        data_codes: Optional[List[int]] = None,
        baseflow_method: str = 'Boughton'
    ) -> None:
        """Process WISKI data from staging to analytics."""
        with self._connect(read_only=False) as con:
            process_wiski_data(con, filter_qc_codes, data_codes, baseflow_method)
    
    def process_equis_data(self) -> None:
        """Process EQuIS data from staging to analytics."""
        with self._connect(read_only=False) as con:
            process_equis_data(con)
    
    def process_all_data(
        self,
        filter_qc_codes: bool = True,
        data_codes: Optional[List[int]] = None,
        baseflow_method: str = 'Boughton'
    ) -> None:
        """Process all data (WISKI and EQuIS) from staging to analytics."""
        with self._connect(read_only=False) as con:
            process_all_data(con, filter_qc_codes, data_codes, baseflow_method)
    
    def download_wiski_data(
        self,
        station_ids: List[str],
        start_year: int = 1996,
        end_year: int = 2030,
        filter_qc_codes: bool = True,
        data_codes: Optional[List[int]] = None,
        baseflow_method: str = 'Boughton',
        replace: bool = False
    ) -> None:
        """Download WISKI data for given station IDs and load into the warehouse."""
        with self._connect(read_only=False) as con:
            download_wiski_data(
                con, station_ids, start_year, end_year,
                filter_qc_codes, data_codes, baseflow_method, replace
            )
    
    def download_equis_data(
        self,
        station_ids: List[str],
        oracle_username: str,
        oracle_password: str,
        replace: bool = False
    ) -> None:
        """Download EQuIS data for given station IDs and load into the warehouse."""
        with self._connect(read_only=False) as con:
            download_equis_data(con, station_ids, oracle_username, oracle_password, replace)
    
    def get_outlets(self, model_name: str) -> pd.DataFrame:
        """Get outlet data for a given model."""
        with self._connect(read_only=True) as con:
            return get_outlets(con, model_name)
    
    def get_station_ids(self, station_origin: Optional[str] = None) -> List[str]:
        """Get list of station IDs, optionally filtered by origin."""
        with self._connect(read_only=True) as con:
            return get_station_ids(con, station_origin)
    
    def get_observation_data(
        self,
        station_ids: List[str],
        constituent: str,
        agg_period: Optional[str] = None
    ) -> pd.DataFrame:
        """Get observation data for given stations and constituent."""
        with self._connect(read_only=True) as con:
            return get_observation_data(con, station_ids, constituent, agg_period)
    
    def get_outlet_data(
        self,
        outlet_id: int,
        constituent: str,
        agg_period: str = 'D'
    ) -> pd.DataFrame:
        """Get outlet observation data with flow for a given outlet and constituent."""
        with self._connect(read_only=True) as con:
            return get_outlet_data(con, outlet_id, constituent, agg_period)
    
    def get_station_data(self, station_id: str, station_origin: str) -> pd.DataFrame:
        """Get all observation data for a specific station."""
        with self._connect(read_only=True) as con:
            return get_station_data(con, station_id, station_origin)
    
    def get_raw_data(self, station_id: str, station_origin: str) -> pd.DataFrame:
        """Get raw staging data for a specific station."""
        with self._connect(read_only=True) as con:
            return get_raw_data(con, station_id, station_origin)
    
    def get_constituent_summary(self) -> pd.DataFrame:
        """Get summary of constituents across all stations."""
        with self._connect(read_only=True) as con:
            return get_constituent_summary(con)
    
    def export_station_to_csv(
        self,
        station_id: str,
        station_origin: str,
        output_path: Union[str, Path]
    ) -> None:
        """Export station data to a CSV file."""
        with self._connect(read_only=True) as con:
            export_station_to_csv(con, station_id, station_origin, output_path)
    
    def export_raw_to_csv(
        self,
        station_id: str,
        station_origin: str,
        output_path: Union[str, Path]
    ) -> None:
        """Export raw staging data to a CSV file."""
        with self._connect(read_only=True) as con:
            export_raw_to_csv(con, station_id, station_origin, output_path)
    
    def get_equis_template(self) -> pd.DataFrame:
        """Get an empty DataFrame with EQuIS staging table schema."""
        with self._connect(read_only=True) as con:
            return get_equis_template(con)
    
    def get_wiski_template(self) -> pd.DataFrame:
        """Get an empty DataFrame with WISKI staging table schema."""
        with self._connect(read_only=True) as con:
            return get_wiski_template(con)
