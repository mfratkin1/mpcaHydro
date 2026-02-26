# -*- coding: utf-8 -*-
"""Data manager module for managing observation data warehouse."""

#from abc import abstractmethod
from pathlib import Path
from mpcaHydro import etlSWD
from mpcaHydro import equis, wiski, warehouse
from mpcaHydro import xref
from mpcaHydro import outlets
from mpcaHydro.reports import reportManager
import duckdb

# Import procedural functions
from mpcaHydro.warehouse_functions import (
    get_db_path,
    init_warehouse,
    update_views,
    process_wiski_data,
    process_equis_data,
    process_all_data,
    download_wiski_data,
    download_equis_data,
    get_outlets,
    get_station_ids,
    get_observation_data,
    get_outlet_data,
    get_station_data,
    get_raw_data,
    get_constituent_summary,
    export_station_to_csv,
    export_raw_to_csv,
    get_equis_template,
    get_wiski_template,
    AGG_DEFAULTS,
    UNIT_DEFAULTS,
    DataManagerWrapper,
)

# Re-export for backwards compatibility
__all__ = [
    # Classes
    'dataManager',
    'DataManagerWrapper',
    # Existing module-level functions
    'validate_constituent',
    'validate_unit',
    'build_warehouse',
    'constituent_summary',
    # Procedural functions
    'get_db_path',
    'init_warehouse',
    'update_views',
    'process_wiski_data',
    'process_equis_data',
    'process_all_data',
    'download_wiski_data',
    'download_equis_data',
    'drop_wiski_station_data',
    'drop_equis_station_data',
    'get_outlets',
    'get_station_ids',
    'get_observation_data',
    'get_outlet_data',
    'get_station_data',
    'get_raw_data',
    'get_constituent_summary',
    'export_station_to_csv',
    'export_raw_to_csv',
    'get_equis_template',
    'get_wiski_template',
    'AGG_DEFAULTS',
    'UNIT_DEFAULTS',
]


def validate_constituent(constituent):
    assert constituent in ['Q','TSS','TP','OP','TKN','N','WT','DO','WL','CHLA']

def validate_unit(unit):
    assert(unit in ['mg/l','lb','cfs','degF'])


def build_warehouse(folderpath):
    folderpath = Path(folderpath)
    db_path = folderpath.joinpath('observations.duckdb').as_posix()
    warehouse.init_db(db_path)

def constituent_summary(db_path):
    with duckdb.connect(db_path) as con:
        query = '''
        SELECT
          station_id,
          station_origin,
          constituent,
          COUNT(*) AS sample_count,
          year(MIN(datetime)) AS start_date,
          year(MAX(datetime)) AS end_date
        FROM
          observations
        GROUP BY
          constituent, station_id,station_origin
        ORDER BY
          sample_count;'''
          
        res = con.execute(query)
        return res.fetch_df()




class dataManager():

    def __init__(self,folderpath, oracle_username = None, oracle_password =None, reset = False):
        
        self.data = {}
        self.folderpath = Path(folderpath)
        self.db_path = self.folderpath.joinpath('observations.duckdb')
        self.oracle_username = oracle_username
        self.oracle_password = oracle_password

        if not self.db_path.exists() or reset:
            self._build_warehouse()

        self.xref = xref #TODO: implement xref manager class
        self.outlets = outlets #TODO: implement outlets manager class
        self.reports = reportManager(self.db_path)

    def _set_oracle_credentials(self, oracle_username, oracle_password):
        self.oracle_username = oracle_username
        self.oracle_password = oracle_password
    
    def connect_to_oracle(self):
        assert (self.credentials_exist(), 'Oracle credentials not found. Set ORACLE_USER and ORACLE_PASSWORD environment variables or use swd as station_origin')
        return equis.connect(user = self.oracle_username, password = self.oracle_password)
    
    def credentials_exist(self):
        if (self.oracle_username is not None) & (self.oracle_password is not None):
            return True
        else:
            return False
        
    def _build_warehouse(self):
        warehouse.init_db(self.db_path.as_posix(),True)

    def _process_wiski_data(self,filter_qc_codes = True, data_codes = None, baseflow_method = 'Boughton'):
        with warehouse.connect(self.db_path,read_only = False) as con:
            df = con.execute("SELECT * FROM staging.wiski").df()
            df_transformed = wiski.transform(df, filter_qc_codes, data_codes, baseflow_method)
            warehouse.load_df_to_table(con,df_transformed, 'analytics.wiski')
            warehouse.update_views(con)

    def _process_equis_data(self):
        with warehouse.connect(self.db_path,read_only = False) as con:
            df = con.execute("SELECT * FROM staging.equis").df()
            df_transformed = equis.transform(df)
            warehouse.load_df_to_table(con,df_transformed, 'analytics.equis')
            warehouse.update_views(con)

    def _process_data(self,filter_qc_codes = True, data_codes = None, baseflow_method = 'Boughton'):
        self._process_wiski_data(filter_qc_codes, data_codes, baseflow_method)
        self._process_equis_data()

    def _update_views(self):
        with warehouse.connect(self.db_path,read_only = False) as con:
            warehouse.update_views(con)

    def _download_wiski_data(self,station_ids,start_year = 1996, end_year = 2030, filter_qc_codes = True, data_codes = None, baseflow_method = 'Boughton'):
        with warehouse.connect(self.db_path,read_only = False) as con:
            df = wiski.download(station_ids,start_year = start_year, end_year = end_year)
            if not df.empty:
                warehouse.load_df_to_table(con,df, 'staging.wiski')
                warehouse.load_df_to_table(con,wiski.transform(df, filter_qc_codes,data_codes,baseflow_method), 'analytics.wiski')
                warehouse.update_views(con)
            else:
                print('No data neccesary for HSPF calibration available from wiski for stations:',station_ids)     

    def _download_equis_data(self,station_ids):
        if self.credentials_exist():
            oracle_conn = self.connect_to_oracle()
            print('Connected to Oracle database.')
            with warehouse.connect(self.db_path,read_only = False) as con:
                df = equis.download(station_ids, connection=oracle_conn)
                equis.close_connection(oracle_conn)
                if not df.empty:
                    warehouse.load_df_to_table(con,df, 'staging.equis')
                    warehouse.load_df_to_table(con,equis.transform(df.copy()), 'analytics.equis')
                    warehouse.update_views(con)
                else:
                    print('No data neccesary for HSPF calibration available from equis for stations:',station_ids)
        else:
            raise ValueError('Oracle credentials not found. Set ORACLE_USER and ORACLE_PASSWORD environment variables or use swd as station_origin')
        

    def _get_equis_template(self):
        with duckdb.connect(self.db_path,read_only=True) as con:
            query = '''
            SELECT *
            FROM staging.equis
            LIMIT 0'''
            df = con.execute(query).fetch_df().to_csv(self.folderpath.joinpath('equis_template.csv'), index=False)
        return df
    
    def _get_wiski_template(self):
        with duckdb.connect(self.db_path,read_only=True) as con:
            query = '''
            SELECT *
            FROM staging.wiski
            LIMIT 0'''
            df = con.execute(query).fetch_df().to_csv(self.folderpath.joinpath('wiski_template.csv'), index=False)
        return df

    def get_outlets(self,model_name):
        with duckdb.connect(self.db_path,read_only=True) as con:
            query = '''
            SELECT *
            FROM outlets.station_reach_pairs
            WHERE repository_name = ?
            ORDER BY outlet_id'''
            df = con.execute(query,[model_name]).fetch_df()
        return df
    
    def get_station_ids(self,station_origin = None):
        with duckdb.connect(self.db_path,read_only=True) as con:
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
                df = con.execute(query,[station_origin]).fetch_df()
        
        return df['station_id'].to_list()
    

    def get_observation_data(self,station_ids,constituent,agg_period = None):
        with duckdb.connect(self.db_path,read_only=True) as con:
            query = '''
            SELECT *
            FROM analytics.observations
            WHERE station_id IN ? AND constituent = ?'''
            df = con.execute(query,[station_ids,constituent]).fetch_df()
        
        unit = UNIT_DEFAULTS[constituent]
        agg_func = AGG_DEFAULTS[unit]

        df.set_index('datetime',inplace=True)
        df.attrs['unit'] = unit
        df.attrs['constituent'] = constituent
        if agg_period is not None:
            df = df[['value']].resample(agg_period).agg(agg_func)
            df.attrs['agg_period'] = agg_period

        df.rename(columns={'value': 'observed'}, inplace=True) 
        return df.dropna(subset=['observed'])
    
    def get_outlet_data(self,outlet_id,constituent,agg_period = 'D',to_csv = False):
        with duckdb.connect(self.db_path,read_only=True) as con:
            query = '''
            SELECT *
            FROM analytics.outlet_observations_with_flow
            WHERE outlet_id = ? AND constituent = ?'''
            df = con.execute(query,[outlet_id,constituent]).fetch_df()    

        unit = UNIT_DEFAULTS[constituent]
        agg_func = AGG_DEFAULTS[unit]

        df.set_index('datetime',inplace=True)
        df.attrs['unit'] = unit
        df.attrs['constituent'] = constituent
        if agg_period is not None:
            df = df[['value','flow_value','baseflow_value']].resample(agg_period).agg(agg_func)
            df.attrs['agg_period'] = agg_period

        df.rename(columns={'value': 'observed',
                           'flow_value': 'observed_flow',
                           'baseflow_value': 'observed_baseflow'}, inplace=True) 
        return df.dropna(subset=['observed'])
    
    def get_station_data(self,station_id,station_origin, to_csv = False):
        with duckdb.connect(self.db_path,read_only=True) as con:
            query = '''
            SELECT *
            FROM analytics.observations
            WHERE station_id = ? AND station_origin = ?'''
            df = con.execute(query,[station_id,station_origin]).fetch_df()    

        if to_csv:
            df.to_csv(self.folderpath.joinpath(f'{station_id}.csv'), index=False)
        return df

    def get_raw_data(self,station_id,station_origin, to_csv = False):
        with duckdb.connect(self.db_path,read_only=True) as con:
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
                raise ValueError(f'Station origin {station_origin} not recognized. Valid options are equis or wiski.')
            
            df = con.execute(query,[station_id]).fetch_df()    

        if to_csv:
            df.to_csv(self.folderpath.joinpath(f'{station_id}_raw.csv'), index=False)
        return df



# class database():
#     def __init__(self,db_path):
#         self.dbm = MonitoringDatabase(db_path)
        
    
#     def get_timeseries(self,station_ds, constituent,agg_period):      
#         validate_constituent(constituent)
#         unit = UNIT_DEFAULTS[constituent]
#         agg_func = AGG_DEFAULTS[unit]
#         return odm.get_timeseries(station_id,constituent)

    
#     def get_samples(self,station_ds, constituent,agg_period):
#         validate_constituent(constituent)
#         unit = UNIT_DEFAULTS[constituent]
#         agg_func = AGG_DEFAULTS[unit]
#         return odm.get_sample(station_id,constituent)

#     def get_samples_and_timeseries(self,station_ds, constituent,agg_period)
        
