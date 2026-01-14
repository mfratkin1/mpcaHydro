# -*- coding: utf-8 -*-
"""
Created on Fri Jun  3 10:01:14 2022

@author: mfratki
"""

import pandas as pd
#from abc import abstractmethod
from pathlib import Path
from mpcaHydro import etlSWD
from mpcaHydro import equis, wiski, warehouse
from mpcaHydro import xref
from mpcaHydro import outlets
from mpcaHydro.reports import reportManager
import duckdb

AGG_DEFAULTS = {'cfs':'mean',
                'mg/l':'mean',
                'degf': 'mean',
                'lb':'sum'}

UNIT_DEFAULTS = {'Q': 'cfs',
                 'QB': 'cfs',
                 'TSS': 'mg/l',
                 'TP' : 'mg/l',
                 'OP' : 'mg/l',
                 'TKN': 'mg/l',
                 'N'  : 'mg/l',
                 'WT' : 'degf',
                 'WL' : 'ft'}


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

    def __init__(self,folderpath, oracle_user = None, oracle_password =None):
        
        self.data = {}
        self.folderpath = Path(folderpath)
        self.db_path = self.folderpath.joinpath('observations.duckdb')
        
        self.oracle_user = oracle_user
        self.oracle_password = oracle_password
        warehouse.init_db(self.db_path,reset = False)
        self.xref = xref
        self.outlets = outlets
        self.reports = reportManager(self.db_path)

    
    def connect_to_oracle(self):
        assert (self.credentials_exist(), 'Oracle credentials not found. Set ORACLE_USER and ORACLE_PASSWORD environment variables or use swd as station_origin')
        equis.connect(user = self.oracle_user, password = self.oracle_password)
    
    def credentials_exist(self):
        if (self.oracle_user is not None) & (self.oracle_password is not None):
            return True
        else:
            return False
        
    def _build_warehouse(self):
        build_warehouse(self.folderpath)

    def download_station_data(self,station_id,station_origin,overwrite=True,to_csv = False,filter_qc_codes = True, start_year = 1996, end_year = 2030,baseflow_method = 'Boughton'):
        '''
        Method to download data for a specific station and load it into the warehouse.
        
        :param self: Description
        :param station_id: Station identifier
        :param station_origin: source of station data: wiski, equis, or swd
        :param overwrite: Whether to overwrite existing data
        :param to_csv: Whether to export data to CSV
        :param filter_qc_codes: Whether to filter quality control codes
        :param start_year: Start year for data download
        :param end_year: End year for data download
        :param baseflow_method: Method for baseflow calculation
        '''
        with duckdb.connect(self.db_path,read_only=False) as con:
            if overwrite:
                warehouse.drop_station_id(con,station_id,station_origin)
                warehouse.update_views(con)

            if station_origin == 'wiski':
                df = wiski.download([station_id],start_year = start_year, end_year = end_year)
                warehouse.load_df_to_staging(con,df, 'wiski_raw', replace = overwrite)
                warehouse.load_df_to_analytics(con,wiski.transform(df,filter_qc_codes = filter_qc_codes,baseflow_method = baseflow_method),'wiski') # method includes normalization
                
            elif station_origin == 'equis':
                assert (self.credentials_exist(), 'Oracle credentials not found. Set ORACLE_USER and ORACLE_PASSWORD environment variables or use swd as station_origin')
                df = equis.download([station_id])
                warehouse.load_df_to_staging(con,df, 'equis_raw',replace = overwrite)
                warehouse.load_df_to_analytics(con,equis.transform(df),'equis')

            elif station_origin == 'swd':
                df = etlSWD.download(station_id)
                warehouse.load_df_to_staging(con,df, 'swd_raw', replace = overwrite)
                warehouse.load_df_to_analytics(con,etlSWD.transform(df),'swd')
            else:
                raise ValueError('station_origin must be wiski, equis, or swd')    
    
        with duckdb.connect(self.db_path,read_only=False) as con:
            warehouse.update_views(con)

        if to_csv:
            self.to_csv(station_id)
            
        return df
    
    def get_outlets(self):
        with duckdb.connect(self.db_path,read_only=True) as con:
            query = '''
            SELECT *
            FROM outlets.station_reach_pairs
            ORDER BY outlet_id'''
            df = con.execute(query).fetch_df()
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
    

    def get_station_data(self,station_ids,constituent,agg_period = None):
        

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
        return df
    
    def get_outlet_data(self,outlet_id,constituent,agg_period = 'D'):
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
        return df

    

    def to_csv(self,station_id,folderpath = None):
        if folderpath is None:
            folderpath = self.folderpath
        else:
            folderpath = Path(folderpath)
        df = self._load(station_id)
        if len(df) > 0:
            df.to_csv(folderpath.joinpath(station_id + '.csv'))
        else:
            print(f'No {station_id} calibration data available at Station {station_id}')
        
        df.to_csv(folderpath.joinpath(station_id + '.csv'))


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
        
