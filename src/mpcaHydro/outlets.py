# -*- coding: utf-8 -*-
"""
Created on Thu May  1 09:51:51 2025

@author: mfratki
"""
#import sqlite3
from pathlib import Path
import geopandas as gpd
import pandas as pd
import duckdb
from mpcaHydro.sql_loader import get_outlets_schema_sql
#from hspf_tools.calibrator import etlWISKI, etlSWD


#stations_wiski = gpd.read_file('C:/Users/mfratki/Documents/GitHub/pyhcal/src/pyhcal/data/stations_wiski.gpkg')

_stations_wiski = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_wiski.gpkg'))
stations_wiski = _stations_wiski.loc[:,['station_id','true_opnid','opnids','comments','modeled','repo_name','wplmn_flag']]
stations_wiski['source'] = 'wiski'
_stations_equis = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_EQUIS.gpkg'))
stations_equis = _stations_equis.loc[:,['station_id','true_opnid','opnids','comments','modeled','repo_name']]
stations_equis['source'] = 'equis'
stations_equis['wplmn_flag'] = 0




DB_PATH = str(Path(__file__).resolve().parent/'data\\outlet.duckdb')

MODL_DB = pd.concat([stations_wiski,stations_equis])
MODL_DB['opnids'] = MODL_DB['opnids'].str.strip().replace('',pd.NA)
MODL_DB = MODL_DB.dropna(subset='opnids')
MODL_DB = MODL_DB.drop_duplicates(['station_id','source']).reset_index(drop=True)

def _reload():
    global _stations_wiski, stations_wiski, _stations_equis, stations_equis, MODL_DB
    _stations_wiski = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_wiski.gpkg'))
    stations_wiski = _stations_wiski.loc[:,['station_id','true_opnid','opnids','comments','modeled','repo_name','wplmn_flag']]
    stations_wiski['source'] = 'wiski'
    _stations_equis = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_EQUIS.gpkg'))
    stations_equis = _stations_equis.loc[:,['station_id','true_opnid','opnids','comments','modeled','repo_name']]
    stations_equis['source'] = 'equis'
    stations_equis['wplmn_flag'] = 0


    MODL_DB = pd.concat([stations_wiski,stations_equis])
    MODL_DB['opnids'] = MODL_DB['opnids'].str.strip().replace('',pd.NA)
    MODL_DB = MODL_DB.dropna(subset='opnids')
    MODL_DB = MODL_DB.drop_duplicates(['station_id','source']).reset_index(drop=True)


def split_opnids(opnids: list):
    return [int(float(j)) for i in opnids for j in i]

def get_model_db(model_name: str):
    return MODL_DB.query('repo_name == @model_name')

def valid_models():
    return MODL_DB['repo_name'].unique().tolist()

def equis_stations(model_name):
    return _stations_equis.query('repo_name == @model_name')['station_id'].tolist()

def wiski_stations(model_name):
    return _stations_wiski.query('repo_name == @model_name')['station_id'].tolist()

def wplmn_stations(model_name):
    return MODL_DB.query('repo_name == @model_name and wplmn_flag == 1 and source == "wiski"')['station_id'].tolist()

def wplmn_station_opnids(model_name):
    opnids = MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and wplmn_flag == 1 and source == "wiski"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def wiski_station_opnids(model_name):
    opnids = MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == "wiski"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def equis_station_opnids(model_name):
    opnids = MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == "equis"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def mapped_station_opnids(station_id, station_origin):
    opnids = MODL_DB.dropna(subset=['opnids']).query('station_id == @station_id and source == @station_origin')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def mapped_stations(model_name,station_origin):
    assert(station_origin in ['wiski', 'equis'])
    return MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == @station_origin')['station_id'].tolist()
    
def mapped_equis_stations(model_name):
    return MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == "equis"')['station_id'].tolist()

def mapped_wiski_stations(model_name):
    return MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == "wiski"')['station_id'].tolist()

def outlets(model_name):
    return [group for _, group in MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name').groupby(by = ['opnids','repo_name'])]

def outlet_stations(model_name):
    return [group['station_id'].to_list() for _, group in MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name').groupby(by = ['opnids','repo_name'])]


def connect(db_path, read_only=True):
    #Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path,read_only=read_only)


def init_db(db_path: str,reset: bool = False):
    """
    Initialize the DuckDB database: create staging and analytics schemas
    """
    db_path = Path(db_path)
    if reset and db_path.exists():
        db_path.unlink()
    with connect(db_path.as_posix(),False) as con:
        con.execute(get_outlets_schema_sql())



# Accessors:
def get_outlets_by_model(model_name: str):
    with connect(DB_PATH) as con:
        df = con.execute(
            """
            SELECT r.*
            FROM outlets.station_reach_pairs r
            WHERE r.repository_name = ?
            """,
            [model_name]
        ).fetchdf()
    return df

def get_outlets_by_reach(reach_id: int, model_name: str):
    """
    Return all outlet rows for outlets that include the given reach_id in the given model_name.
    """
    with connect(DB_PATH) as con:
        df = con.execute(
            """
            SELECT r.*
            FROM outlets.station_reach_pairs r
            WHERE r.reach_id = ? AND r.repository_name = ?
            """,
        [reach_id, model_name]).fetchdf()
    return df
 
def get_outlets_by_station(station_id: str, station_origin: str):
    """
    Return all outlet rows for outlets that include the given reach_id in the given model_name.
    """
    with connect(DB_PATH) as con:

        df = con.execute(
        """
        SELECT r.*
        FROM outlets.station_reach_pairs r
        WHERE r.station_id = ? AND r.station_origin = ?
        """,
        [station_id, station_origin]).fetchdf()
    return df



class OutletGateway:
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.db_path = DB_PATH
        self.modl_db = get_model_db(model_name)

    # Legacy methods to access functions
    def wplmn_station_opnids(self):
        return wplmn_station_opnids(self.model_name)

    def wiski_station_opnids(self):
        return wiski_station_opnids(self.model_name)

    def equis_station_opnids(self):
        return equis_station_opnids(self.model_name)

    def station_opnids(self):
        return station_opnids(self.model_name)

    def equis_stations(self):
        return equis_stations(self.model_name)

    def wiski_stations(self):
        return wiski_stations(self.model_name)

    def wplmn_stations(self):
        return wplmn_stations(self.model_name)

    def outlets(self):
        return outlets(self.model_name)

    def outlet_stations(self):
        return outlet_stations(self.model_name)

    # Accessors for outlets
    def get_outlets(self):
        return get_outlets_by_model(self.model_name)

    def get_outlets_by_reach(self, reach_id: int):
        return get_outlets_by_reach(reach_id, self.model_name)

    def get_outlets_by_station(self, station_id: str, station_origin: str):
        assert(station_id in self.wiski_stations() + self.equis_stations()), f"Station ID {station_id} not found in model {self.model_name}"
        return get_outlets_by_station(station_id, station_origin)

# constructors:
def build_outlet_db(db_path: str = None):
    if db_path is None:
        db_path = DB_PATH
    init_db(db_path,reset=True)
    with connect(db_path,False) as con:
        build_outlets(con)


def build_outlets(con, model_name: str = None):
    if model_name is not None:
        modl_db = get_model_db(model_name)
    else:
        modl_db = MODL_DB

    for index, (_, group) in enumerate(modl_db.drop_duplicates(['station_id','source']).groupby(by = ['opnids','repo_name'])):
        repo_name = group['repo_name'].iloc[0]    
        add_outlet(con, outlet_id = index, outlet_name = None, repository_name = repo_name, notes = None)
        
        opnids = set(split_opnids(group['opnids'].str.split(',').to_list()))

        for opnid in opnids:
            add_reach(con, outlet_id = index, reach_id = int(opnid), repository_name = repo_name)

        for _, row in group.drop_duplicates(subset=['station_id', 'source']).iterrows():
            add_station(con, outlet_id = index, station_id = row['station_id'], station_origin = row['source'], true_opnid = row['true_opnid'], repository_name= repo_name, comments = row['comments'])

 
def create_outlet_schema(con, model_name : str):
    for index, (_, group) in enumerate(outlets(model_name)):
        repo_name = group['repo_name'].iloc[0]    
        add_outlet(con, outlet_id = index, outlet_name = None, repository_name = repo_name, notes = None)
        
        opnids = set(split_opnids(group['opnids'].str.split(',').to_list()))

        for opnid in opnids:
            add_reach(con, outlet_id = index, reach_id = int(opnid), repository_name = repo_name)

        for _, row in group.drop_duplicates(subset=['station_id', 'source']).iterrows():
            add_station(con, outlet_id = index, station_id = row['station_id'], station_origin = row['source'], true_opnid = row['true_opnid'], repository_name= repo_name, comments = row['comments'])


def add_outlet(con,
               outlet_id: int,
               repository_name: str,
               outlet_name = None,
               notes = None):
    """
    Insert an outlet. repository_name is required.
    """
    con.execute(
        "INSERT INTO outlets.outlet_groups (outlet_id, repository_name, outlet_name, notes) VALUES (?, ?, ?, ?)",
        [outlet_id, repository_name, outlet_name, notes]
    )

def add_station(con,
                outlet_id: int,
                station_id: int,
                station_origin: str,
                true_opnid: int,
                repository_name: str,
                comments = None):
    """
    Insert a station membership for an outlet.
    Constraints:
    - PRIMARY KEY (station_id, station_origin): unique per origin across all outlets.
    - true_opnid and true_opnid_repository_name are required per schema.
    """
    con.execute(
        """INSERT INTO outlets.outlet_stations
           (outlet_id, station_id, station_origin, true_opnid, repository_name, comments)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [outlet_id, station_id, station_origin, true_opnid, repository_name, comments]
    )

def add_reach(con,
              outlet_id: int,
              reach_id: int,
              repository_name: str):
    """
    Insert a reach membership for an outlet.
    - repository_name is required and participates in the PK (reach_id, repository_name).
    """
    con.execute(
        """INSERT INTO outlets.outlet_reaches (outlet_id, reach_id, repository_name)
           VALUES (?, ?, ?)""",
        [outlet_id, reach_id, repository_name]
    )


    
#row = modl_db.MODL_DB.iloc[0]

#info = etlWISKI.info(row['station_id'])

#modl_db.MODL_DB.query('source == "equis"')

# outlet_dict = {'stations': {'wiski': ['E66050001'],
#                'equis': ['S002-118']},
#                'reaches': {'Clearwater': [650]}
                      



# station_ids = ['S002-118']
# #station_ids = ['E66050001']
# reach_ids = [650]
# flow_station_ids =  ['E66050001']
