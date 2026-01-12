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
#from hspf_tools.calibrator import etlWISKI, etlSWD


#stations_wiski = gpd.read_file('C:/Users/mfratki/Documents/GitHub/pyhcal/src/pyhcal/data/stations_wiski.gpkg')


_stations_wiski = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_wiski.gpkg'))
stations_wiski = _stations_wiski.dropna(subset='opnids')[['station_id','true_opnid','opnids','comments','modeled','repository_name','wplmn_flag']]
stations_wiski['source'] = 'wiski'
_stations_equis = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_EQUIS.gpkg'))
stations_equis = _stations_equis.dropna(subset='opnids')[['station_id','true_opnid','opnids','comments','modeled','repository_name']]
stations_equis['source'] = 'equis'
stations_equis['wplmn_flag'] = 0


DB_PATH = str(Path(__file__).resolve().parent/'data\\outlets.duckdb')

MODL_DB = pd.concat([stations_wiski,stations_equis])
MODL_DB['opnids'] = MODL_DB['opnids'].str.strip().replace('',pd.NA)
MODL_DB = MODL_DB.dropna(subset='opnids')
MODL_DB = MODL_DB.drop_duplicates(['station_id','source']).reset_index(drop=True)

def _reload():
    global _stations_wiski, stations_wiski, _stations_equis, stations_equis, MODL_DB
    _stations_wiski = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_wiski.gpkg'))
    stations_wiski = _stations_wiski.dropna(subset='opnids')[['station_id','true_opnid','opnids','comments','modeled','repository_name','wplmn_flag']]
    stations_wiski['source'] = 'wiski'
    _stations_equis = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_EQUIS.gpkg'))
    stations_equis = _stations_equis.dropna(subset='opnids')[['station_id','true_opnid','opnids','comments','modeled','repository_name']]
    stations_equis['source'] = 'equis'
    stations_equis['wplmn_flag'] = 0

    MODL_DB = pd.concat([stations_wiski,stations_equis])
    MODL_DB['opnids'] = MODL_DB['opnids'].str.strip().replace('',pd.NA)
    MODL_DB = MODL_DB.dropna(subset='opnids')
    MODL_DB = MODL_DB.drop_duplicates(['station_id','source']).reset_index(drop=True)


def get_model_db(model_name: str):
    return MODL_DB.query('repository_name == @model_name')

def split_opnids(opnids: list):
    return [abs(int(float(j))) for i in opnids for j in i]

def valid_models():
    return MODL_DB['repository_name'].unique().tolist()

def wplmn_station_opnids(model_name):
    opnids = MODL_DB.query('repository_name == @model_name and wplmn_flag == 1 and source == "wiski"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def wiski_station_opnids(model_name):
    opnids = MODL_DB.query('repository_name == @model_name and source == "wiski"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def equis_station_opnids(model_name):
    opnids = MODL_DB.query('repository_name == @model_name and source == "equis"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def station_opnids(model_name):
    opnids = MODL_DB.query('repository_name == @model_name')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def equis_stations(model_name):
    return MODL_DB.query('repository_name == @model_name and source == "equis"')['station_id'].tolist()

def wiski_stations(model_name):
    return MODL_DB.query('repository_name == @model_name and source == "wiski"')['station_id'].tolist()

def wplmn_stations(model_name):
    return MODL_DB.query('repository_name == @model_name and wplmn_flag == 1 and source == "wiski"')['station_id'].tolist()

def outlets(model_name):
    return [group for _, group in MODL_DB.query('repository_name == @model_name').groupby(by = ['opnids','repository_name'])]

def outlet_stations(model_name):
    return [group['station_id'].to_list() for _, group in MODL_DB.query('repository_name == @model_name').groupby(by = ['opnids','repository_name'])]

def _split_opnids(opnids: list):
    return [int(float(j)) for i in opnids for j in i]

def connect(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)


def init_db(db_path: str,reset: bool = False):
    """
    Initialize the DuckDB database: create staging and analytics schemas
    """
    db_path = Path(db_path)
    if reset and db_path.exists():
        db_path.unlink()

    with connect(db_path.as_posix()) as con:
        con.execute(OUTLETS_SCHEMA)



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

# constructors:
def build_outlet_db(db_path: str = None):
    if db_path is None:
        db_path = DB_PATH
    init_db(db_path,reset=True)
    with connect(db_path) as con:
        build_outlets(con)


def build_outlets(con, model_name: str = None):
    if model_name is not None:
        modl_db = get_model_db(model_name)
    else:
        modl_db = MODL_DB

    for index, (_, group) in enumerate(modl_db.drop_duplicates(['station_id','source']).groupby(by = ['opnids','repository_name'])):
        repo_name = group['repository_name'].iloc[0]    
        add_outlet(con, outlet_id = index, outlet_name = None, repository_name = repo_name, notes = None)
        
        opnids = set(_split_opnids(group['opnids'].str.split(',').to_list()))

        for opnid in opnids:
            if opnid < 0:
                exclude = 1
            else:
                exclude = 0
            add_reach(con, outlet_id = index, reach_id = abs(opnid),exclude = exclude, repository_name = repo_name)

        for _, row in group.drop_duplicates(subset=['station_id', 'source']).iterrows():
            add_station(con, outlet_id = index, station_id = row['station_id'], station_origin = row['source'], true_opnid = row['true_opnid'], repository_name= repo_name, comments = row['comments'])

 
def create_outlet_schema(con, model_name : str):
    for index, (_, group) in enumerate(modl_db.outlets(model_name)):
        repo_name = group['repository_name'].iloc[0]    
        add_outlet(con, outlet_id = index, outlet_name = None, repository_name = repo_name, notes = None)
        
        opnids = set(_split_opnids(group['opnids'].str.split(',').to_list()))

        for opnid in opnids:
            if opnid < 0:
                exclude = 1
            else:
                exclude = 0
            add_reach(con, outlet_id = index, reach_id = abs(opnid),exclude = exclude, repository_name = repo_name)

        for _, row in group.drop_duplicates(subset=['station_id', 'source']).iterrows():
            add_station(con, outlet_id = index, station_id = row['station_id'], station_origin = row['source'], true_opnid = row['true_opnid'], repository_name= repo_name, comments = row['comments'])


def add_outlet(con,
               outlet_id: str,
               repository_name: str,
               outlet_name = None,
               notes = None):
    """
    Insert an outlet. repository_name is required.
    """
    con.execute(
        "INSERT INTO outlets.outlets (outlet_id, repository_name, outlet_name, notes) VALUES (?, ?, ?, ?)",
        [outlet_id, repository_name, outlet_name, notes]
    )

def add_station(con,
                outlet_id: str,
                station_id: str,
                station_origin: str,
                true_opnid: str,
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
              outlet_id: str,
              reach_id: str,
              repository_name: str,
              exclude: int = 0):
    """
    Insert a reach membership for an outlet.
    - repository_name is required and participates in the PK (reach_id, repository_name).
    - exclude = 1 to mark a reach as excluded from association views.
    """
    con.execute(
        """INSERT INTO outlets.outlet_reaches (outlet_id, reach_id, repository_name, exclude)
           VALUES (?, ?, ?, ?)""",
        [outlet_id, reach_id, repository_name, int(exclude)]
    )


OUTLETS_SCHEMA  = """-- schema.sql
-- Simple 3-table design to manage associations between model reaches and observation stations via outlets.
-- Compatible with DuckDB and SQLite.

-- Table 1: outlets
-- Represents a logical grouping that ties stations and reaches together.
CREATE TABLE IF NOT EXISTS outlets (
  outlet_id TEXT PRIMARY KEY,
  repository_name TEXT NOT NULL,
  outlet_name TEXT,
  notes TEXT             -- optional: general notes about the outlet grouping
);

-- Table 2: outlet_stations
-- One-to-many: outlet -> stations
CREATE TABLE IF NOT EXISTS outlet_stations (
  outlet_id TEXT NOT NULL,
  station_id TEXT NOT NULL,
  station_origin TEXT NOT NULL,       -- e.g., 'wiski', 'equis'
  repository_name TEXT NOT NULL,  -- repository model the station is physically located in
  true_opnid TEXT NOT NULL,           -- The specific reach the station physically sits on (optional)
  comments TEXT,             -- Per-station comments, issues, etc.
  CONSTRAINT uq_station_origin UNIQUE (station_id, station_origin),
  FOREIGN KEY (outlet_id) REFERENCES outlets(outlet_id)
);

-- Table 3: outlet_reaches
-- One-to-many: outlet -> reaches
-- A reach can appear in multiple outlets, enabling many-to-many overall.
CREATE TABLE IF NOT EXISTS outlet_reaches (
  outlet_id TEXT NOT NULL,
  reach_id TEXT NOT NULL,    -- model reach identifier (aka opind)
  repository_name TEXT NOT NULL,  -- optional: where the mapping comes from
  exclude INTEGER DEFAULT 0, -- flag to indicate if this reach should be excluded (1) or included (0)
  FOREIGN KEY (outlet_id) REFERENCES outlets(outlet_id)
);

-- Useful views:

-- View: station_reach_pairs
-- Derives the implicit many-to-many station <-> reach relationship via shared outlet_id
CREATE VIEW IF NOT EXISTS station_reach_pairs AS
SELECT
  s.outlet_id,
  s.station_id,
  s.station_origin,
  r.reach_id,
  r.exclude,
  r.repository_name,
FROM outlet_stations s
JOIN outlet_reaches r
  ON s.outlet_id = r.outlet_id;

"""  
    
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
