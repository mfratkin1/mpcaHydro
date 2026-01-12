import pandas as pd
from pathlib import Path

WISKI_EQUIS_XREF = pd.read_csv(Path(__file__).parent/'data/WISKI_EQUIS_XREF.csv')
#WISKI_EQUIS_XREF = pd.read_csv('C:/Users/mfratki/Documents/GitHub/hspf_tools/WISKI_EQUIS_XREF.csv')


def are_lists_identical(nested_list):
    # Sort each sublist
    sorted_sublists = [sorted(sublist) for sublist in nested_list]
    # Compare all sublists to the first one
    return all(sublist == sorted_sublists[0] for sublist in sorted_sublists)                                                                                               

def get_wiski_stations():
    return list(WISKI_EQUIS_XREF['WISKI_STATION_NO'].unique())

def get_equis_stations():
    return list(WISKI_EQUIS_XREF['EQUIS_STATION_ID'].unique())

def wiski_equis_alias(wiski_station_id):
    equis_ids =  list(set(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_STATION_NO'] == wiski_station_id,'WISKI_EQUIS_ID'].to_list()))
    equis_ids = [equis_id for equis_id in equis_ids if not pd.isna(equis_id)]
    if len(equis_ids) == 0:
        return []
    elif len(equis_ids) > 1:
        print(f'Too Many Equis Stations for {wiski_station_id}')
        raise 
    else:
        return equis_ids[0]

def wiski_equis_associations(wiski_station_id):
    equis_ids =  list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_STATION_NO'] == wiski_station_id,'EQUIS_STATION_ID'].unique())
    equis_ids =  [equis_id for equis_id in equis_ids if not pd.isna(equis_id)]
    if len(equis_ids) == 0:
        return []
    else:
        return equis_ids
    
def equis_wiski_associations(equis_station_id):
    wiski_ids = list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['EQUIS_STATION_ID'] == equis_station_id,'WISKI_STATION_NO'].unique())
    wiski_ids = [wiski_id for wiski_id in wiski_ids if not pd.isna(wiski_id)]
    if len(wiski_ids) == 0:
        return []
    else:
        return wiski_ids
    
def equis_wiski_alias(equis_station_id):
    wiski_ids =  list(set(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_EQUIS_ID'] == equis_station_id,'WISKI_STATION_NO'].to_list()))
    wiski_ids = [wiski_id for wiski_id in wiski_ids if not pd.isna(wiski_id)]
    if len(wiski_ids) == 0:
        return []
    elif len(wiski_ids) > 1:
        print(f'Too Many WISKI Stations for {equis_station_id}')
        raise ValueError(f'Too Many WISKI Stations for {equis_station_id}')
    else:
        return wiski_ids[0]

def _equis_wiski_associations(equis_station_ids):
    wiski_stations = [equis_wiski_associations(equis_station_id) for equis_station_id in equis_station_ids]
    if are_lists_identical(wiski_stations):
        return wiski_stations[0]
    else:
        return []
        
def _stations_by_wid(wid_no,station_origin):
    if station_origin in ['wiski','wplmn']:
        station_col = 'WISKI_STATION_NO'
    elif station_origin in ['equis','swd']:
        station_col = 'EQUIS_STATION_ID'
    else:
        raise
        
    return list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WID'] == wid_no,station_col].unique())

