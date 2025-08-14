# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 14:13:23 2023

@author: mfratki
"""

import pandas as pd
#from hspf_tools.orm.models import Station
# import geopandas as gpd




                   
CONSTITUENT_MAP = {'Total suspended solids':'TSS',
                   'Total solids': 'TSS',
                   'Solids, Suspended' : 'TSS',
                   'Solids, Total Suspended' : 'TSS',
                  'Residue - nonfilterable (TSS)': 'TSS',
                 'Kjeldahl nitrogen as N': 'TKN',
                 'Inorganic nitrogen (nitrate and nitrate) as N': 'N',
                 'Nitrogen, Total Kjeldahl (TKN) as N': 'TKN',
                 'Nitrate + Nitrite Nitrogen, Total as N': 'N',
                 'Nitrate/Nitrite as N (N+N) as N': 'N',
                 'Nutrient-nitrogen as N': 'N',
                 'Nitrate/Nitrite as N': 'N',
                 'Phosphorus, Total as P as P':'TP',
                 'Phosphorus, Total as P' : 'TP',
                 'Phosphorus as P': 'TP',
                 'Total Phosphorus as P': 'TP',
                 'Orthophosphate as P': 'OP',
                 'Carbonaceous biochemical oxygen demand, standard conditions': 'BOD',
                 'Chemical oxygen demand':'BOD',
                 'Biochemical oxygen demand, standard conditions': 'BOD',
                 'Chlorophyll a, corrected for pheophytin':'CHLA',
                 'Chlorophyll-A':'CHLA',
                 'Chlorophyll-a, Pheophytin Corrected':'CHLA',
                 'Flow':'Q',
                 'Temperature, water': 'WT',
                 'Dissolved oxygen': 'DO',
                 'Dissolved oxygen (DO)': 'DO',
                 'Suspended Sediment Concentration': 'SSC'}    

# station_no  = 	'S010-822'
# data = download(station_no)
# data = transform(data) 


# def download(station_nos):
#     df = pd.concat([_download(station_no) for station_no in station_nos])
#     return df
import requests

def _download(station_no):
    # Replace {station_no} in the URL with the actual station number
    url = f"https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results?stationId={station_no}&format=json"
    
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        # Parse the JSON data
        if response.json()['recordCount'] == 0:
            return pd.DataFrame(columns = response.json()['column_names'])
        else:
            return pd.DataFrame(response.json()['data'])
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None



def download(station_no):
    #df = pd.read_csv(f'https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results?stationId={station_no}&format=csv')
    df = _download(station_no)
    if df.empty:
        return df
    else:
        df['station_id'] = station_no
        return transform(df)

def info(station_no):
    #df = pd.read_csv(f'https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results?stationId={station_no}&format=csv')
    df = _download(station_no)
    df['station_id'] = station_no
    df.loc[:,'resultUnit'] = df['resultUnit'].str.lower()
    df.replace({'resultUnit':'kg'},'lb',inplace=True)
    df.replace({'resultUnit':'ug/l'},'mg/l',inplace=True)
    df.replace({'resultUnit':'deg c'},'degF',inplace=True)
    df.replace({'resultUnit':'deg c'},'degF',inplace=True)
    
    return df.drop_duplicates(subset = 'station_id')
  

# def _info(station_nos):
#     station_info = info(station_nos)
#     if station_info.empty:
#         return Station(station_nos,
#                        'equis',
#                        station_type = 'River')
#     else:             
#         return Station(station_info.iloc[0]['stationId'],
#                        'equis',
#                        station_name = station_info.iloc[0]['stationName'],
#                        station_type = 'River')
            
    

def transform(df):
    df = df.loc[df['parameter'].isin(CONSTITUENT_MAP.keys()),:]
    df['datetime'] = pd.to_datetime(list(df.loc[:,'sampleDate'] +' ' + df.loc[:,'sampleTime']))
    df = df.loc[(df['datetime'] > '1996') & (df['result'] != '(null)')]
    
    if df.empty:
        return df
    
    df['result'] = pd.to_numeric(df['result'], errors='coerce')
    df.rename(columns = {'result': 'value',
                           'parameter': 'variable',
                           'stationName': 'station_name',
                           'stationID': 'station_id',
                           'resultUnit':'unit'},inplace=True)
    
    df['constituent'] = df['variable'].map(CONSTITUENT_MAP)
    df['source'] = 'swd'
    df['quality_id'] = pd.NA
    station_name = df.iloc[0]['station_name']
    df = df.loc[:,['datetime','value','variable','unit','station_id','station_name','constituent','source']]
    
    
    df = df.astype({'value':float,
               'unit':str,
               'station_id':str,
               'station_name':str,
               'constituent':str})
    
    # convert ug to mg/l
    df.loc[:,'unit'] = df['unit'].str.lower()
    df.loc[df['unit'] == 'ug/l','value'] = df.loc[df['unit'] == 'ug/l','value']*.001
    df.loc[df['unit'] == 'kg','value'] = df.loc[df['unit'] == 'kg','value']*2.20462
    df.loc[df['unit'] == 'deg c','value'] = df.loc[df['unit'] == 'deg c','value']*9/5 + 32 # Convert celsius to faren

    df.replace({'unit':'kg'},'lb',inplace=True)
    df.replace({'unit':'ug/l'},'mg/l',inplace=True)
    df.replace({'unit':'deg c'},'degF',inplace=True)

    # df['unit'].replace('kg','lb',inplace=True)
    # df['unit'].replace('ug/l','mg/l',inplace=True)
    # df['unit'].replace('deg c','degF',inplace=True)
    df['data_type'] = 'discrete'
    df['data_format'] = 'instantaneous'
    df.set_index('datetime',drop=True,inplace=True)
    df.index = df.index.tz_localize('UTC-06:00')
    
    df.index = df.index.round('H').round('H')
    df = df.reset_index()
    df = df.groupby(['datetime','variable','unit','station_id','station_name','constituent','data_format','data_type','source']).mean()
    df = df.reset_index()
    df = df.set_index('datetime')
    df['quality_id'] = pd.NA
    df['station_name'] = station_name
    return df

def load(df,file_path):
    df.to_csv(file_path)      
    
    


# base_url = 'https://webapp.pca.state.mn.us/surface-water/search?'


# https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results?


# dataType
# geographicType
# specificGeoAreaCode
# wuType
# stationType
# stationId


        
# CONSTITUENT_MAP = {'TSS': ['Total suspended solids'],
#                 'TKN': ['Kjeldahl nitrogen as N','Nitrogen, Total Kjeldahl (TKN) as N'],
#                 'N'  :  ['Nitrate + Nitrite Nitrogen, Total as N','Nitrate/Nitrite as N (N+N) as N'],
#                 'TP' :  ['Phosphorus, Total as P as P'],
#                 'BOD': ['Carbonaceous biochemical oxygen demand, standard conditions',
#                                 'Chemical oxygen demand'],
#                 'CHLA': ['Chlorophyll a, corrected for pheophytin',
#                               'Chlorophyll-A',
#                               'Chlorophyll-a, Pheophytin Corrected'],
#                 'Q': ['Flow']}
