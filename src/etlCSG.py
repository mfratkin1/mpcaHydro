# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 14:13:23 2023

@author: mfratki
"""

import pandas as pd
# import geopandas as gpd


CONSITUENT_MAP = {'Water Temp. (C)': 'WT',
                 'Discharge (cfs)': 'Q',
                 'DO (mg/L)': 'DO'
    }

def download(station_no):
    # save_path = Path(save_path)
    # file_path = save_path.joinpath('csg.csv')

    station = station_no[1:]
    df = pd.read_csv(f'https://maps2.dnr.state.mn.us/cgi-bin/csg.cgi?mode=dump_hydro_data_as_csv&site={station}&startdate=1996-1-1&enddate=2050-1-1')
    df['station_id'] = station_no

    return df



    # def process(df):
    #     df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    #     df.set_index('Timestamp',inplace=True)
    #     value_variables = [column for column in df.columns if (column not in ['Site','Timestamp','station_no']) & ~(column.endswith('Quality'))]
        
    #     test = df[value_variables].resample(rule='1H', kind='interval').mean().dropna()
    #     df = df['Value'].resample(rule='1H', kind='interval').mean().to_frame()
        
def transform(data):
    
    
    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data['Timestamp'].dt.tz_localize('UTC')
    
    id_columns = ['Timestamp','station_id']
    quality_columns = ['Water Temp. (C) Quality',
                     'Discharge (cfs) Quality',
                     'DO (mg/L) Quality']
    
    value_columns = ['Water Temp. (C)',
                     'Discharge (cfs)',
                     'DO (mg/L)']

    value_columns = [column for column in data.columns if column in value_columns]
    quality_columns = [column for column in data.columns if column in quality_columns]


    
    data_melt = pd.melt(data,col_level=0,id_vars = id_columns,value_vars = value_columns)
    data_melt['Quality'] = pd.melt(data,col_level=0,id_vars = id_columns,value_vars = quality_columns)['value']

    data_melt.rename(columns = {'Timestamp': 'datetime',
                                'Value': 'value',
                                'stationparameter_name': 'variable',
                                'station_no': 'station_id',
                                'Quality' : 'quality'},inplace = True)
    
    data_melt['unit'] = data_melt['variable'].map({'Water Temp. (C)' : 'C',
                                                   'Discharge (cfs)' : 'cfs',
                                                   'DO (mg/L)' : 'mg/L'})   
    
    data_melt['constituent'] = data_melt['variable'].map({'Water Temp. (C)' : 'WT',
                                                          'Discharge (cfs)' : 'Q',
                                                          'DO (mg/L)' : 'DO'})
  
    data_melt.dropna(subset = 'value',inplace=True)
    data['source'] = 'csg'
    return data_melt





def load(data,file_path):
    
    data.to_csv(file_path)



        