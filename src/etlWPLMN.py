# -*- coding: utf-8 -*-
"""
Created on Tue Oct  3 08:04:49 2023

@author: mfratki
"""



import pandas as pd
from hspf_tools.calibrator.WISKI import pyWISK
import time


PARAMETERS_MAP={'5004':'TP Load',
                '5005':'TP Conc',
                '5014':'TSS Load',
                '5015':'TSS Conc',
                '5024':'N Load',
                '5025':'N Conc',
                '5034':'OP Load',
                '5035':'OP Conc',
                '5044':'TKN Load',
                '5045':'TKN Conc'}#,
                #'262' :'Flow'}

def download(station_nos):
    
   
    wiski = pyWISK()
    ts_ids = wiski.get_ts_ids(station_nos = station_nos,
                      stationgroup_id = '1319204',
                      stationparameter_no = list(PARAMETERS_MAP.keys()),
                      ts_name = ['20.Day.Mean'])
    
    flow_ts_ids = wiski.get_ts_ids(station_nos = station_nos,
                      stationgroup_id = '1319204',
                      stationparameter_no = '262',
                      ts_name = ['20.Day.Mean.Archive'])
    
    ts_ids = pd.concat([ts_ids,flow_ts_ids])
    
    if len(ts_ids) == 0:
        print('No WPLMN Sites Available')
        return pd.DataFrame() 
    
    dfs = []
    for ts_id in ts_ids['ts_id']:
        dfs.append(wiski.get_ts(ts_id))
        time.sleep(1)
    
    return pd.concat(dfs)

def transform(data):
    # Constants related to the WPLMN data in the Kisters database
    #UNITS = {'Conc':'5','Mass':'4'}
    #PARAMETER_NOS = ['5004','5005','5014','5015','5024','5025','5034','5035','5044','5045','262']
    NO_DATA_CODES = [255,151,-1]
    #MODELED_CODES = [34,2,30,31,32,70,140,80,15,10,20,27,180,35,141]
    #MEASURED_CODES = [3]
    
    data['datetime'] = pd.to_datetime(data['Timestamp'])
    #data['date'] = data['datetime'].dt.date
    #data['time'] = pd.NA
    data = data.loc[~data['Quality Code'].isin(NO_DATA_CODES),:]
    data.rename(columns = {'Value': 'value',
                           'stationparameter_name': 'variable',
                           'station_name': 'station_name',
                           'station_no': 'station_id',
                           'Quality Code': 'quality_id',
                           'ts_unitsymbol':'unit'},inplace = True)
    data['constituent'] = data['stationparameter_no'].map({'5005': 'TP',
                                                           '5004': 'TP',
                                                           '5014': 'TSS',
                                                           '5015': 'TSS',
                                                           '5024': 'N',
                                                           '5025': 'N',
                                                           '5034': 'OP',
                                                           '5035': 'OP',
                                                           '5044': 'TKN',
                                                           '5045': 'TKN',
                                                           '262' : 'Q'})
    data = data.loc[:,['datetime','value','variable','unit','quality_id','station_id','station_name','constituent']]
    
    data['unit'].replace('ft³/s','cfs',inplace=True)
    data['source'] = 'wplmn'
    return data
    
    # data['Timestamp'] = pd.to_datetime(data['Timestamp']).dt.date
    # data.set_index('Timestamp',drop=True,inplace = True)
    # data['Qc Desc'] = 'modeled point'
    # data.loc[data['Quality Code'] == 3,'Qc Desc'] = 'discrete sample point'
    # data.loc[data['Quality Code'].isin(NO_DATA_CODES),'Qc Desc'] = 'missing data'
    # data.rename(columns = {'Timestamp': 'datetime',
    #                             'Value': 'value',
    #                             'stationparameter_name': 'variable',
    #                             'station_name': 'station_name',
    #                             'station_no': 'station_id'},inplace = True)
    # data['Var Des'] = data['stationparameter_no'].map(PARAMETERS_MAP)

    return data

def load(data,file_path):
    data.to_csv(file_path)