# -*- coding: utf-8 -*-
"""
Created on Tue Oct  3 08:04:49 2023

@author: mfratki
"""

import pandas as pd
from mpcaHydro.WISKI import pyWISK
#from hspf_tools.orm.models import Station
import time
wiski = pyWISK()


'''
Q - Discharge - cfs/s

Constituents
WT - Water Temperature - degrees C
WL - Water Level - ft
OP - Orthophosphate - mg/L
DO - Dissolved Oxygen - mg/L
TP - Total Phosphorus - mg/L
TSS - Total Suspended Solids - mg/L
N - Nitrogen - mg/L
TKN - Total Kjeldahl Nitrogen - mg/L
CHL - Chlorophyll - ug/L
BOD - Biochemical Oxygen Demand - mg/L


load is a derived measurement

'''


PARAMETERTYPE_MAP ={'11522': 'TP',
                    '11531': 'TP',
                    '11532': 'TSS',
                    '11523': 'TSS',
                    '11526': 'N',
                    '11519': 'N',
                    '11520': 'OP',
                    '11528': 'OP',
                    '11530': 'TKN',
                    '11521': 'TKN',
                    '11500' : 'Q',
                    '11504': 'WT',
                    '11533': 'DO',
                    '11507':'WL'}
#STATIONPARAMETER_NOS = ['262*','450*','451*','863*','866*','5034' ,'5035','5005', '5004','5014' ,'5015','5024'  ,'5025','5044' ,'5045']
STATIONPARAMETER_NOS = ['262*','450*','451*','863*','866*']

DATA_CODES = [1,3,10,12,
                               15,
                               20,
                               29,
                               30,
                               31,
                               32,
                               34,
                               45,
                               46,
                               47,
                               48,
                               49]



# TS_NAME_SELECTOR = {'Q':{'Internal':['20.Day.Mean.Archive','15.Rated'],
#                          'External': ['20.Day.Mean','08.Provisional.Edited']},
#                     'WT':{'Internal':['20.Day.Mean','09.Archive'],
#                           'External': ['20.Day.Mean','08.Provisional.Edited']},
#                     'TSS':{'Internal':['20.Day.Mean','09.Archive'],
#                           'External': ['20.Day.Mean','08.Provisional.Edited']},
#                     'N':{'Internal':['20.Day.Mean','09.Archive'],
#                           'External': ['20.Day.Mean','08.Provisional.Edited']},
#                     'TKN':{'Internal':['20.Day.Mean','09.Archive'],
#                           'External': ['20.Day.Mean','08.Provisional.Edited']},
#                     'TP':{'Internal':['20.Day.Mean','09.Archive'],
#                           'External': ['20.Day.Mean','08.Provisional.Edited']},
#                     'OP':{'Internal':['20.Day.Mean','09.Archive'],
#                           'External': ['20.Day.Mean','08.Provisional.Edited']},
#                     'DO':{'Internal':['20.Day.Mean','09.Archive'],
#                           'External': ['20.Day.Mean','08.Provisional.Edited']}}


TS_NAME_SELECTOR = {'Q':{'Internal':{'daily':'20.Day.Mean.Archive',
                                     'unit': '15.Rated'},
                         'External': {'daily': '20.Day.Mean',
                                      'unit': '08.Provisional.Edited'}},
                    'WT':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},
                    'TSS':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                   
                    'N':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                    
                    'TKN':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                    
                    'TP':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                    
                    'OP':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                    
                    'DO':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}}}



CONSTITUENT_NAME_NO = {'Q'  :['262*'],#,'263'],
                       'WT' :['450*', '451*'], # '450.42','451.42'],
                       'OP' :['863*'],
                       'DO' :['866*'],
                       'TP' :None,
                       'TSS':None,
                       'N'  :None,
                       'TKN':None}

CONSTITUENT_NAME_NO_WPLMN = {'Q'  :['262*'],#,'263'],
                       'WT' :['450*', '451*'], # '450.42','451.42'],
                       'OP' :['863*','5034' ,'5035'],
                       'DO' :['866*'],
                       'TP' :['5005'  ,'5004'],
                       'TSS':['5014' ,'5015'],
                       'N'  :['5024'  ,'5025'],
                       'TKN':['5044' ,'5045']}

VALID_CONSTITUENTS = ['Q','WT','OP','DO','TP','TSS','N','TKN']

# def _info(station_nos):
#     station_info = info(station_nos)
#     if station_info.empty:
#         return Station(station_nos,
#                        'wiski',
#                        station_name = '',
#                        station_type = 'River')
#     return Station(station_info.iloc[0]['station_no'],
#                    'wiski',
#                    station_name = station_info.iloc[0]['station_name'],
#                    latitude = station_info.iloc[0]['station_latitude'],
#                    longitude = station_info.iloc[0]['station_longitude'],
#                    station_type = 'River')
        

def info(station_nos):
    ts_ids = wiski.get_ts_ids(station_nos = station_nos,
                        stationparameter_no = STATIONPARAMETER_NOS)
    ts_ids = ts_ids.drop_duplicates(subset = 'parametertype_name')
    ts_ids['constituent'] = ts_ids['parametertype_id'].map(PARAMETERTYPE_MAP)
    ts_ids = ts_ids.loc[:,['station_no','station_name','stn_HUC12','stn_EQuIS_ID','stn_AUID','constituent','parametertype_name','station_latitude','station_longitude']]
    return ts_ids    

def download(station_nos,start_year = 1996, end_year = 2030, raw = False,wplmn = False):

    
    print('Downloading Timeseries Data')
    df = pd.concat([_download(constituent,station_nos,start_year,end_year,raw,wplmn) for constituent in VALID_CONSTITUENTS])
    
    station_metadata = wiski.get_stations(station_no = station_nos,returnfields = ['stationgroup_id'])
    if any(station_metadata['stationgroup_id'].isin(['1319204'])):
        df['wplmn_flag'] = 1
    else:
        df['wplmn_flag'] = 0
    print('Done!')
    return df

def transform(data):

    data['datetime'] = pd.to_datetime(data.loc[:,'Timestamp'])
    data = data.loc[data['Quality Code'].isin(DATA_CODES)].copy()
    data.rename(columns = {'Value': 'value',
                           'stationparameter_name': 'variable',
                           'station_name': 'station_name',
                           'station_no': 'station_id',
                           'Quality Code': 'quality_id',
                           'ts_unitsymbol':'unit'},inplace = True)
    
    #if data['ts_name'].str

    
    data['constituent'] = data['parametertype_id'].map(PARAMETERTYPE_MAP)
    
    data = data.loc[:,['datetime','value','variable','unit','station_id','station_name','constituent','data_format','quality_id','interval_minutes']]
    
    
    data.loc[data['unit'] == 'kg','value'] = data.loc[data['unit'] == 'kg','value'].apply(lambda x: (x*2.20462))
    data.replace({'unit':'kg'},'lb',inplace=True)
    data.replace({'unit':'ft³/s'},'cfs',inplace=True)
    data.loc[:,'unit'] = data['unit'].str.lower()
    data.replace({'unit':'°f'},'degF',inplace = True)
    data['data_type'] = 'continuous'
    data['source'] = 'wiski'
    data.set_index('datetime',drop=True,inplace=True)
    data.index = data.index.tz_convert('UTC+06:00')
    
    
    data.index = data.index.round('h').round('h')
    data = data.reset_index()
    data = data.groupby(['datetime','variable','unit','station_id','station_name','constituent','interval_minutes','data_format','data_type','source']).mean()
    data = data.reset_index()
    data = data.set_index('datetime')
    
    
    
    
    return data

def load(data,file_path):
    data.to_csv(file_path)



def _download(constituent,station_nos,start_year = 1996,end_year = 2030, raw = False,wplmn = False):

    if station_nos[0] == 'E':
        ts_names = TS_NAME_SELECTOR[constituent]['External']
    else:
        ts_names =TS_NAME_SELECTOR[constituent]['Internal']
    
    if wplmn:
        constituent_nos = CONSTITUENT_NAME_NO_WPLMN[constituent]
    else:
        constituent_nos = CONSTITUENT_NAME_NO[constituent]
        
    ts_ids = wiski.get_ts_ids(station_nos = station_nos,
                        stationparameter_no = constituent_nos,
                        ts_name = ts_names['unit'])
    
    interval_minutes = 60
    if ts_ids.empty:
        ts_ids = wiski.get_ts_ids(station_nos = station_nos,
                            stationparameter_no = constituent_nos,
                            ts_name = ts_names['daily'])
        interval_minutes = 1440 
        if ts_ids.empty:
            return pd.DataFrame()    
    
    df = convert_to_df(ts_ids['ts_id'],start_year,end_year)

    if df.empty:
        return pd.DataFrame()    
    
    if constituent == 'WT':
        df.loc[:,'ts_unitsymbol'] = df['ts_unitsymbol'].str.lower()
        df.loc[df['ts_unitsymbol'] == '°c','Value'] = df.loc[df['ts_unitsymbol'] == '°c','Value'].apply(lambda x: (x*9/5)+32)
        df.replace({'ts_unitsymbol':'°c'},'°f',inplace=True)
        
    df['data_format'] = 'instantaneous'
    df['interval_minutes'] = interval_minutes
    if not raw: df = transform(df)
    return df


def download_chunk(ts_id,start_year = 1996,end_year = 2030, interval = 5):
    frames = [pd.DataFrame()]

    for start in range(start_year,end_year,interval):
        end = int(start + interval-1)
        if end > end_year:
            end = end_year
        df = wiski.get_ts(ts_id,start_date = f'{start}-01-01',end_date = f'{end}-12-31')
        if not df.empty: frames.append(df)
        df.index = pd.to_datetime(df['Timestamp'])
        time.sleep(.1)   
    return pd.concat(frames)

def convert_to_df(ts_ids,start_year = 1996,end_year = 2030):
    dfs = []
    for ts_id in ts_ids:
        dfs.append(download_chunk(ts_id,start_year,end_year))
        time.sleep(.1)
    df =  pd.concat(dfs)
    return df



def discharge(station_nos,start_year = 1996,end_year = 2030,raw=False):
    return _download('Q',station_nos,start_year,end_year,raw)


def temperature(station_nos,start_year = 1996,end_year = 2030,raw=False):
    return _download('WT',station_nos,start_year,end_year,raw)


def orthophosphate(station_nos,start_year = 1996,end_year = 2030,raw=False):
    return _download('OP',station_nos,start_year,end_year,raw)

def dissolved_oxygen(station_nos,start_year = 1996,end_year = 2030,raw=False):
    return _download('DO',station_nos,start_year,end_year,raw)

def nitrogen(station_nos,start_year = 1996,end_year = 2030,raw=False):
    return _download('N',station_nos,start_year,end_year,raw)

def total_suspended_solids(station_nos,start_year = 1996,end_year = 2030,raw=False):
    return _download('TSS',station_nos,start_year,end_year,raw)

def total_phosphorous(station_nos,start_year = 1996,end_year = 2030,raw=False):
    return _download('TP',station_nos,start_year,end_year,raw)

def tkn(station_nos,start_year = 1996,end_year = 2030,raw=False):
    return _download('TKN',station_nos,start_year,end_year,raw)










# def discharge(station_nos,start_year = 1996,end_year = 2030,raw=False):
#     if station_nos[0] == 'E':
#         ts_names = ['08.Provisional.Edited']
#     else:
#         ts_names = ['15.Rated']
        
#     ts_ids = wiski.get_ts_ids(station_nos = station_nos,
#                         stationparameter_no = ['262*'],
#                         ts_name = ts_names)
    
#     if ts_ids.empty:
#         return pd.DataFrame()

    
#     df = convert_to_df(ts_ids['ts_id'],start_year,end_year)

#     if df.empty:
#         df = pd.DataFrame()
#     else:
#         df['data_format'] = 'instantaneous'
#         if not raw: df = transform(df)
#     return df



# def temperature(station_nos,start_year = 1996,end_year = 2030,raw=False):
#     if station_nos[0] == 'E':
#         ts_names = ['08.Provisional.Edited']
#     else:
#         ts_names = ['09.Archive']
  
    
#     ts_ids = wiski.get_ts_ids(station_nos = station_nos,
#                       stationparameter_no = ['450*','450.42*','451*','451.42*'],
#                       ts_name = ts_names)
        
#     if ts_ids.empty:
#         return pd.DataFrame()
    
           
#     df = convert_to_df(ts_ids['ts_id'],start_year,end_year)
#     if df.empty:
#         df = pd.DataFrame()
#     else:
#         #df.loc[df['ts_unitsymbol'] == '°f','Value'] = df.loc[df['ts_unitsymbol'] == '°f','Value'].apply(lambda x: (x - 32)*5/9)
#         df.loc[df['ts_unitsymbol'] == '°c','Value'] = df.loc[df['ts_unitsymbol'] == '°c','Value'].apply(lambda x: (x*9/5)+32)

#         df['ts_unitsymbol'].replace('°c','°f',inplace=True)
#         df['data_format'] = 'instantaneous'
#         if not raw: df = transform(df)

#     return df

# def dissolved_oxygen(station_nos,start_year = 1996,end_year = 2030,raw=False):
#     if station_nos[0] == 'E':
#         ts_names = ['08.Provisional.Edited']
#     else:
#         ts_names = ['09.Archive']
  
#     ts_ids = wiski.get_ts_ids(station_nos = station_nos,
#                       stationparameter_no = ['866*'],
#                       ts_name = ts_names)
        
#     if ts_ids.empty:
#         return pd.DataFrame()
#     df = convert_to_df(ts_ids['ts_id'],start_year,end_year)
    
#     if df.empty:
#         df = pd.DataFrame()
#     else:
#         df['data_format'] = 'instantaneous'
#         if not raw: df = transform(df)
#     return df


# def orthophosphate(station_nos,start_year = 1996,end_year = 2030,raw=False):
#     ts_ids = wiski.get_ts_ids(station_nos = station_nos,
#                       stationparameter_no = ['5034','5035'],
#                       ts_name = ['20.Day.Mean'])
        
#     if ts_ids.empty:
#         return pd.DataFrame()
#     df = convert_to_df(ts_ids['ts_id'],start_year,end_year)

    
#     if df.empty:
#         df = pd.DataFrame()
#     else:
#         df['data_format'] = 'aggregated'
#         if not raw: df = transform(df)

#     return df

# def total_phosphorous(station_nos,start_year = 1996,end_year = 2030,raw=False):
  
#     ts_ids = wiski.get_ts_ids(station_nos = station_nos,
#                       stationparameter_no = ['5004','5005'],
#                       ts_name = ['20.Day.Mean'])    
#     if ts_ids.empty:
#         return pd.DataFrame()
#     df = convert_to_df(ts_ids['ts_id'],start_year,end_year)
    
#     if df.empty:
#         df = pd.DataFrame()
#     else:
#         df['data_format'] = 'aggregated'
#         if not raw: df = transform(df)

#     return df


# def total_suspended_solids(station_nos,start_year = 1996,end_year = 2030,raw=False):
  
#     ts_ids = wiski.get_ts_ids(station_nos = station_nos,
#                       stationparameter_no = ['5014','5015'],
#                       ts_name = ['20.Day.Mean'])
#     if ts_ids.empty:
#         return pd.DataFrame()
#     df = convert_to_df(ts_ids['ts_id'],start_year,end_year)
    
#     if df.empty:
#         df = pd.DataFrame()
#     else:
#         df['data_format'] = 'aggregated'
#         if not raw: df = transform(df)

#     return df


# def tkn(station_nos,start_year = 1996,end_year = 2030,raw=False):
  
#     ts_ids = wiski.get_ts_ids(station_nos = station_nos,
#                       stationparameter_no = ['5044','5045'],
#                       ts_name = ['20.Day.Mean'])
        
#     if ts_ids.empty:
#         return pd.DataFrame()
#     df = convert_to_df(ts_ids['ts_id'],start_year,end_year)
   
#     if df.empty:
#         df = pd.DataFrame()
#     else:
#         df['data_format'] = 'aggregated'
#         if not raw: df = transform(df)

#     return df


# def nitrogen(station_nos,start_year = 1996,end_year = 2030,raw=False):
  
#     ts_ids = wiski.get_ts_ids(station_nos = station_nos,
#                       stationparameter_no = ['5024','5025'],
#                       ts_name = ['20.Day.Mean'])
        
#     if ts_ids.empty:
#         return pd.DataFrame()
#     df = convert_to_df(ts_ids['ts_id'],start_year,end_year)
   
#     if df.empty:
#         df = pd.DataFrame()
#     else:
#         df['data_format'] = 'aggregated'
#         if not raw: df = transform(df)

#     return df



# station_nos = ['H57026001'] # Exception station. Flow data is from a different station upstream
# station_nos = ['H36061003']








# TS_NAMES = ['20.Day']
# wiski = pyWISK()
# #external vs internal station

# station_nos = 'E58059001'

# PARAMETERS_MAP={'5004':'TP Load',
#                 '5005':'TP Conc',
#                 '5014':'TSS Load',
#                 '5015':'TSS Conc',
#                 '5024':'N Load',
#                 '5025':'N Conc',
#                 '5034':'OP Load',
#                 '5035':'OP Conc',
#                 '5044':'TKN Load',
#                 '5045':'TKN Conc',
#                 '262*' :'Flow'}




# PARAMETERS = ['262*',
# '450*',
# '451*',
# '341*',
# '855*',
# '866*',]


# '501*',
# '502*',
# '503*',
# '504*']

# PARAMETER_NO_MAP = {'262':'Q',
#                  '263':'Q',
#                  '450':'WT',
#                  '451':'WT',
#                  '450.42':'WT',
#                  '451.42':'WT',
#                  '863':'OP',
#                  '865':'DO',
#                  '866':'DO',
#                  '867':'DO',
#                  '5005': 'TP',
#                  '5004': 'TP',
#                  '5014': 'TSS',
#                  '5015': 'TSS',
#                  '5024': 'N',
#                  '5025': 'N',
#                  '5034': 'OP',
#                  '5035': 'OP',
#                  '5044': 'TKN',
#                  '5045': 'TKN'}
