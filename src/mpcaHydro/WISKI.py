import pandas as pd
from mpcaHydro import pywisk
import baseflow as bf
import time


#%% Define Selectors and Maps
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

DATA_CODES = [1,3,10,12,15,20,29,30,31,32,34,45,46,47,48,49]


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
                                       'unit': '08.Provisional.Edited'}},
                    'TRB':{'Internal':{'daily':'20.Day.Mean',
                                    'unit': '09.Archive'},
                        'External': {'daily': '20.Day.Mean',
                                    'unit': '08.Provisional.Edited'}}}



CONSTITUENT_NAME_NO = {'Q'  :['262*'],#,'263'],
                       'WT' :['450*', '451*'], # '450.42','451.42'],
                       'OP' :['863*'],
                       'DO' :['866*'],
                       'TRB':['811*'],
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

VALID_CONSTITUENTS = ['Q','WT','OP','DO','TP','TSS','N','TKN','TRB']

def test_connection():
    '''
    Test connection to WISKI database.
    '''
    return pywisk.test_connection()

def download(station_ids: list, start_year: int = 1996, end_year: int = 2030,wplmn: bool = False):
    '''
    Fetch data for given station IDs from WISKI database using the KISTERS API.
    '''
    dfs = [pd.DataFrame()]
    for station_id in station_ids:
        if not isinstance(station_id,str):
            raise ValueError(f'Station ID {station_id} is not a string')
        print('Downloading Timeseries Data')
        df = pd.concat([_download(constituent,station_id,start_year,end_year,wplmn) for constituent in VALID_CONSTITUENTS])

        if not df.empty:
            dfs.append(df)
    df = pd.concat(dfs)

    station_metadata = pywisk.get_stations(station_no = station_ids,returnfields = ['stationgroup_id'])
    if any(station_metadata['stationgroup_id'].isin(['1319204'])):
        df['wplmn_flag'] = 1
    else:
        df['wplmn_flag'] = 0
    print('Done!')
    
    return df

def _download(constituent,station_nos,start_year = 1996,end_year = 2030,wplmn = False):

    if station_nos[0] == 'E':
        ts_names = TS_NAME_SELECTOR[constituent]['External']
    else:
        ts_names =TS_NAME_SELECTOR[constituent]['Internal']
    
    if wplmn:
        constituent_nos = CONSTITUENT_NAME_NO_WPLMN[constituent]
    else:
        constituent_nos = CONSTITUENT_NAME_NO[constituent]
    
    if constituent_nos is not None:
        ts_ids = pywisk.get_ts_ids(station_nos = station_nos,
                            stationparameter_no = constituent_nos,
                            ts_name = ts_names['unit'])
        
        if ts_ids.empty:
            ts_ids = pywisk.get_ts_ids(station_nos = station_nos,
                                stationparameter_no = constituent_nos,
                                ts_name = ts_names['daily'])
            if ts_ids.empty:
                return pd.DataFrame()    
        
        df = convert_to_df(ts_ids['ts_id'],start_year,end_year)

        if df.empty:
            return pd.DataFrame()    
    else:
        df = pd.DataFrame()
    return df


def download_chunk(ts_id,start_year = 1996,end_year = 2030, interval = 4, as_json = False):
    frames = [pd.DataFrame()]

    for start in range(start_year,end_year,interval):
        end = int(start + interval-1)
        if end > end_year:
            end = end_year
        df = pywisk.get_ts(ts_id,start_date = f'{start}-01-01',end_date = f'{end}-12-31',as_json = as_json)
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


def discharge(station_nos,start_year = 1996,end_year = 2030):
    return _download('Q',station_nos,start_year,end_year)


def temperature(station_nos,start_year = 1996,end_year = 2030):
    return _download('WT',station_nos,start_year,end_year)


def orthophosphate(station_nos,start_year = 1996,end_year = 2030):
    return _download('OP',station_nos,start_year,end_year)

def dissolved_oxygen(station_nos,start_year = 1996,end_year = 2030):
    return _download('DO',station_nos,start_year,end_year)

def nitrogen(station_nos,start_year = 1996,end_year = 2030):
    return _download('N',station_nos,start_year,end_year)

def total_suspended_solids(station_nos,start_year = 1996,end_year = 2030):
    return _download('TSS',station_nos,start_year,end_year)

def total_phosphorous(station_nos,start_year = 1996,end_year = 2030):
    return _download('TP',station_nos,start_year,end_year)

def tkn(station_nos,start_year = 1996,end_year = 2030):
    return _download('TKN',station_nos,start_year,end_year)



def convert_units(df):
    '''
    Convert units to standard units
    '''
    # Convert units
    #Water temperature``
    df.loc[:,'ts_unitsymbol'] = df['ts_unitsymbol'].str.lower()
    df.replace({'ts_unitsymbol':'°c'},'degf',inplace = True)
    df.loc[df['ts_unitsymbol'] == 'degf','Value'] = df.loc[df['ts_unitsymbol'] == 'degf','Value'].apply(lambda x: (x*9/5)+32)

    # Convert kg to lb
    df.loc[df['ts_unitsymbol'] == 'kg','Value'] = df.loc[df['ts_unitsymbol'] == 'kg','Value'].apply(lambda x: (x*2.20462))
    df.replace({'ts_unitsymbol':'kg'},'lb',inplace=True)

    # rename ft3/s to cfs
    df.replace({'ts_unitsymbol':'ft³/s'},'cfs',inplace=True)
    return df


def normalize_columns(df):
    '''
    Normalize column names and units
    '''
    # Map parameter numbers to constituent names
    df['constituent'] = df['parametertype_id'].map(PARAMETERTYPE_MAP)
    
    df.rename(columns={
        'station_no':'station_id',
        'Timestamp':'datetime',
        'Value':'value',
        'ts_unitsymbol':'unit',
        'Quality Code':'quality_code',
        'Quality Code Name':'quality_code_name'}, inplace=True)
    return df
    


def filter_quality_codes(df, data_codes):
    '''
    Filter dataframe by valid quality codes
    '''
    return df.loc[df['quality_code'].isin(data_codes)]

def average_results(df):
    df['datetime'] = pd.to_datetime(df.loc[:,'datetime'])
    df['datetime'] = df['datetime'].dt.round('h')
    return df.groupby(['station_id', 'datetime', 'constituent', 'unit']).agg(value=('value', 'mean')).reset_index()
    # Convert units


def calculate_baseflow(df, method = 'Boughton'):
    dfs = [df]
    for station_id in df['station_id'].unique():
        df_station = df.query(f'constituent == "Q" & station_id == "{station_id}"')[['datetime', 'value']].copy().set_index('datetime')
        if df_station.empty:
            continue
        else:
            df_baseflow = bf.single(df_station['value'], area = None, method = method,return_kge = False)[0][method]
            
            df_baseflow = pd.DataFrame(
                {
                    "station_id": station_id,
                    "station_origin": 'wiski',
                    "datetime": df_baseflow.index,
                    "value": df_baseflow.values,
                    "constituent": 'QB',
                    "unit": 'cfs',
                }
            )
            dfs.append(df_baseflow)
    
    return pd.concat(dfs)


def normalize(df):
    '''
    Standardize raw WISKI data into standardized format without transformations.
    The standardized format includes normalized column names and units.
    ---
    Parameters:
    df (pandas.DataFrame): Raw WISKI data
    Returns:
    pandas.DataFrame: Normalized WISKI data
    '''

    df = convert_units(df)
    df = normalize_columns(df)
    return df

def transform(df, filter_qc_codes = True, data_codes = None, baseflow_method = 'Boughton'):
    '''
    Transform normalized WISKI data into standardized format
    '''
    df = normalize(df)
    if filter_qc_codes:
        if data_codes is None:
            data_codes = DATA_CODES
        df = filter_quality_codes(df, data_codes)
    df = average_results(df)
    df = calculate_baseflow(df, method = baseflow_method)
    df['station_origin'] = 'wiski'
    return df



