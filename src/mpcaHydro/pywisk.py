# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 16:18:03 2023

@author: mfratki
"""
from pathlib import Path
import requests
from requests.exceptions import ConnectionError, Timeout, HTTPError, RequestException
import pandas as pd
import time

CERT_PATH = str(Path(__file__).resolve().parent/'data\\wiskiweb01.pca.state.mn.us.crt')
#TODO: Use this url to make sure web service is working https://wiskiweb01.pca.state.mn.us/
class Service():
    base_url = 'https://wiskiweb01.pca.state.mn.us/KiWIS/KiWIS?'
    base_dict = {
        'datasource': '0',
        'service': 'kisters',
        'type': 'queryServices',
        'format': 'json'}
    
    def __init__(self):
        self._url = None
        self._args = None
        self.response = None
        
    def test_connection(self):
        timeout = 5 
        try:
            # Using requests.head() to fetch headers is faster than requests.get()
            # as it doesn't download the full content
            response = requests.head('https://wiskiweb01.pca.state.mn.us', timeout=timeout)
            
            # raise_for_status() raises an HTTPError for 4xx or 5xx status codes
            response.raise_for_status() 
            
            # If no exception was raised, the website is considered "up"
            return True, f"Website is UP (Status Code: {response.status_code})"
        
        except ConnectionError as e:
            # Handles DNS failures, refused connections, etc.
            return False, f"Website is DOWN (Connection Error): {e}"
        except Timeout as e:
            # Handles cases where the server takes too long to respond
            return False, f"Website is DOWN (Timeout Error): {e}"
        except HTTPError as e:
            # Handles HTTP errors like 404 Not Found, 500 Internal Server Error, etc.
            return False, f"Website is experiencing issues (HTTP Error): {e}"
        except RequestException as e:
            # Handles any other exceptions that might occur during the request
            return False, f"An unexpected error occurred: {e}"

    def _requestTypes(self):
        url = self.url({'request': 'getrequestinfo'}) 
        return requests.get(url,verify=CERT_PATH).json()[0]  
        
    def getRequests(self):
        return list(self._requestTypes()['Requests'].keys())
     
    def queryfields(self,request_type):
        return list(self._requestTypes()['Requests'][request_type]['QueryFields']['Content'].keys())
     
    def returnfields(self,request_type):
        return list(self._requestTypes()['Requests'][request_type]['Returnfields']['Content'].keys())
    
    def optionalfields(self,request_type):
        return list(self._requestTypes()['Requests'][request_type]['Optionalfields']['Content'].keys())
    
    def formats(self,request_type):
        return list(self._requestTypes()['Requests'][request_type]['Formats']['Content'].keys())
    
    def info(self,request_type):
        url = self.url({'request': 'getrequestinfo'})
        response = requests.get(url, verify=CERT_PATH)
        get_requests = response.json()    
        return get_requests[0]['Requests'].keys()
        

    def url(self,args_dict):
        args_dict = self.base_dict | args_dict
        args = []
        for k,v in args_dict.items():
            if v is None:
                continue
            elif isinstance(v,list):
                v = [str(vv) for vv in v]
                v = ','.join(v)    
            args.append(f'{k}={v}')
        args = '&'.join(args)
        
        url = self.base_url + args
        self._url = url
        return url
    
    def get_json(self,args_dict):
        # Download request
        self.response = requests.get(self.url(args_dict), verify=CERT_PATH)
        if self.response.status_code != 200:
            print('Error: ' + self.response.json()['message'])
            self.response.raise_for_status()  # raises exception when not a 2xx response

        return  self.response.json()  
    
    def df(self,args_dict):


        get_requests = self.get_json(args_dict)
        # Convert to dataframe
        if args_dict['request'] in ['getTimeseriesValues']:
            dfs = []
            for get_request in get_requests:
                df = pd.DataFrame(get_request['data'],columns = get_request['columns'].split(','))
                del get_request['data']
                del get_request['rows']
                del get_request['columns']  
                for k,v in get_request.items(): df[k] = v       
                dfs.append(df)
            df = pd.concat(dfs)
        else:
            df = pd.DataFrame(get_requests[1:], columns = get_requests[0])
        
        # print('Done!')
        return df
    
    def get(self,args):
        request_type = args['request']
        #assert(request_type in self.getRequests())
        _args = {queryfield: None for queryfield in self.queryfields(request_type)} | {optionalfield: None for optionalfield in self.optionalfields(request_type)}
        args = {**_args, **args}
        self._args = args        
        return self.df(args)
    
    def _filter(self,args):
        
        '''
        Filter for ensuring not too many values are requested and determining the proper division
        given the number of timeseries, timeseries length, and timeseries sampling interval
        '''
        'minute','hour','daily'
        
        MAX_OUTPUT = 240000 #True max output is 250,000 but giving myself a bit of a buffer
        
        
        n_timeseries = 1
        n_years = 1
        #1 timeseries for 1 year
        n_values = 60*24*365*n_timeseries*n_years
        
        if n_values < MAX_OUTPUT :
            return 0
        elif n_timeseries == 1:
            n_values/MAX_OUTPUT
                

    
'''
Potential use cases:

1. timeseries for a given ts_id
2. All timeseries for a given station
3. All timeseries for a given parameter
4. All timeseries for a given huc_id    
5. All timeseries of a given resolution 

'''

''''
Aggregate (aggregate) - Builds representative periodic values.
Attributes:	Interval (MANDATORY): HHMMSS, decadal, yearly, year, monthly, month, daily, day, hourly, hour
Aggregation Types (MANDATORY LIST): min, max, mean, average, total, counts, perc-#
Returnfields:	Count
Interpolation Type
Average
Quality Code
Mean
Total
Maximum
Minimum
Timestamp
P#
Examples:	aggregate(daily~total)
aggregate(yearly~mean~min~max)
aggregate(hourly~perc-25~perc-75)
'''

VALID_AGGREGATION_TYPES = ['min', 'max', 'mean', 'average', 'total', 'counts']
VALID_INTERVALS = ['decadal', 'yearly', 'year', 'monthly', 'month', 'daily', 'day', 'hourly', 'hour']
SERVICE = Service()

def construct_aggregation(interval, aggregation_type):
    validate_interval(interval)
    validate_aggregation_type(aggregation_type) 
    return f'aggregate({interval}~{aggregation_type})'

def validate_aggregation_type(aggregation_type):
    if aggregation_type.startswith('perc-'):
        validate_percentile(aggregation_type)
    else: 
        assert(aggregation_type in VALID_AGGREGATION_TYPES)
    return True
   
def validate_percentile(aggregation_type):
    assert(aggregation_type.startswith('perc-'))
    perc_value = aggregation_type.split('-')[1]
    assert(perc_value.isdigit())
    perc_value = int(perc_value)
    assert(0 < perc_value < 100)
    return True


def validate_interval(interval):
    assert(interval in VALID_INTERVALS or validate_custom_interval(interval))
    return True

def validate_custom_interval(interval:str):
    # Custom interval in HHMMSS format
    assert(len(interval) == 6)
    assert(all(char.isdigit() for char in interval))
    assert(0 <= int(interval[0:2]) < 24)  # hours
    assert(0 <= int(interval[2:4]) < 60)  # minutes
    assert(0 <= int(interval[4:6]) < 60)  # seconds
    return True


def test_connection():
    return SERVICE.test_connection()

def get(args_dict):
    return SERVICE.get(args_dict)

def get_ts(
            ts_id,
            aggregation_interval = None,
            aggregation_type = None,
            start_date = '1996-01-01',
            end_date = '2050-12-31',
            stationgroup_id = None,
            timezone = 'GMT-6',
            as_json = False):
    
    if (aggregation_interval is not None) and (aggregation_type is not None):
        transformation = construct_aggregation(aggregation_interval, aggregation_type)
        ts_id = f'{ts_id};{transformation}'

    #print('Downloading Timeseries Data')
    args = {'request':'getTimeseriesValues',
            'ts_id' : ts_id,
            'from': start_date,
            'to': end_date,
            'returnfields': ['Timestamp', 'Value', 'Quality Code','Quality Code Name'],
            'metadata': 'true',
            'md_returnfields': ['ts_unitsymbol',
                                'ts_name',
                                'ts_id',
                                'station_no',
                                'station_name',
                                'station_latitude',
                                'station_longitude',
                                'parametertype_id',
                                'parametertype_name',
                                'stationparameter_no',
                                'stationparameter_name'],
            'timezone':timezone,
            'ca_sta_returnfields': ['stn_HUC12','stn_EQuIS_ID']}
    
    if as_json:
        output = SERVICE.get_json(args)
    else: 
        output = SERVICE.get(args)
    #print('Done!')
    return output
    
def get_stations(
                    huc_id = None, 
                    parametertype_id = None,
                    stationgroup_id = None,
                    stationparameter_no = None,
                    station_no = None,
                    returnfields = []):
    
    args = {'request':'getStationList'}
    
    returnfields = list(set(['ca_sta','station_no','station_name'] + returnfields))
        
    args ={'request': 'getStationList',
            'stationparameter_no': stationparameter_no,
            'stationgroup_id': stationgroup_id,
            'parametertype_id': parametertype_id,
            'station_no': station_no,
            #'object_type': object_type,
            'returnfields': returnfields,
            #                  'parametertype_id','parametertype_name',
            #                  'station_latitude','station_longitude',
            #                  'stationparameter_no','stationparameter_name'],
            'ca_sta_returnfields': ['stn_HUC12','stn_EQuIS_ID','stn_AUID','hydrounit_title','hydrounit_no','NearestTown']
            }
    
    
    df = SERVICE.get(args)
    if huc_id is not None: df = df.loc[df['stn_HUC12'].str.startswith(huc_id)]
    return df

def get_ts_ids(
                station_nos = None,
                ts_ids = None,
                parametertype_id = None,
                stationparameter_no = None,
                stationgroup_id = None,
                ts_name = None,
                returnfields = None):
    

    if returnfields is None:
        returnfields = ['ts_id','ts_name','ca_sta','station_no',
                            'ts_unitsymbol',
                            'parametertype_id','parametertype_name',
                            'station_latitude','station_longitude',
                            'stationparameter_no','stationparameter_name',
                            'station_no','station_name',
                            'coverage','ts_density']
    

    args ={'request': 'getTimeseriesList',
            'station_no': station_nos,
            'ts_id': ts_ids,
            'parametertype_id': parametertype_id,
            'stationparameter_no': stationparameter_no,
            'ts_name' : ts_name,
            'returnfields': returnfields,
            'ca_sta_returnfields': ['stn_HUC12','stn_EQuIS_ID','stn_AUID']}
    
    df = SERVICE.get(args)
    return df
    


def get_wplmn(station_nos):
    
    PARAMETERS_MAP={'5004':'TP Load',
                    '5005':'TP Conc',
                    '5014':'TSS Load',
                    '5015':'TSS Conc',
                    '5024':'N Load',
                    '5025':'N Conc',
                    '5034':'OP Load',
                    '5035':'OP Conc',
                    '5044':'TKN Load',
                    '5045':'TKN Conc',
                    '262' :'Flow'}
        
    ts_ids = self.get_ts_ids(station_nos = station_nos,
                        stationgroup_id = '1319204',
                        stationparameter_no = list(PARAMETERS_MAP.keys()),
                        ts_name = ['20.Day.Mean'])
    
    if len(ts_ids) == 0:
        print('No WPLMN Sites Available')
        return pd.DataFrame() 
    
    dfs = []
    for ts_id in ts_ids['ts_id']:
        dfs.append(self.get_ts(ts_id))
        time.sleep(1)
    
    return pd.concat(dfs)


        
# nutrient
#     -N03N02
#     -OP
#     -NH3
#     -TP
#     -DO
#     -CHla
# temperature
# flow

# test = pyWISK()

# df = test.get_ts(ts_ids = 424663010)

# df = test.get_ts(station_nos = 'W25060001')

# df = test.get_wplmn(huc8_id = '07020005')

# df = test.get_ts(huc_id = '07010205',stationgroup_id = '1319204',parametertype_id = 11500)
