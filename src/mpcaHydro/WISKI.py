# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 16:18:03 2023

@author: mfratki
"""
import requests
import pandas as pd
import time

#TODO: Use this url to make sure web service is working https://wiskiweb01.pca.state.mn.us/
class Service():
    base_url = 'http://wiskiweb01.pca.state.mn.us/KiWIS/KiWIS?'
    base_dict = {
        'datasource': '0',
        'service': 'kisters',
        'type': 'queryServices',
        'format': 'json'}
    
    def __init__(self):
        #TODO: store request types in a file and load them here to avoid making a request when the class is instantiated
        #url = self.url({'request': 'getrequestinfo'})
        #self.requestTypes = requests.get(url).json()[0]  
        self._url = None
        self._args = None
        
    # def _requestTypes(self):
    #     url = self.url({'request': 'getrequestinfo'})
    #     self.requestTypes = requests.get(url).json()[0]  
    #     self._url = None
    #     self._args = None
    def _requestTypes(self):
        url = self.url({'request': 'getrequestinfo'}) 
        return requests.get(url).json()[0]  
        
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
        response = requests.get(url)
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
    

    def df(self,args_dict):
        
        # Download request
        # print('Downloading')
        response = requests.get(self.url(args_dict))
        
        response.raise_for_status()  # raises exception when not a 2xx response
        
        
        
        if response.status_code != 200:
            print('Error: ' + response.json()['message'])
            return 1

        get_requests = response.json()    
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
        assert(request_type in self.getRequests())
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

class pyWISK():
    
    def __init__(self):
        self.service = Service()

    
    def get(self,args_dict):
        return self.service.get(args_dict)
    
    def get_ts(self,
               ts_ids = None,
               huc_id = None,
               station_nos = None,
               parametertype_id = None,
               parameter_no = None,
               start_date = '1996-01-01',
               end_date = '2050-12-31',
               stationgroup_id = None,
               timezone = 'UTC'):
        
        if ts_ids is None:
            print('Determing Timeseries IDs')
            ts_ids = self.get_ts_ids(station_nos,huc_id,parametertype_id)
            print('Done!')
        
        #print('Downloading Timeseries Data')
        args = {'request':'getTimeseriesValues',
                'ts_id' : ts_ids,
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
        
        df = self.service.get(args)
        #print('Done!')
        return df
        
    def get_stations(self,
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
        
        
        df = self.service.get(args)
        if huc_id is not None: df = df.loc[df['stn_HUC12'].str.startswith(huc_id)]
        return df
    
    def get_ts_ids(self,
                   station_nos=None,
                   huc_id = None,
                   parametertype_id = None,
                   stationparameter_no = None,
                   stationgroup_id = None,
                   ts_name = None,
                   returnfields = None):
        
        if station_nos is None:
            station_nos = self.get_stations(huc_id,parametertype_id,stationgroup_id,stationparameter_no)['station_no'].to_list()
        
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
               'parametertype_id': parametertype_id,
               'stationparameter_no': stationparameter_no,
               'ts_name' : ts_name,
               'returnfields': returnfields,
               'ca_sta_returnfields': ['stn_HUC12','stn_EQuIS_ID','stn_AUID']}
        
        df = self.service.get(args)
        return df
        
    
    
    def get_wplmn(self,station_nos):
        
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


    # CONSTITUENT_NAME_NO = {'Q'  :['262'],#,'263'],
    #                        'WT' :['450'],# , '451' , '450.42','451.42'],
    #                        'OP' :['863'   ,'5034'  ,'5035'],
    #                        'DO' :['865'   ,'866'   , '867'],
    #                        'TP' :['5005'  ,'5004'],
    #                        'TSS':['5014' ,'5015'],
    #                        'N'  :['5024'  ,'5025'],
    #                        'TKN':['5044' ,'5045']}
    
    # TS_NAME_SELECTOR = {'Q':{'daily':['20.Day.Mean.Archive','20.Day.Mean'],
    #       'unit': ['15.Rated','08.Provisional.Edited']},
    #  'WT':{'daily':['20.Day.Mean','20.Day.Mean'],
    #        'unit': ['09.Archive','08.Provisional.Edited']},
    #  'TSS':{'daily':['20.Day.Mean','20.Day.Mean'],
    #        'unit': ['09.Archive','08.Provisional.Edited']},
    #  'N':{'daily':['20.Day.Mean','20.Day.Mean'],
    #        'unit': ['09.Archive','08.Provisional.Edited']},
    #  'TKN':{'daily':['20.Day.Mean','20.Day.Mean'],
    #        'unit': ['09.Archive','08.Provisional.Edited']},
    #  'TP':{'daily':['20.Day.Mean','20.Day.Mean'],
    #        'unit': ['09.Archive','08.Provisional.Edited']},
    #  'OP':{'daily':['20.Day.Mean','20.Day.Mean'],
    #        'unit': ['09.Archive','08.Provisional.Edited']},
    #  'DO':{'daily':['20.Day.Mean','20.Day.Mean'],
    #        'unit': ['09.Archive','08.Provisional.Edited']}}

    # def extract(self,station_nos,constituent,resolution):
    #     ts_names = self.TS_NAME_SELECTOR[constituent][resolution]
    #     data = self.get_ts_ids(station_no = station_nos,stationparameter_no = self.CONSTITUENT_NAME_NO[constituent],ts_name =ts_names)
    #     # Filter by MPCA distinction between internal and external sites and how time series are named
    #     ts_ids = pd.concat([data.loc[(data['station_no'].str.startswith('E')) & (data['ts_name'] == ts_names[1])],
    #                         data.loc[(~data['station_no'].str.startswith('E')) & (data['ts_name'] == ts_names[0])]])
    #     dfs = [self.get_ts(ts_ids = ts_id) for ts_id in ts_ids['ts_id']]
    #     data = pd.concat(dfs)
    #     return data


            
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
