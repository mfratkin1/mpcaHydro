# -*- coding: utf-8 -*-
"""
Created on Fri Jun  3 10:01:14 2022

@author: mfratki
"""

import pandas as pd
#from abc import abstractmethod
from pathlib import Path
from mpcaHydro import etlWISKI, etlSWD#, etlEQUIS


#
'''
Q
WT
TSS
N
TKN
OP
TP
CHLA
DO


class Station

- id
- name
- source
- data





'''
WISKI_EQUIS_XREF = pd.read_csv(Path(__file__).parent/'data/WISKI_EQUIS_XREF.csv')
#WISKI_EQUIS_XREF = pd.read_csv('C:/Users/mfratki/Documents/GitHub/hspf_tools/WISKI_EQUIS_XREF.csv')

AGG_DEFAULTS = {'cfs':'mean',
                'mg/l':'mean',
                'degF': 'mean',
                'lb':'sum'}

UNIT_DEFAULTS = {'Q': 'cfs',
                 'TSS': 'mg/l',
                 'TP' : 'mg/l',
                 'OP' : 'mg/l',
                 'TKN': 'mg/l',
                 'N'  : 'mg/l',
                 'WT' : 'degF',
                 'WL' : 'ft'}

# VALID_UNITS = {'Q': 'cfs',
#                  'TSS': 'mg/l','lb',
#                  'TP' : 'mg/l',
#                  'OP' : 'mg/l',
#                  'TKN': 'mg/l',
#                  'N'  : 'mg/l',
#                  'WT' : 'degF',
#                  'WL' : 'ft'}


def are_lists_identical(nested_list):
    # Sort each sublist
    sorted_sublists = [sorted(sublist) for sublist in nested_list]
    # Compare all sublists to the first one
    return all(sublist == sorted_sublists[0] for sublist in sorted_sublists)                                                                                               

class dataManager():

    def __init__(self,folderpath):
        
        self.data = {}
        self.folderpath = Path(folderpath)

    def get_wiski_stations(self):
        return list(WISKI_EQUIS_XREF['WISKI_STATION_NO'].unique())
    
    def get_equis_stations(self):
        return list(WISKI_EQUIS_XREF['EQUIS_STATION_ID'].unique())
    
    def wiski_equis_alias(self,wiski_station_id):
        equis_ids =  list(set(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_STATION_NO'] == wiski_station_id,'WISKI_EQUIS_ID'].to_list()))
        equis_ids = [equis_id for equis_id in equis_ids if not pd.isna(equis_id)]
        if len(equis_ids) == 0:
            return []
        elif len(equis_ids) > 1:
            print(f'Too Many Equis Stations for {wiski_station_id}')
            raise 
        else:
            return equis_ids[0]

    def wiski_equis_associations(self,wiski_station_id):
        equis_ids =  list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_STATION_NO'] == wiski_station_id,'EQUIS_STATION_ID'].unique())
        equis_ids =  [equis_id for equis_id in equis_ids if not pd.isna(equis_id)]
        if len(equis_ids) == 0:
            return []
        else:
            return equis_ids
        
    def equis_wiski_associations(self,equis_station_id):
        wiski_ids = list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['EQUIS_STATION_ID'] == equis_station_id,'WISKI_STATION_NO'].unique())
        wiski_ids = [wiski_id for wiski_id in wiski_ids if not pd.isna(wiski_id)]
        if len(wiski_ids) == 0:
            return []
        else:
            return wiski_ids

    def _equis_wiski_associations(self,equis_station_ids):
        wiski_stations = [self.equis_wiski_associations(equis_station_id) for equis_station_id in equis_station_ids]
        if are_lists_identical(wiski_stations):
            return wiski_stations[0]
        else:
            return []
            
    def _stations_by_wid(self,wid_no,station_origin):
        if station_origin in ['wiski','wplmn']:
            station_col = 'WISKI_STATION_NO'
        elif station_origin in ['equis','swd']:
            station_col = 'EQUIS_STATION_ID'
        else:
            raise
            
        return list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WID'] == wid_no,station_col].unique())

    
    def download_stations_by_wid(self, wid_no,station_origin, folderpath = None, overwrite = False):

        station_ids = self._station_by_wid(wid_no,station_origin)
        
        if not station_ids.empty:
            for _, row in station_ids.iterrows():
                self.download_station_data(row['station_id'],station_origin, folderpath, overwrite)

    def _download_station_data(self,station_id,station_origin,overwrite=False): 
        assert(station_origin in ['wiski','equis','swd','wplmn'])
        if station_origin == 'wiski':
            #equis_stations = list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_STATION_NO'] == station_id,'WISKI_EQUIS_ID'].unique())
            #[self.download_station_data(equis_station,'equis',overwrite = overwrite) for equis_station in equis_stations]
            self.download_station_data(station_id,'wiski',overwrite = overwrite)
            equis_alias = self.wiski_equis_alias(station_id)
            self.download_station_data(equis_alias,'swd',overwrite = overwrite)
        elif station_origin == 'wplmn':
            self.download_station_data(station_id,'wplmn',overwrite = overwrite)
            equis_alias = self.wiski_equis_alias(station_id)
            self.download_station_data(equis_alias,'swd',overwrite = overwrite)
        else:
            wiski_station = self.equis_wiski_associations(station_id)
            #wiski_station = WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['EQUIS_STATION_ID'] == station_id,'WISKI_STATION_NO']
            self.download_station_data(station_id,'equis',overwrite = overwrite)
            self.download_station_data(wiski_station,'wiski',overwrite = overwrite)
        

    def download_station_data(self,station_id,source,folderpath=None,overwrite = False):
        assert(source in ['wiski','equis','swd','wplmn'])
        station_id = str(station_id)
        save_name = station_id
        if source == 'wplmn':
            save_name = station_id + '_wplmn'
        
        if folderpath is None:
            folderpath = self.folderpath
        else:
            folderpath = Path(folderpath)
        
        
        if (folderpath.joinpath(save_name + '.csv').exists()) & (not overwrite):
            print (f'{station_id} data already downloaded')
            return
        
        if source == 'wiski':
            data = etlWISKI.download(station_id)
        elif source == 'swd':
            data = etlSWD.download(station_id)
        elif source == 'equis':
            data = etlSWD.download(station_id)
        else:
            data = etlWISKI.download(station_id,wplmn=True)
            #raise NotImplementedError()
            #data = etlEQUIS.download(station_id)

       
        
        if len(data) > 0:
            data.to_csv(folderpath.joinpath(save_name + '.csv'))
            self.data[station_id] = data
        else:
            print(f'No {source} calibration cata available at Station {station_id}')
        
        
    def _load(self,station_id):
        df =  pd.read_csv(self.folderpath.joinpath(station_id + '.csv'), 
                          index_col='datetime', 
                          parse_dates=['datetime'], 
                          #usecols=['Ts Date','Station number','variable', 'value','reach_id'],
                          dtype={'station_id': str, 'value': float, 'variable': str,'constituent':str,'unit':str})
        self.data[station_id] = df
        return df
    
    def load(self,station_id):
        try:
            df = self.data[station_id]
        except:
            df =  pd.read_csv(self.folderpath.joinpath(station_id + '.csv'), 
                              index_col='datetime', 
                              parse_dates=['datetime'], 
                              #usecols=['Ts Date','Station number','variable', 'value','reach_id'],
                              dtype={'station_id': str, 'value': float, 'variable': str,'constituent':str,'unit':str})
            self.data[station_id] = df
        return df
    
    def info(self,constituent):
        return pd.concat([self._load(file.stem) for file in self.folderpath.iterdir() if file.suffix == '.csv'])[['station_id','constituent','value']].groupby(by = ['station_id','constituent']).count()
        
    def get_wplmn_data(self,station_id,constituent,unit = 'mg/l', agg_period = 'YE', samples_only = True):
        
        assert constituent in ['Q','TSS','TP','OP','TKN','N','WT','DO','WL','CHLA']
        station_id = station_id + '_wplmn'
        dfsub = self._load(station_id)
        
        if samples_only:
            dfsub = dfsub.loc[dfsub['quality_id'] == 3]
        agg_func = 'mean'
        
        dfsub = dfsub.loc[(dfsub['constituent'] == constituent) & 
                              (dfsub['unit'] == unit),
                              ['value','data_format','source']]

        
        df = dfsub[['value']].resample(agg_period).agg(agg_func)
        
        if df.empty:
            dfsub = df
        else:
            
            df['data_format'] = dfsub['data_format'].iloc[0]
            df['source'] = dfsub['source'].iloc[0]
            
            #if (constituent == 'TSS') & (unit == 'lb'): #convert TSS from lbs to us tons
            #    dfsub['value'] = dfsub['value']/2000
    
            #dfsub = dfsub.resample('H').mean().dropna()
        
        df.attrs['unit'] = unit
        df.attrs['constituent'] = constituent
        return df['value'].to_frame().dropna()
    
    def get_data(self,station_id,constituent,agg_period = 'D'):
        return self._get_data([station_id],constituent,agg_period)
    
    def _get_data(self,station_ids,constituent,agg_period = 'D',tz_offset = '-6'):
        '''
        
        Returns the processed observational data associated with the calibration specific id. 
            

        Parameters
        ----------
        station_id : STR
            Station ID as a string
        constituent : TYPE
            Constituent abbreviation used for calibration. Valid options:
                'Q',
                'TSS',
                'TP',
                'OP',
                'TKN',
                'N',
                'WT',
                'DO',
                'WL']
        unit : TYPE, optional
            Units of data. The default is 'mg/l'.
        sample_flag : TYPE, optional
            For WPLMN data this flag determines modeled loads are returned. The default is False.

        Returns
        -------
        dfsub : Pands.Series
            Pandas series of data. Note that no metadata is returned.

        '''
        
        assert constituent in ['Q','TSS','TP','OP','TKN','N','WT','DO','WL','CHLA']
        
        unit = UNIT_DEFAULTS[constituent]
        agg_func = AGG_DEFAULTS[unit]
            
        dfsub = pd.concat([self.load(station_id) for station_id in station_ids]) # Check cache
        dfsub = dfsub.loc[(dfsub['constituent'] == constituent) &
                              (dfsub['unit'] == unit),
                              ['value','data_format','source']]   
        
        df = dfsub[['value']].resample(agg_period).agg(agg_func)
        df.attrs['unit'] = unit
        df.attrs['constituent'] = constituent
        
        if df.empty:
            
            return df
        else:
            
            df['data_format'] = dfsub['data_format'].iloc[0]
            df['source'] = dfsub['source'].iloc[0]


        # convert to desired timzone before stripping timezone information.
        #df.index.tz_convert('UTC-06:00').tz_localize(None)
        df.index = df.index.tz_localize(None)
        return df['value'].to_frame().dropna()
    

def validate_constituent(constituent):
    assert constituent in ['Q','TSS','TP','OP','TKN','N','WT','DO','WL','CHLA']

def validate_unit(unit):
    assert(unit in ['mg/l','lb','cfs','degF'])



# class database():
#     def __init__(self,db_path):
#         self.dbm = MonitoringDatabase(db_path)
        
    
#     def get_timeseries(self,station_ds, constituent,agg_period):      
#         validate_constituent(constituent)
#         unit = UNIT_DEFAULTS[constituent]
#         agg_func = AGG_DEFAULTS[unit]
#         return odm.get_timeseries(station_id,constituent)

    
#     def get_samples(self,station_ds, constituent,agg_period):
#         validate_constituent(constituent)
#         unit = UNIT_DEFAULTS[constituent]
#         agg_func = AGG_DEFAULTS[unit]
#         return odm.get_sample(station_id,constituent)

#     def get_samples_and_timeseries(self,station_ds, constituent,agg_period)
        
