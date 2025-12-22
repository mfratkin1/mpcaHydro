# -*- coding: utf-8 -*-
"""
Created on Fri Jun  3 10:01:14 2022

@author: mfratki
"""

import pandas as pd
#from abc import abstractmethod
from pathlib import Path
from mpcaHydro import etlSWD
from mpcaHydro import equis, wiski, warehouse
import duckdb


WISKI_EQUIS_XREF = pd.read_csv(Path(__file__).parent/'data/WISKI_EQUIS_XREF.csv')
#WISKI_EQUIS_XREF = pd.read_csv('C:/Users/mfratki/Documents/GitHub/hspf_tools/WISKI_EQUIS_XREF.csv')

AGG_DEFAULTS = {'cfs':'mean',
                'mg/l':'mean',
                'degF': 'mean',
                'lb':'sum'}

UNIT_DEFAULTS = {'Q': 'cfs',
                 'QB': 'cfs',
                 'TSS': 'mg/l',
                 'TP' : 'mg/l',
                 'OP' : 'mg/l',
                 'TKN': 'mg/l',
                 'N'  : 'mg/l',
                 'WT' : 'degF',
                 'WL' : 'ft'}

def are_lists_identical(nested_list):
    # Sort each sublist
    sorted_sublists = [sorted(sublist) for sublist in nested_list]
    # Compare all sublists to the first one
    return all(sublist == sorted_sublists[0] for sublist in sorted_sublists)                                                                                               

def construct_database(folderpath):
    folderpath = Path(folderpath)
    db_path = folderpath.joinpath('observations.duckdb').as_posix()
    with duckdb.connect(db_path) as con:
        con.execute("DROP TABLE IF EXISTS observations")
        datafiles = folderpath.joinpath('*.csv').as_posix()
        query = '''
        CREATE TABLE observations AS SELECT * 
        FROM
        read_csv_auto(?,
                        union_by_name = true);
        
        '''
        con.execute(query,[datafiles])


def build_warehouse(folderpath):
    folderpath = Path(folderpath)
    db_path = folderpath.joinpath('observations.duckdb').as_posix()
    warehouse.init_db(db_path)

def constituent_summary(db_path):
    with duckdb.connect(db_path) as con:
        query = '''
        SELECT
          station_id,
          station_origin,
          constituent,
          COUNT(*) AS sample_count,
          year(MIN(datetime)) AS start_date,
          year(MAX(datetime)) AS end_date
        FROM
          observations
        GROUP BY
          constituent, station_id,station_origin
        ORDER BY
          sample_count;'''
          
        res = con.execute(query)
        return res.fetch_df()


class dataManager():

    def __init__(self,folderpath, oracle_user = None, oracle_password =None):
        
        self.data = {}
        self.folderpath = Path(folderpath)
        self.db_path = self.folderpath.joinpath('observations.duckdb')
        self.oracle_user = oracle_user
        self.oracle_password = oracle_password
    
    def connect_to_oracle(self):
        assert (self.credentials_exist(), 'Oracle credentials not found. Set ORACLE_USER and ORACLE_PASSWORD environment variables or use swd as station_origin')
        equis.connect(user = self.oracle_user, password = self.oracle_password)
    
    def credentials_exist(self):
        if (self.oracle_user is not None) & (self.oracle_password is not None):
            return True
        else:
            return False
        
    def _reconstruct_database(self):
        construct_database(self.folderpath)
    
    def _build_warehouse(self):
        build_warehouse(self.folderpath)
        
    def constituent_summary(self,constituents = None):
        with duckdb.connect(self.db_path) as con:
            if constituents is None:
                constituents = con.query('''
                                        SELECT DISTINCT
                                        constituent
                                        FROM observations''').to_df()['constituent'].to_list()

            query = '''
            SELECT
            station_id,
            station_origin,
            constituent,
            COUNT(*) AS sample_count,
            year(MIN(datetime)) AS start_date,
            year(MAX(datetime)) AS end_date
            FROM
            observations
            WHERE
            constituent in (SELECT UNNEST(?))
            GROUP BY
            constituent,station_id,station_origin
            ORDER BY
            constituent,sample_count;'''
        
            df = con.execute(query,[constituents]).fetch_df()
        return df

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
        
    def equis_wiski_alias(self,equis_station_id):
        wiski_ids =  list(set(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_EQUIS_ID'] == equis_station_id,'WISKI_STATION_NO'].to_list()))
        wiski_ids = [wiski_id for wiski_id in wiski_ids if not pd.isna(wiski_id)]
        if len(wiski_ids) == 0:
            return []
        elif len(wiski_ids) > 1:
            print(f'Too Many WISKI Stations for {equis_station_id}')
            raise 
        else:
            return wiski_ids[0]

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
            self.download_station_data(station_id,'wiski',overwrite = overwrite)
        elif station_origin == 'wplmn':
            self.download_station_data(station_id,'wplmn',overwrite = overwrite)
        elif station_origin == 'swd':
            self.download_station_data(station_id,'swd',overwrite = overwrite)
        else:
            self.download_station_data(station_id,'equis',overwrite = overwrite)


       

    def download_station_data(self,station_id,station_origin,start_year = 1996, end_year = 2030,folderpath=None,overwrite = False,baseflow_method = 'Boughton'):
        assert(station_origin in ['wiski','equis','swd','wplmn'])
        station_id = str(station_id)
        save_name = station_id
        if station_origin == 'wplmn':
            save_name = station_id + '_wplmn'
        
        if folderpath is None:
            folderpath = self.folderpath
        else:
            folderpath = Path(folderpath)
        
        
        if (folderpath.joinpath(save_name + '.csv').exists()) & (not overwrite):
            print (f'{station_id} data already downloaded')
            return
        
        if station_origin == 'wiski':
            data = wiski.transform(wiski.download([station_id],wplmn=False, baseflow_method = baseflow_method))
        elif station_origin == 'swd':
            data = etlSWD.download(station_id)
        elif station_origin == 'equis':
            assert (self.credentials_exist(), 'Oracle credentials not found. Set ORACLE_USER and ORACLE_PASSWORD environment variables or use swd as station_origin')
            data = equis.transform(equis.download([station_id]))
        else:
            data = wiski.transform(wiski.download([station_id],wplmn=True, baseflow_method = baseflow_method))
        

       
        
        if len(data) > 0:
            data.to_csv(folderpath.joinpath(save_name + '.csv'))
            self.data[station_id] = data
        else:
            print(f'No {station_origin} calibration cata available at Station {station_id}')
        
    def _load(self,station_id):
        with duckdb.connect(self.db_path) as con:
            query = '''
            SELECT *
            FROM analytics.observations
            WHERE station_id = ?'''
            df = con.execute(query,[station_id]).fetch_df()
            df.set_index('datetime',inplace=True)
            self.data[station_id] = df
            return df

    def _load2(self,station_id):
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
            df = self._load(station_id)
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
                              ['value','station_origin']]

        
        df = dfsub[['value']].resample(agg_period).agg(agg_func)
        
        if df.empty:
            dfsub = df
        else:
            
            df['station_origin'] = dfsub['station_origin'].iloc[0]
            
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
        
        assert constituent in ['Q','QB','TSS','TP','OP','TKN','N','WT','DO','WL','CHLA']
        
        unit = UNIT_DEFAULTS[constituent]
        agg_func = AGG_DEFAULTS[unit]
            
        dfsub = pd.concat([self.load(station_id) for station_id in station_ids]) # Check cache
        dfsub.index = dfsub.index.tz_localize(None) # Drop timezone info
        #dfsub.set_index('datetime',drop=True,inplace=True)
        dfsub.rename(columns={'source':'station_origin'},inplace=True)
        dfsub = dfsub.loc[(dfsub['constituent'] == constituent) &
                              (dfsub['unit'] == unit),
                              ['value','station_origin']]   
        
        df = dfsub[['value']].resample(agg_period).agg(agg_func)
        df.attrs['unit'] = unit
        df.attrs['constituent'] = constituent
        
        if df.empty:
            
            return df
        else:
            
            df['station_origin'] = dfsub['station_origin'].iloc[0]


        # convert to desired timzone before stripping timezone information.
        #df.index.tz_convert('UTC-06:00').tz_localize(None)
    
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
        
