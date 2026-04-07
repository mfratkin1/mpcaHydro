from mpcaHydro import wiski
from mpcaHydro import pywisk
from mpcaHydro import outlets

station_ids = ['H67014001']


df_info = wiski.info(station_ids,'Q')
start_year = int(df_info['from'].str[0:4].iloc[0])
wiski.discharge(station_ids,
                start_year = start_year, 
                end_year = start_year + 5)

model_name = 'CrowWing'
station_ids = outlets.mapped_wiski_stations(model_name)
df_info = wiski.info(station_ids,'Q')
start_year = df_info['from'].str[0:4].apply(lambda x:int(x)).min()
df = wiski.discharge(station_ids,
                start_year = start_year, 
                end_year = start_year + 2)

int(df_info['from'].str[0:4])
station_ids = ['H67009001',
 'H67012001',
 'H67013001',
 'H67014001',
 'H67016001',
 'H67014002',
 'E67013001',
 'W67036001',
 'W67004001']