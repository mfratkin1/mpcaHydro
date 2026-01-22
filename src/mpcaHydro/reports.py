from pathlib import Path
import duckdb
import glob

#TODO ensure all reports are actually in the reports schema

class reportManager():
    def __init__(self,db_path:Path):
        self.db_path = db_path

    def wiski_qc_counts(self):
        with duckdb.connect(self.db_path,read_only=True) as con:
            return wiski_qc_counts(con)
        
    def constituent_summary(self,constituent: str = None):
        with duckdb.connect(self.db_path,read_only=True) as con:
            return constituent_summary(con,constituent)
        
    def station_reach_pairs(self):
        with duckdb.connect(self.db_path,read_only=True) as con:
            return station_reach_pairs(con)
        
    def outlet_summary(self):
        with duckdb.connect(self.db_path,read_only=True) as con:
            return outlet_summary(con)
        


def outlet_summary(con: duckdb.DuckDBPyConnection):
    query = '''
    SELECT *,
    FROM 
        reports.outlet_constituent_summary
    ORDER BY
        outlet_id,
        constituent
    '''
    df = con.execute(query).fetch_df()
    return df
        

def wiski_qc_counts(con: duckdb.DuckDBPyConnection):
    query = '''
    SELECT *,
    FROM 
        reports.wiski_qc_count
    ORDER BY
        station_no,
        parametertype_name
    '''
    df = con.execute(query).fetch_df()
    return df

def constituent_summary(con: duckdb.DuckDBPyConnection,constituent: str = None):
    
    query = '''
    SELECT *,
    FROM 
        reports.constituent_summary
    ORDER BY
        station_id,
        station_origin,
        constituent
    '''
    df = con.execute(query).fetch_df()
    if constituent is not None:
        df = df[df['constituent'] == constituent]
    return df

def station_reach_pairs(con: duckdb.DuckDBPyConnection):
    query = '''
    SELECT *,
    FROM 
        reports.station_reach_pairs
    ORDER BY
        outlet_id,
        station_id
    '''
    df = con.execute(query).fetch_df()
    return df